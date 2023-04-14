/*
 *     Copyright 2022 The Urchin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bitahub

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/urchinfs/bitahub-sdk/types"
	"github.com/urchinfs/bitahub-sdk/util"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Client interface {
	DatasetExists(ctx context.Context, datasetName string) (bool, error)

	ListDatasets(ctx context.Context) ([]DatasetInfo, error)

	StatFile(ctx context.Context, datasetName, fileName string) (FileInfo, error)

	UploadFile(ctx context.Context, datasetName, fileName, digest string, reader io.Reader) error

	RemoveFile(ctx context.Context, datasetName, fileName string) error

	RemoveFiles(ctx context.Context, datasetName string, objects []*FileInfo) error

	ListFiles(ctx context.Context, datasetName, prefix, marker string, limit int64) ([]*FileInfo, error)

	ListDirFiles(ctx context.Context, datasetName, prefix string) ([]*FileInfo, error)

	IsFileExist(ctx context.Context, datasetName, fileName string) (bool, error)

	IsDatasetExist(ctx context.Context, datasetName string) (bool, error)

	GetDownloadLink(ctx context.Context, datasetName, fileName string, expire time.Duration) (string, error)

	CreateDir(ctx context.Context, datasetName, folderName string) error

	GetDirMetadata(ctx context.Context, datasetName, folderKey string) (*FileInfo, bool, error)

	PreTransfer(ctx context.Context, datasetName, fileName string) error

	PostTransfer(ctx context.Context, datasetName, fileName string, isSuccess bool) error
}

type client struct {
	httpClient           *resty.Client
	redisStorage         *util.RedisStorage
	token                string
	tokenExpireTimestamp int64
	username             string
	password             string
	bitaHubUrl           string
	redisEndpoints       []string
	redisPassword        string
	enableCluster        bool
}

func New(username, password, bitaHubUrl string, redisEndpoints []string, redisPassword string, enableCluster bool) (Client, error) {
	b := &client{
		username:       username,
		password:       password,
		bitaHubUrl:     bitaHubUrl,
		redisEndpoints: redisEndpoints,
		redisPassword:  redisPassword,
		enableCluster:  enableCluster,
		httpClient:     resty.New(),
		redisStorage:   util.NewRedisStorage(redisEndpoints, redisPassword, enableCluster),
	}
	b.httpClient.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true})

	if b.username == "" || b.password == "" || b.bitaHubUrl == "" {
		return nil, types.ErrorInvalidParameter
	}

	if b.redisStorage == nil {
		return nil, errors.New("init redis error")
	}

	return b, nil
}

type DatasetInfo struct {
	// The name of the dataset.
	Name string `json:"name"`
	// Date the dataset was created.
	CreationDate time.Time `json:"creationDate"`
}

type FileInfo struct {
	Key          string
	Size         int64
	ETag         string
	ContentType  string
	LastModified time.Time
	Expires      time.Time
	Metadata     http.Header
}

type Reply struct {
	Message struct {
		Code    int32  `json:"code"`
		Message string `json:"message"`
		Status  int32  `json:"status"`
	} `json:"message"`
	Data json.RawMessage `json:"data,omitempty"`
}

func (r Reply) String() string {
	return fmt.Sprintf("{Code: %d, Message: %s, Status: %d}",
		r.Message.Code, r.Message.Message, r.Message.Status)
}

type GetTokenReply struct {
	Token string `json:"token"`
}

type GetDownloadCodeResp struct {
	DownloadCode string `json:"downloadCode"`
	DownloadId   int64  `json:"downloadId"`
	NextAction   int32  `json:"nextAction"`
}

type DatasetListReply struct {
	CurrentPage int32 `json:"currentPage"`
	HasNextPage bool  `json:"hasNextPage"`
	List        []struct {
		CreateTime int64  `json:"createTime"`
		Name       string `json:"name"`
		UserId     string `json:"userId"`
	} `json:"list"`
	PageSize int32 `json:"pageSize"`
	Total    int32 `json:"total"`
}

type FileListReply struct {
	FileList []struct {
		Directory  bool   `json:"directory"`
		FileName   string `json:"fileName"`
		Size       string `json:"size"`
		UpdateTime int64  `json:"updateTime"`
	} `json:"fileList"`
	PageNumber int64 `json:"pageNumber"`
	TotalPage  int64 `json:"totalPage"`
}

func SizeToBytes(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	s = strings.ToUpper(s)

	i := strings.IndexFunc(s, unicode.IsLetter)

	if i == -1 {
		return 0, errors.New("invalid size")
	}

	bytesString, multiple := s[:i], s[i:]
	bytes, err := strconv.ParseFloat(bytesString, 64)
	if err != nil || bytes <= 0 {
		return 0, errors.New("invalid size")
	}

	switch multiple {
	case "E", "EB", "EIB":
		return uint64(bytes * types.EXABYTE), nil
	case "P", "PB", "PIB":
		return uint64(bytes * types.PETABYTE), nil
	case "T", "TB", "TIB":
		return uint64(bytes * types.TERABYTE), nil
	case "G", "GB", "GIB":
		return uint64(bytes * types.GIGABYTE), nil
	case "M", "MB", "MIB":
		return uint64(bytes * types.MEGABYTE), nil
	case "K", "KB", "KIB":
		return uint64(bytes * types.KILOBYTE), nil
	case "B":
		return uint64(bytes), nil
	default:
		return 0, errors.New("invalid size")
	}
}

func parseBody(ctx context.Context, reply *Reply, body interface{}) error {
	if reply.Message.Status != types.HttpSuccess || reply.Message.Code != 0 {
		return errors.New("Code:" + strconv.FormatInt(int64(reply.Message.Code), 10) + ", Msg:" + reply.Message.Message)
	}

	if body != nil {
		err := json.Unmarshal(reply.Data, body)
		if err != nil {
			return errors.New("reply.Data parse error")
		}
	}

	return nil
}

func (c *client) sendHttpRequest(ctx context.Context, httpMethod, httpPath string, jsonBody string, respData interface{}) error {
	for {
		r := &Reply{}
		response := &resty.Response{}
		var err error
		if httpMethod == types.HttpMethodGet {
			response, err = c.httpClient.R().
				SetHeader(types.AuthHeader, c.token).
				SetResult(r).
				Get(c.bitaHubUrl + httpPath)
			if err != nil {
				return err
			}
		} else if httpMethod == types.HttpMethodPost {
			response, err = c.httpClient.R().
				SetHeader("Content-Type", "application/json").
				SetHeader(types.AuthHeader, c.token).
				SetBody(jsonBody).SetResult(r).
				Post(c.bitaHubUrl + httpPath)
			if err != nil {
				return err
			}
		} else if httpMethod == types.HttpMethodDelete {
			response, err = c.httpClient.R().
				SetHeader(types.AuthHeader, c.token).
				SetResult(r).
				Delete(c.bitaHubUrl + httpPath)
			if err != nil {
				return err
			}
		} else {
			return types.ErrorInternal
		}

		if !response.IsSuccess() {
			err := json.Unmarshal(response.Body(), r)
			if err == nil {
				if r.Message.Code == -1 && r.Message.Status == 200 && r.Message.Message == types.TokenExpiredMsg {
					c.token = ""
					if err = c.refreshToken(ctx); err != nil {
						return err
					}

					continue
				}
			}

			return errors.New("Code:" + strconv.FormatInt(int64(response.StatusCode()), 10) + ", Msg:" + string(response.Body()))
		}

		err = parseBody(ctx, r, respData)
		if err != nil {
			return err
		}

		break
	}

	return nil
}

func (c *client) getToken(ctx context.Context, username, password string) (string, error) {
	token := ""
	urlPath := fmt.Sprintf("/gateway/business/api/v2/login?username=%s&password=%s", username, password)
	resp := &GetTokenReply{}
	err := c.sendHttpRequest(ctx, types.HttpMethodPost, urlPath, "", resp)
	if err != nil {
		return token, err
	}

	token = resp.Token
	return token, nil
}

func (c *client) refreshToken(ctx context.Context) error {
	nowTime := time.Now().Unix()
	if c.token == "" || c.tokenExpireTimestamp < nowTime {
		token, err := c.getToken(ctx, c.username, c.password)
		if err != nil {
			return err
		}

		c.token = token
		c.tokenExpireTimestamp = nowTime + types.TokenExpireTime
	}

	return nil
}

func (c *client) findFileByName(ctx context.Context, datasetName, fileName string, idDir bool) (FileInfo, error) {
	dirName := datasetName
	if !strings.HasPrefix(dirName, "/") {
		dirName = "/" + dirName
	}

	tmpDir := filepath.Dir(fileName)
	if tmpDir != "." {
		if !strings.HasPrefix(tmpDir, "/") {
			dirName += "/"
		}
		dirName += tmpDir
	}

	pageIndex := 0
	const (
		pageSize = 100
	)

	fileName = filepath.Base(fileName)
	for {
		resp := &FileListReply{}
		bhPath := fmt.Sprintf("/gateway/fileCenter/api/file/getFileList?type=3&dir=%s&pageNumber=%d&pageSize=%d",
			dirName, pageIndex, pageSize)

		err := c.sendHttpRequest(ctx, types.HttpMethodGet, bhPath, "", resp)
		if err != nil {
			return FileInfo{}, err
		}

		for _, item := range resp.FileList {
			tm := time.UnixMilli(item.UpdateTime)
			timeObj, err := time.ParseInLocation(time.RFC3339Nano, tm.Format(time.RFC3339Nano), time.Local)
			if err != nil {
				timeObj = time.Time{}
			}

			byteSize, err := SizeToBytes(item.Size)
			if err != nil {
				byteSize = 0
			}

			if item.FileName == fileName && item.Directory == idDir {
				return FileInfo{
					Key:          item.FileName,
					Size:         int64(byteSize),
					LastModified: timeObj,
				}, nil
			}
		}

		if len(resp.FileList) < pageSize {
			break
		}

		pageIndex += pageSize
	}

	return FileInfo{}, errors.New("NoSuchKey")
}

func (c *client) DatasetExists(ctx context.Context, datasetName string) (bool, error) {
	if err := c.refreshToken(ctx); err != nil {
		return false, err
	}

	pageIndex := 1
	const (
		pageSize = 20
	)

	for {
		resp := &DatasetListReply{}
		bhPath := fmt.Sprintf("/gateway/dataSet/api/getUserDataSet?pageNumber=%d&pageSize=%d", pageIndex, pageSize)
		err := c.sendHttpRequest(ctx, types.HttpMethodGet, bhPath, "", resp)
		if err != nil {
			return false, err
		}

		for _, dataset := range resp.List {
			if datasetName == dataset.Name {
				return true, nil
			}
		}

		if !resp.HasNextPage || len(resp.List) < pageSize {
			break
		}

		pageIndex += 1
	}

	return false, nil
}

func (c *client) ListDatasets(ctx context.Context) ([]DatasetInfo, error) {
	if err := c.refreshToken(ctx); err != nil {
		return []DatasetInfo{}, err
	}

	pageIndex := 0
	const (
		pageSize = 20
	)

	var datasetsInfo []DatasetInfo
	for {
		resp := &DatasetListReply{}
		bhPath := fmt.Sprintf("/gateway/dataSet/api/getUserDataSet?pageNumber=%d&pageSize=%d", pageIndex, pageSize)
		err := c.sendHttpRequest(ctx, types.HttpMethodGet, bhPath, "", resp)
		if err != nil {
			return []DatasetInfo{}, err
		}

		for _, dataset := range resp.List {
			tm := time.UnixMilli(dataset.CreateTime)
			timeObj, err := time.ParseInLocation(time.RFC3339Nano, tm.Format(time.RFC3339Nano), time.Local)
			if err != nil {
				timeObj = time.Time{}
			}

			datasetsInfo = append(datasetsInfo, DatasetInfo{
				Name:         dataset.Name,
				CreationDate: timeObj,
			})
		}

		if !resp.HasNextPage || len(resp.List) < pageSize {
			break
		}

		pageIndex += pageSize
	}

	return datasetsInfo, nil
}

func (c *client) StatFile(ctx context.Context, datasetName, fileName string) (FileInfo, error) {
	if err := c.refreshToken(ctx); err != nil {
		return FileInfo{}, err
	}

	objectInfo, err := c.findFileByName(ctx, datasetName, fileName, false)
	if err != nil {
		return FileInfo{}, err
	}

	return objectInfo, nil
}

func (c *client) UploadFile(ctx context.Context, datasetName, fileName, digest string, reader io.Reader) error {
	dirName := datasetName
	if !strings.HasPrefix(dirName, "/") {
		dirName = "/" + dirName
	}

	tmpDir := filepath.Dir(fileName)
	if tmpDir != "." {
		if !strings.HasPrefix(tmpDir, "/") {
			dirName += "/"
		}
		dirName += tmpDir
	}

	fileName = filepath.Base(fileName)
	if err := c.refreshToken(ctx); err != nil {
		return err
	}

	bhPath := fmt.Sprintf("/gateway/fileCenter/api/file/upload?type=3")
	r := &Reply{}
	response, err := c.httpClient.R().
		SetHeader(types.AuthHeader, c.token).
		SetHeader("Content-Type", "multipart/form-data").
		SetQueryParam("dir", dirName).
		SetQueryParam("name", fileName).
		SetFileReader("file", fileName, reader).
		SetResult(r).
		Post(c.bitaHubUrl + bhPath)
	if err != nil {
		return err
	}
	if !response.IsSuccess() {
		r := &Reply{}
		err = json.Unmarshal(response.Body(), r)
		if err != nil {
			return errors.New("internal error: json.Unmarshal fail")
		}

		return errors.New("Code:" + strconv.FormatInt(int64(r.Message.Code), 10) + ", Msg:" + r.Message.Message)
	}

	return nil
}

func (c *client) RemoveFile(ctx context.Context, datasetName, fileName string) error {
	if err := c.refreshToken(ctx); err != nil {
		return err
	}

	filePath := datasetName
	if !strings.HasPrefix(filePath, "/") {
		filePath = "/" + filePath
	}

	if !strings.HasPrefix(fileName, "/") {
		filePath += "/"
	}
	filePath += fileName

	bhPath := fmt.Sprintf("/gateway/fileCenter/api/file/deleteFile?type=3&dirs=%s", filePath)
	err := c.sendHttpRequest(ctx, types.HttpMethodPost, bhPath, "", nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *client) RemoveFiles(ctx context.Context, datasetName string, objects []*FileInfo) error {
	for _, obj := range objects {
		err := c.RemoveFile(ctx, datasetName, obj.Key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *client) ListFiles(ctx context.Context, datasetName, prefix, marker string, limit int64) ([]*FileInfo, error) {
	if err := c.refreshToken(ctx); err != nil {
		return nil, err
	}

	dirName := datasetName
	if !strings.HasPrefix(dirName, "/") {
		dirName = "/" + dirName
	}

	if !strings.HasPrefix(prefix, "/") {
		dirName += "/"
	}
	dirName += prefix

	pageIndex := 0
	const (
		pageSize = 100
	)

	var objects []*FileInfo
	for {
		resp := &FileListReply{}
		bhPath := fmt.Sprintf("/gateway/fileCenter/api/file/getFileList?type=3&dir=%s&pageNumber=%d&pageSize=%d",
			dirName, pageIndex, pageSize)

		err := c.sendHttpRequest(ctx, types.HttpMethodGet, bhPath, "", resp)
		if err != nil {
			return nil, err
		}

		for _, item := range resp.FileList {
			tm := time.UnixMilli(item.UpdateTime)
			timeObj, err := time.ParseInLocation(time.RFC3339Nano, tm.Format(time.RFC3339Nano), time.Local)
			if err != nil {
				timeObj = time.Time{}
			}

			byteSize, err := SizeToBytes(item.Size)
			if err != nil {
				byteSize = 0
			}

			objects = append(objects, &FileInfo{
				Key:          item.FileName,
				Size:         int64(byteSize),
				LastModified: timeObj,
			})
		}

		if len(resp.FileList) < pageSize {
			break
		}

		pageIndex += pageSize
	}

	return objects, nil
}

func (c *client) listDirObjs(ctx context.Context, datasetName, path string) ([]*FileInfo, error) {
	if path == "." || path == ".." {
		return nil, nil
	}

	dirName := datasetName
	if !strings.HasPrefix(dirName, "/") {
		dirName = "/" + dirName
	}

	if !strings.HasPrefix(path, "/") {
		dirName += "/"
	}
	dirName += path

	pageIndex := 0
	const (
		pageSize = 100
	)

	var objects []*FileInfo
	for {
		resp := &FileListReply{}
		bhPath := fmt.Sprintf("/gateway/fileCenter/api/file/getFileList?type=3&dir=%s&pageNumber=%d&pageSize=%d",
			dirName, pageIndex, pageSize)

		err := c.sendHttpRequest(ctx, types.HttpMethodGet, bhPath, "", resp)
		if err != nil {
			return nil, err
		}

		for _, item := range resp.FileList {
			tm := time.UnixMilli(item.UpdateTime)
			timeObj, err := time.ParseInLocation(time.RFC3339Nano, tm.Format(time.RFC3339Nano), time.Local)
			if err != nil {
				timeObj = time.Time{}
			}

			byteSize, err := SizeToBytes(item.Size)
			if err != nil {
				byteSize = 0
			}

			if !item.Directory {
				objects = append(objects, &FileInfo{
					Key:          filepath.Join(path, item.FileName),
					Size:         int64(byteSize),
					LastModified: timeObj,
				})
			} else {
				tmpObjs, err := c.listDirObjs(ctx, datasetName, filepath.Join(path, item.FileName))
				if err != nil {
					return nil, err
				}

				objects = append(objects, tmpObjs...)
			}
		}

		if len(resp.FileList) < pageSize {
			break
		}

		pageIndex += pageSize
	}

	return objects, nil
}

func (c *client) ListDirFiles(ctx context.Context, datasetName, prefix string) ([]*FileInfo, error) {
	if err := c.refreshToken(ctx); err != nil {
		return nil, err
	}

	resp, err := c.listDirObjs(ctx, datasetName, prefix)
	if err != nil {
		return nil, err
	}

	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	resp = append(resp, &FileInfo{
		Key: prefix,
	})

	return resp, nil
}

func (c *client) IsFileExist(ctx context.Context, datasetName, fileName string) (bool, error) {
	if err := c.refreshToken(ctx); err != nil {
		return false, err
	}

	objectInfo, err := c.findFileByName(ctx, datasetName, fileName, false)
	if err != nil || objectInfo.Key == "" {
		return false, err
	}

	return true, nil
}

func (c *client) IsDatasetExist(ctx context.Context, datasetName string) (bool, error) {
	return c.DatasetExists(ctx, datasetName)
}

func (c *client) GetDownloadLink(ctx context.Context, datasetName, fileName string, expire time.Duration) (string, error) {
	fileName = strings.TrimLeft(fileName, "/")
	fileName = strings.TrimRight(fileName, "/")
	downloadInfoKey := c.redisStorage.MakeStorageKey([]string{datasetName, fileName}, "")
	value, err := c.redisStorage.Get(downloadInfoKey)
	if err != nil {
		if err != types.ErrorNotExists {
			return "", err
		}
	} else {
		ttlDuration, err := c.redisStorage.GetTTL(downloadInfoKey)
		if err != nil {
			return "", err
		}

		if ttlDuration < util.DefaultMinOpTimeout {
			_ = c.redisStorage.SetTTL(downloadInfoKey, util.DefaultMinOpTimeout)
		}

		if value != nil {
			signedUrl := fmt.Sprintf("%s/gateway/fileCenter/api/split/outer/download/%s", c.bitaHubUrl, string(value))
			return signedUrl, nil
		}
	}

	memberCnt, err := c.redisStorage.GetSetMemberCnt(util.MembersSetKey)
	if err != nil {
		return "", err
	}

	if memberCnt > util.DefaultMaxMembersCnt {
		return "", types.ErrorNotAllowed
	}

	const (
		DefaultSignedUrlTime = 60 * 60 * 6
		ErrorDirBigFile      = "Code:10007"
	)

	nowTime := time.Now().Unix()
	if c.tokenExpireTimestamp-nowTime < DefaultSignedUrlTime {
		c.token = ""
	}

	if err := c.refreshToken(ctx); err != nil {
		return "", err
	}

	dirName := datasetName
	if !strings.HasPrefix(dirName, "/") {
		dirName = "/" + dirName
	}

	jsonBody, err := json.Marshal([]string{filepath.Join(dirName, fileName)})
	if err != nil {
		return "", errors.New("internal error: json.Unmarshal fail")
	}

	bhPath := fmt.Sprintf("/gateway/fileCenter/api/split/detect?type=3")
	resp := &GetDownloadCodeResp{}
	err = c.sendHttpRequest(ctx, types.HttpMethodPost, bhPath, string(jsonBody), resp)
	if err != nil {
		if strings.Contains(err.Error(), ErrorDirBigFile) {
			downloadInfo := types.SaltSignedUrlStr + filepath.Join(datasetName, fileName)
			err = c.redisStorage.SetWithTimeout(downloadInfoKey, []byte(downloadInfo), util.DefaultOpTimeout)
			if err != nil {
				return "", err
			}

			signedUrl := fmt.Sprintf("%s/gateway/fileCenter/api/split/outer/download/%s", c.bitaHubUrl, downloadInfo)
			return signedUrl, nil
		}

		return "", err
	}

	downloadInfo := fmt.Sprintf("%d/%s", resp.DownloadId, resp.DownloadCode)
	signedUrl := fmt.Sprintf("%s/gateway/fileCenter/api/split/outer/download/%s", c.bitaHubUrl, downloadInfo)

	err = c.redisStorage.SetWithTimeout(downloadInfoKey, []byte(downloadInfo), util.DefaultOpTimeout)
	if err != nil {
		return "", err
	}

	return signedUrl, nil
}

func (c *client) CreateDir(ctx context.Context, datasetName, folderName string) error {
	if err := c.refreshToken(ctx); err != nil {
		return err
	}

	dirName := datasetName
	if !strings.HasPrefix(dirName, "/") {
		dirName = "/" + dirName
	}

	if !strings.HasPrefix(folderName, "/") {
		dirName += "/"
	}
	dirName += folderName

	bhPath := fmt.Sprintf("/gateway/fileCenter/api/file/addDir")
	r := &Reply{}
	response, err := c.httpClient.R().
		SetHeader("Content-Type", "application/json").
		SetHeader(types.AuthHeader, c.token).
		SetQueryParam("type", "3").
		SetQueryParam("dir", dirName).
		SetResult(r).
		Post(c.bitaHubUrl + bhPath)
	if err != nil {
		return err
	}
	if !response.IsSuccess() {
		r := &Reply{}
		err = json.Unmarshal(response.Body(), r)
		if err != nil {
			return errors.New("internal error: json.Unmarshal fail")
		}

		return errors.New("Code:" + strconv.FormatInt(int64(r.Message.Code), 10) + ", Msg:" + r.Message.Message)
	}

	return nil
}

func (c *client) GetDirMetadata(ctx context.Context, datasetName, folderKey string) (*FileInfo, bool, error) {
	if err := c.refreshToken(ctx); err != nil {
		return nil, false, nil
	}

	folderKey = strings.TrimRight(folderKey, "/")
	folderInfo, err := c.findFileByName(ctx, datasetName, folderKey, true)
	if err != nil || folderInfo.Key == "" {
		return nil, false, err
	}

	return &folderInfo, true, nil
}

func (c *client) PreTransfer(ctx context.Context, datasetName, fileName string) error {
	fileName = strings.TrimLeft(fileName, "/")
	fileName = strings.TrimRight(fileName, "/")
	downloadInfoKey := c.redisStorage.MakeStorageKey([]string{datasetName, fileName}, "")
	err := c.redisStorage.InsertSet(util.MembersSetKey, downloadInfoKey)
	if err != nil {
		return err
	}

	value, err := c.redisStorage.Get(downloadInfoKey)
	if err != nil {
		if err == types.ErrorNotExists {
			return nil
		}

		return err
	}

	if strings.Contains(string(value), types.SaltSignedUrlStr) {
		return nil
	}

	if err := c.refreshToken(ctx); err != nil {
		return err
	}

	bhPath := fmt.Sprintf("/gateway/fileCenter/api/split/outer/downloadProbe/%s", string(value))
	err = c.sendHttpRequest(ctx, types.HttpMethodGet, bhPath, "", nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *client) PostTransfer(ctx context.Context, datasetName, fileName string, isSuccess bool) error {
	fileName = strings.TrimLeft(fileName, "/")
	fileName = strings.TrimRight(fileName, "/")
	downloadInfoKey := c.redisStorage.MakeStorageKey([]string{datasetName, fileName}, "")
	value, err := c.redisStorage.Get(downloadInfoKey)
	if err != nil {
		if err == types.ErrorNotExists {
			return nil
		}

		return err
	}

	if strings.Contains(string(value), types.SaltSignedUrlStr) {
		return nil
	}

	if err := c.refreshToken(ctx); err != nil {
		return err
	}

	if isSuccess {
		bhPath := fmt.Sprintf("/gateway/fileCenter/api/split/outer/markSucess/%s", string(value))
		err := c.sendHttpRequest(ctx, types.HttpMethodGet, bhPath, "", nil)
		if err != nil {
			return err
		}
	} else {
		bhPath := fmt.Sprintf("/gateway/fileCenter/api/split/outer/deleteDownloadRecord/%s", string(value))
		err := c.sendHttpRequest(ctx, types.HttpMethodGet, bhPath, "", nil)
		if err != nil {
			return err
		}
	}

	err = c.redisStorage.Delete(downloadInfoKey)
	if err != nil {
		return err
	}

	err = c.redisStorage.DeleteSetMember(util.MembersSetKey, downloadInfoKey)
	if err != nil {
		return err
	}

	processedMemberCnt, _ := c.redisStorage.GetSetMemberCnt(util.ProcessedMembersSetKey)
	if processedMemberCnt > 0 {
		time.Sleep(time.Second * 11)

		_ = c.redisStorage.Delete(util.ProcessedMembersSetKey)
	}
	_ = c.redisStorage.InsertSet(util.ProcessedMembersSetKey, downloadInfoKey)

	return nil
}

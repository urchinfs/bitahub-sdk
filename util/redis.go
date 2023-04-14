package util

import (
	"github.com/go-redis/redis"
	"github.com/urchinfs/bitahub-sdk/types"
	"reflect"
	"strings"
	"time"
)

const (
	DefaultOpTimeout         = 86400 * 1 * time.Second
	DefaultMinOpTimeout      = 60 * 60 * 6 * time.Second
	DefaultMembersSetTimeout = 60 * 60 * 2 * time.Second
	DefaultMaxMembersCnt     = 1

	storagePrefix          = "bitahub:signedUrl"
	MembersSetKey          = "bitahub:members:set"
	ProcessedMembersSetKey = "bitahub:processed:members:set"
	ConfStorageKey         = "config:stroage:bitahub"
	keySeperator           = ":"
)

type RedisStorage struct {
	client redis.Cmdable
}

func NewRedisStorage(endpoints []string, password string, enableCluster bool) *RedisStorage {
	var client redis.Cmdable
	if enableCluster {
		client = redis.NewClusterClient(
			&redis.ClusterOptions{
				Addrs:    endpoints,
				Password: password,
			},
		)
	} else if len(endpoints) != 0 {
		client = redis.NewClient(
			&redis.Options{
				Addr:     endpoints[0],
				Password: password,
			},
		)
	}
	if reflect.ValueOf(client).IsNil() {
		return nil
	}

	return &RedisStorage{
		client: client,
	}
}

func (s *RedisStorage) Get(key string) ([]byte, error) {
	exists, err := s.Exists(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, types.ErrorNotExists
	}
	value, err := s.client.Get(key).Result()
	if err != nil {
		return nil, err
	}

	return []byte(value), nil
}

func (s *RedisStorage) Set(key string, value []byte) error {
	_, err := s.client.Set(key, value, DefaultOpTimeout).Result()
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisStorage) SetWithTimeout(key string, value []byte, timeout time.Duration) error {
	_, err := s.client.Set(key, value, timeout).Result()
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisStorage) Delete(key string) error {
	_, err := s.client.Del(key).Result()
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisStorage) Exists(key string) (bool, error) {
	value, err := s.client.Exists(key).Result()
	if err != nil {
		return false, err
	}

	if value == int64(0) {
		return false, nil
	} else if value == int64(1) {
		return true, nil
	}

	return false, types.ErrorInternal
}

func (s *RedisStorage) InsertSet(key string, value string) error {
	_, err := s.client.SAdd(key, value).Result()
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisStorage) GetSet(key string) ([]string, error) {
	members, err := s.client.SMembers(key).Result()
	if err != nil {
		return nil, err
	}

	return members, nil
}

func (s *RedisStorage) DeleteSet(key string, members []string) error {
	if len(members) == 0 {
		return nil
	}

	sli := make([]interface{}, len(members))
	for i, v := range members {
		sli[i] = v
	}
	_, err := s.client.SRem(key, sli...).Result()
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisStorage) DeleteSetMember(key string, member string) error {
	_, err := s.client.SRem(key, member).Result()
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisStorage) GetSetMemberCnt(key string) (int64, error) {
	memberCnt, err := s.client.SCard(key).Result()
	if err != nil {
		return -1, err
	}

	return memberCnt, nil
}

func (s *RedisStorage) ReadMap(key string) (map[string][]byte, error) {
	array, err := s.client.HKeys(key).Result()
	if err != nil {
		return nil, types.ErrorInternal
	}

	result := make(map[string][]byte)
	for _, hkey := range array {
		value, err := s.client.HGet(key, hkey).Result()
		if err != nil {
			return nil, types.ErrorInternal
		}
		result[hkey] = []byte(value)
	}

	return result, nil
}

func (s *RedisStorage) GetMapElement(key string, element string) ([]byte, error) {
	value, err := s.client.HGet(key, element).Result()
	if err != nil {
		return nil, err
	}

	return []byte(value), nil
}

func (s *RedisStorage) GetTTL(key string) (time.Duration, error) {
	ttlDuration, err := s.client.TTL(key).Result()
	if err != nil {
		return -1, err
	}

	return ttlDuration, nil
}

func (s *RedisStorage) SetTTL(key string, ttlDuration time.Duration) error {
	_, err := s.client.Expire(key, ttlDuration).Result()
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisStorage) MakeStorageKey(identifiers []string, prefix string) string {
	if len(prefix) == 0 {
		prefix = storagePrefix
	}

	identifiers = append([]string{prefix}, identifiers...)
	key := strings.Join(identifiers, keySeperator)

	return key
}

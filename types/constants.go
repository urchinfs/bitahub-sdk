/*
 *     Copyright 2022 The Dragonfly Authors
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

package types

const (
	AuthHeader      = "token"
	HttpSuccess     = 200
	TokenExpireTime = 60 * 60 * 1
	TokenExpiredMsg = "登录超时，请重新登录"

	BitaHubUrl       = "https://www.bitahub.com/"
	SaltSignedUrlStr = "salt-mock-"
)

const (
	HttpMethodGet    = "get"
	HttpMethodPost   = "post"
	HttpMethodDelete = "delete"
)

const (
	BYTE = 1 << (10 * iota)
	KILOBYTE
	MEGABYTE
	GIGABYTE
	TERABYTE
	PETABYTE
	EXABYTE
)

const (
	GB_100 = 1024 * 1024 * 1024 * 100
	GB_10  = 1024 * 1024 * 1024 * 10
	GB_1   = 1024 * 1024 * 1024
	MB_500 = 1024 * 1024 * 500
	MB_100 = 1024 * 1024 * 100
)

const (
	// AffinitySeparator is separator of affinity.
	AffinitySeparator = "|"
)

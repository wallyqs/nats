// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jetstream

// Request API subjects for JetStream.
const (
	// DefaultAPIPrefix is the default prefix for the JetStream API.
	DefaultAPIPrefix = "$JS.API."

	// APIAccountInfo is for obtaining general information about JetStream.
	APIAccountInfo = "INFO"

	// APIConsumerCreateT is used to create consumers.
	APIConsumerCreateT = "CONSUMER.CREATE.%s"

	// APIDurableCreateT is used to create durable consumers.
	APIDurableCreateT = "CONSUMER.DURABLE.CREATE.%s.%s"

	// APIConsumerInfoT is used to create consumers.
	APIConsumerInfoT = "CONSUMER.INFO.%s.%s"

	// APIRequestNextT is the prefix for the request next message(s) for a consumer in worker/pull mode.
	APIRequestNextT = "CONSUMER.MSG.NEXT.%s.%s"

	// APIDeleteConsumerT is used to delete consumers.
	APIConsumerDeleteT = "CONSUMER.DELETE.%s.%s"

	// APIConsumerListT is used to return all detailed consumer information
	APIConsumerListT = "CONSUMER.LIST.%s"

	// APIStreams can lookup a stream by subject.
	APIStreams = "STREAM.NAMES"

	// APIStreamCreateT is the endpoint to create new streams.
	APIStreamCreateT = "STREAM.CREATE.%s"

	// APIStreamInfoT is the endpoint to get information on a stream.
	APIStreamInfoT = "STREAM.INFO.%s"

	// APIStreamUpdate is the endpoint to update existing streams.
	APIStreamUpdateT = "STREAM.UPDATE.%s"

	// APIStreamDeleteT is the endpoint to delete streams.
	APIStreamDeleteT = "STREAM.DELETE.%s"

	// APIStreamSnapshotT is the endpoint to snapshot streams.
	APIStreamSnapshotT = "STREAM.SNAPSHOT.%s"

	// APIStreamRestoreT is the endpoint to restore a stream from a snapshot.
	APIStreamRestoreT = "STREAM.RESTORE.%s"

	// APIPurgeStreamT is the endpoint to purge streams.
	APIStreamPurgeT = "STREAM.PURGE.%s"

	// APIStreamListT is the endpoint that will return all detailed stream information
	APIStreamList = "STREAM.LIST"

	// APIMsgDeleteT is the endpoint to remove a message.
	APIMsgDeleteT = "STREAM.MSG.DELETE.%s"
)

// APIError is included in all API responses if there was an error.
type APIError struct {
	Code        int    `json:"code"`
	Description string `json:"description,omitempty"`
}

// APIResponse is a standard response from the JetStream JSON API
type APIResponse struct {
	Type  string    `json:"type"`
	Error *APIError `json:"error,omitempty"`
}

// APIPaged includes variables used to create paged responses from the JSON API
type APIPaged struct {
	Total  int `json:"total"`
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}

// APIPagedRequest includes parameters allowing specific pages to be requested
// from APIs responding with APIPaged.
type APIPagedRequest struct {
	Offset int `json:"offset"`
}

// AccountLimits is for the information about an account.
type AccountLimits struct {
	MaxMemory    int64 `json:"max_memory"`
	MaxStore     int64 `json:"max_storage"`
	MaxStreams   int   `json:"max_streams"`
	MaxConsumers int   `json:"max_consumers"`
}

// AccountStats returns current statistics about the account's JetStream usage.
type AccountStats struct {
	Memory  uint64        `json:"memory"`
	Store   uint64        `json:"storage"`
	Streams int           `json:"streams"`
	Limits  AccountLimits `json:"limits"`
}

type AccountInfoResponse struct {
	APIResponse
	*AccountStats
}

// type CreateConsumerRequest struct {
// 	Stream string          `json:"stream_name"`
// 	Config *ConsumerConfig `json:"config"`
// }
// type ConsumerResponse struct {
// 	APIResponse
// 	*ConsumerInfo
// }

type StreamNamesResponse struct {
	APIResponse
	APIPaged
	Streams []string `json:"streams"`
}

// // ConsumerDeleteResponse is the response for a Consumer delete request.
// type ConsumerDeleteResponse struct {
// 	APIResponse
// 	Success bool `json:"success,omitempty"`
// }

// // ConsumersRequest is the type used for Consumers requests.
// type ConsumersRequest struct {
// 	APIPagedRequest
// }

// // ConsumerListResponse is the response for a Consumers List request.
// type ConsumerListResponse struct {
// 	APIResponse
// 	APIPaged
// 	Consumers []*ConsumerInfo `json:"consumers"`
// }

// // StreamCreateResponse stream creation.
// type StreamCreateResponse struct {
// 	APIResponse
// 	*StreamInfo
// }

// type StreamInfoResponse = StreamCreateResponse

// // StreamDeleteResponse is the response for a Stream delete request.
// type StreamDeleteResponse struct {
// 	APIResponse
// 	Success bool `json:"success,omitempty"`
// }

// type MsgDeleteRequest struct {
// 	Seq uint64 `json:"seq"`
// }

// // MsgDeleteResponse is the response for a Stream delete request.
// type MsgDeleteResponse struct {
// 	APIResponse
// 	Success bool `json:"success,omitempty"`
// }

// StreamPurgeResponse.
// type StreamPurgeResponse struct {
// 	APIResponse
// 	Success bool   `json:"success,omitempty"`
// 	Purged  uint64 `json:"purged"`
// }

// // StreamNamesRequest is used for Stream Name requests.
// type StreamNamesRequest struct {
// 	APIPagedRequest
// 	// These are filters that can be applied to the list.
// 	Subject string `json:"subject,omitempty"`
// }

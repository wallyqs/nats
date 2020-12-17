package jetstream

import "time"

// NextRequest is for getting next messages for pull based consumers.
type NextRequest struct {
	Expires *time.Time `json:"expires,omitempty"`
	Batch   int        `json:"batch,omitempty"`
	NoWait  bool       `json:"no_wait,omitempty"`
}

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

type AccountInfoResponse struct {
	APIResponse
	*AccountStats
}

// AccountLimits is for the information about
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

type PubAckResponse struct {
	APIResponse
	*PubAck
}

type PubAck struct {
	Stream    string `json:"stream"`
	Sequence  uint64 `json:"seq"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

// JSApiStreamCreateResponse stream creation.
// type JSApiStreamCreateResponse struct {
// 	APIResponse
// 	*StreamInfo
// }
// 
// type JSApiStreamInfoResponse = JSApiStreamCreateResponse

// APIPaged includes variables used to create paged responses from the JSON API
type APIPaged struct {
	Total  int `json:"total"`
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}

type JSApiStreamNamesResponse struct {
	APIResponse
	APIPaged
	Streams []string `json:"streams"`
}

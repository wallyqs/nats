// Copyright 2021 The NATS Authors
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

package nats

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// JetStreamManager is the public interface for managing JetStream streams & consumers.
type JetStreamManager interface {
	// Create a stream.
	AddStream(cfg *StreamConfig) (*StreamInfo, error)

	// Update a stream.
	UpdateStream(cfg *StreamConfig) (*StreamInfo, error)

	// Delete a stream.
	DeleteStream(name string) error

	// Stream information.
	StreamInfo(stream string) (*StreamInfo, error)

	// Purge stream messages.
	PurgeStream(name string) error

	// NewStreamLister is used to return pages of StreamInfo objects.
	NewStreamLister() *StreamLister

	// DeleteMsg erases a message from a Stream.
	DeleteMsg(name string, seq uint64) error

	// Create a consumer.
	AddConsumer(stream string, cfg *ConsumerConfig) (*ConsumerInfo, error)

	// Delete a consumer.
	DeleteConsumer(stream, consumer string) error

	// Consumer information.
	ConsumerInfo(stream, name string) (*ConsumerInfo, error)

	// NewConsumerLister is used to return pages of ConsumerInfo objects.
	NewConsumerLister(stream string) *ConsumerLister
}

// StreamConfig will determine the properties for a stream.
// There are sensible defaults for most. If no subjects are
// given the name will be used as the only subject.
type StreamConfig struct {
	Name         string          `json:"name"`
	Subjects     []string        `json:"subjects,omitempty"`
	Retention    RetentionPolicy `json:"retention"`
	MaxConsumers int             `json:"max_consumers"`
	MaxMsgs      int64           `json:"max_msgs"`
	MaxBytes     int64           `json:"max_bytes"`
	Discard      DiscardPolicy   `json:"discard"`
	MaxAge       time.Duration   `json:"max_age"`
	MaxMsgSize   int32           `json:"max_msg_size,omitempty"`
	Storage      StorageType     `json:"storage"`
	Replicas     int             `json:"num_replicas"`
	NoAck        bool            `json:"no_ack,omitempty"`
	Template     string          `json:"template_owner,omitempty"`
	Duplicates   time.Duration   `json:"duplicate_window,omitempty"`
}

// apiError is included in all API responses if there was an error.
type apiError struct {
	Code        int    `json:"code"`
	Description string `json:"description,omitempty"`
}

// apiResponse is a standard response from the JetStream JSON API.
type apiResponse struct {
	Type  string    `json:"type"`
	Error *apiError `json:"error,omitempty"`
}

// apiPaged includes variables used to create paged responses from the JSON API.
type apiPaged struct {
	Total  int `json:"total"`
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}

// apiPagedRequest includes parameters allowing specific pages to be requested
// from APIs responding with apiPaged.
type apiPagedRequest struct {
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}

// accountStats returns current statistics about the account's JetStream usage.
type accountStats struct {
	Memory  uint64 `json:"memory"`
	Store   uint64 `json:"storage"`
	Streams int    `json:"streams"`
	Limits  struct {
		MaxMemory    int64 `json:"max_memory"`
		MaxStore     int64 `json:"max_storage"`
		MaxStreams   int   `json:"max_streams"`
		MaxConsumers int   `json:"max_consumers"`
	} `json:"limits"`
}

type accountInfoResponse struct {
	apiResponse
	accountStats
}

type createConsumerRequest struct {
	Stream string          `json:"stream_name"`
	Config *ConsumerConfig `json:"config"`
}

type consumerResponse struct {
	apiResponse
	*ConsumerInfo
}

// AddConsumer will add a JetStream consumer.
func (js *js) AddConsumer(stream string, cfg *ConsumerConfig) (*ConsumerInfo, error) {
	if stream == _EMPTY_ {
		return nil, ErrStreamNameRequired
	}
	req, err := json.Marshal(&createConsumerRequest{Stream: stream, Config: cfg})
	if err != nil {
		return nil, err
	}

	var ccSubj string
	if cfg != nil && cfg.Durable != _EMPTY_ {
		if strings.Contains(cfg.Durable, ".") {
			return nil, ErrInvalidDurableName
		}
		ccSubj = fmt.Sprintf(apiDurableCreateT, stream, cfg.Durable)
	} else {
		ccSubj = fmt.Sprintf(apiConsumerCreateT, stream)
	}

	resp, err := js.nc.Request(js.apiSubj(ccSubj), req, js.wait)
	if err != nil {
		if err == ErrNoResponders {
			err = ErrJetStreamNotEnabled
		}
		return nil, err
	}
	var info consumerResponse
	err = json.Unmarshal(resp.Data, &info)
	if err != nil {
		return nil, err
	}
	if info.Error != nil {
		return nil, errors.New(info.Error.Description)
	}
	return info.ConsumerInfo, nil
}

// consumerDeleteResponse is the response for a Consumer delete request.
type consumerDeleteResponse struct {
	apiResponse
	Success bool `json:"success,omitempty"`
}

// DeleteConsumer deletes a Consumer.
func (js *js) DeleteConsumer(stream, durable string) error {
	if stream == _EMPTY_ {
		return ErrStreamNameRequired
	}

	dcSubj := js.apiSubj(fmt.Sprintf(apiConsumerDeleteT, stream, durable))
	r, err := js.nc.Request(dcSubj, nil, js.wait)
	if err != nil {
		return err
	}
	var resp consumerDeleteResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return errors.New(resp.Error.Description)
	}
	return nil
}

// ConsumerInfo returns information about a Consumer.
func (js *js) ConsumerInfo(stream, durable string) (*ConsumerInfo, error) {
	return js.getConsumerInfo(stream, durable)
}

// ConsumerLister fetches pages of ConsumerInfo objects. This object is not
// safe to use for multiple threads.
type ConsumerLister struct {
	stream string
	js     *js

	err      error
	offset   int
	page     []*ConsumerInfo
	pageInfo *apiPaged
}

// consumersRequest is the type used for Consumers requests.
type consumersRequest struct {
	apiPagedRequest
}

// consumerListResponse is the response for a Consumers List request.
type consumerListResponse struct {
	apiResponse
	apiPaged
	Consumers []*ConsumerInfo `json:"consumers"`
}

// Next fetches the next ConsumerInfo page.
func (c *ConsumerLister) Next() bool {
	if c.err != nil {
		return false
	}
	if c.stream == _EMPTY_ {
		c.err = ErrStreamNameRequired
		return false
	}
	if c.pageInfo != nil && c.offset >= c.pageInfo.Total {
		return false
	}

	req, err := json.Marshal(consumersRequest{
		apiPagedRequest: apiPagedRequest{Offset: c.offset},
	})
	if err != nil {
		c.err = err
		return false
	}
	clSubj := c.js.apiSubj(fmt.Sprintf(apiConsumerListT, c.stream))
	r, err := c.js.nc.Request(clSubj, req, c.js.wait)
	if err != nil {
		c.err = err
		return false
	}
	var resp consumerListResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		c.err = err
		return false
	}
	if resp.Error != nil {
		c.err = errors.New(resp.Error.Description)
		return false
	}

	c.pageInfo = &resp.apiPaged
	c.page = resp.Consumers
	c.offset += len(c.page)
	return true
}

// Page returns the current ConsumerInfo page.
func (c *ConsumerLister) Page() []*ConsumerInfo {
	return c.page
}

// Err returns any errors found while fetching pages.
func (c *ConsumerLister) Err() error {
	return c.err
}

// NewConsumerLister is used to return pages of ConsumerInfo objects.
func (js *js) NewConsumerLister(stream string) *ConsumerLister {
	return &ConsumerLister{stream: stream, js: js}
}

// streamCreateResponse stream creation.
type streamCreateResponse struct {
	apiResponse
	*StreamInfo
}

func (js *js) AddStream(cfg *StreamConfig) (*StreamInfo, error) {
	if cfg == nil || cfg.Name == _EMPTY_ {
		return nil, ErrStreamNameRequired
	}

	req, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	csSubj := js.apiSubj(fmt.Sprintf(apiStreamCreateT, cfg.Name))
	r, err := js.nc.Request(csSubj, req, js.wait)
	if err != nil {
		return nil, err
	}
	var resp streamCreateResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, errors.New(resp.Error.Description)
	}
	return resp.StreamInfo, nil
}

type streamInfoResponse = streamCreateResponse

func (js *js) StreamInfo(stream string) (*StreamInfo, error) {
	csSubj := js.apiSubj(fmt.Sprintf(apiStreamInfoT, stream))
	r, err := js.nc.Request(csSubj, nil, js.wait)
	if err != nil {
		return nil, err
	}
	var resp streamInfoResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, errors.New(resp.Error.Description)
	}
	return resp.StreamInfo, nil
}

// StreamInfo shows config and current state for this stream.
type StreamInfo struct {
	Config  StreamConfig `json:"config"`
	Created time.Time    `json:"created"`
	State   StreamState  `json:"state"`
}

// StreamStats is information about the given stream.
type StreamState struct {
	Msgs      uint64    `json:"messages"`
	Bytes     uint64    `json:"bytes"`
	FirstSeq  uint64    `json:"first_seq"`
	FirstTime time.Time `json:"first_ts"`
	LastSeq   uint64    `json:"last_seq"`
	LastTime  time.Time `json:"last_ts"`
	Consumers int       `json:"consumer_count"`
}

// UpdateStream updates a Stream.
func (js *js) UpdateStream(cfg *StreamConfig) (*StreamInfo, error) {
	if cfg == nil || cfg.Name == _EMPTY_ {
		return nil, ErrStreamNameRequired
	}

	req, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	usSubj := js.apiSubj(fmt.Sprintf(apiStreamUpdateT, cfg.Name))
	r, err := js.nc.Request(usSubj, req, js.wait)
	if err != nil {
		return nil, err
	}
	var resp streamInfoResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, errors.New(resp.Error.Description)
	}
	return resp.StreamInfo, nil
}

// streamDeleteResponse is the response for a Stream delete request.
type streamDeleteResponse struct {
	apiResponse
	Success bool `json:"success,omitempty"`
}

// DeleteStream deletes a Stream.
func (js *js) DeleteStream(name string) error {
	if name == _EMPTY_ {
		return ErrStreamNameRequired
	}

	dsSubj := js.apiSubj(fmt.Sprintf(apiStreamDeleteT, name))
	r, err := js.nc.Request(dsSubj, nil, js.wait)
	if err != nil {
		return err
	}
	var resp streamDeleteResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return errors.New(resp.Error.Description)
	}
	return nil
}

type msgDeleteRequest struct {
	Seq uint64 `json:"seq"`
}

// msgDeleteResponse is the response for a Stream delete request.
type msgDeleteResponse struct {
	apiResponse
	Success bool `json:"success,omitempty"`
}

// DeleteMsg deletes a message from a stream.
func (js *js) DeleteMsg(name string, seq uint64) error {
	if name == _EMPTY_ {
		return ErrStreamNameRequired
	}

	req, err := json.Marshal(&msgDeleteRequest{Seq: seq})
	if err != nil {
		return err
	}

	dsSubj := js.apiSubj(fmt.Sprintf(apiMsgDeleteT, name))
	r, err := js.nc.Request(dsSubj, req, js.wait)
	if err != nil {
		return err
	}
	var resp msgDeleteResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return errors.New(resp.Error.Description)
	}
	return nil
}

type streamPurgeResponse struct {
	apiResponse
	Success bool   `json:"success,omitempty"`
	Purged  uint64 `json:"purged"`
}

// PurgeStream purges messages on a Stream.
func (js *js) PurgeStream(name string) error {
	psSubj := js.apiSubj(fmt.Sprintf(apiStreamPurgeT, name))
	r, err := js.nc.Request(psSubj, nil, js.wait)
	if err != nil {
		return err
	}
	var resp streamPurgeResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return errors.New(resp.Error.Description)
	}
	return nil
}

// streamNamesRequest is used for Stream Name requests.
type streamNamesRequest struct {
	apiPagedRequest
	// These are filters that can be applied to the list.
	Subject string `json:"subject,omitempty"`
}

// streamListResponse list of detailed stream information.
// A nil request is valid and means all streams.
type streamListResponse struct {
	apiResponse
	apiPaged
	Streams []*StreamInfo `json:"streams"`
}

// StreamLister fetches pages of StreamInfo objects. This object is not safe
// to use for multiple threads.
type StreamLister struct {
	js   *js
	page []*StreamInfo
	err  error

	offset   int
	pageInfo *apiPaged
}

// Next fetches the next StreamInfo page.
func (s *StreamLister) Next() bool {
	if s.err != nil {
		return false
	}
	if s.pageInfo != nil && s.offset >= s.pageInfo.Total {
		return false
	}
	req, err := json.Marshal(streamNamesRequest{
		apiPagedRequest: apiPagedRequest{Offset: s.offset},
	})
	if err != nil {
		s.err = err
		return false
	}

	slSubj := s.js.apiSubj(apiStreamList)
	r, err := s.js.nc.Request(slSubj, req, s.js.wait)
	if err != nil {
		s.err = err
		return false
	}
	var resp streamListResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		s.err = err
		return false
	}
	if resp.Error != nil {
		s.err = errors.New(resp.Error.Description)
		return false
	}

	s.pageInfo = &resp.apiPaged
	s.page = resp.Streams
	s.offset += len(s.page)
	fmt.Println(s.pageInfo.Total, s.page)
	return true
}

// NextMsg 
func (s *StreamLister) NextMsg() bool {
	return false
}

// Page returns the current StreamInfo page.
func (s *StreamLister) Page() []*StreamInfo {
	return s.page
}

// Err returns any errors found while fetching pages.
func (s *StreamLister) Err() error {
	return s.err
}

// NewStreamLister is used to return pages of StreamInfo objects.
func (js *js) NewStreamLister() *StreamLister {
	return &StreamLister{js: js}
}

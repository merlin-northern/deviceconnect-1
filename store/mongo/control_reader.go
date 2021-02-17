// Copyright 2021 Northern.tech AS
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package mongo

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/mendersoftware/deviceconnect/app"
	"github.com/mendersoftware/deviceconnect/model"
)

const (
	controlReadBufferSize = 4096
)

type ControlMessageReader struct {
	currentOffset uint64
	output        []byte
	outputLength  int
	buffer        bytes.Buffer
	c             *mongo.Cursor
	gzipReader    *gzip.Reader
}

func NewControlMessageReader(ctx context.Context, c *mongo.Cursor) *ControlMessageReader {
	reader := &ControlMessageReader{
		currentOffset: 0,
		output:        make([]byte, controlReadBufferSize),
		buffer:        bytes.Buffer{},
		c:             c,
		outputLength:  0,
	}

	c.Next(ctx)
	var r model.ControlData
	err := c.Decode(&r)
	if err != nil {
		return nil
	}

	reader.buffer.Reset()
	reader.buffer.Write(r.Control)
	gzipReader, e := gzip.NewReader(&reader.buffer)
	if e != nil {
		err = e
	}

	n, e := gzipReader.Read(reader.output)
	reader.gzipReader = gzipReader
	reader.outputLength = n
	if n == 0 || e != nil {
		return reader
	}

	return reader
}

func (r *ControlMessageReader) Pop() *app.Control {
	if r.outputLength < 3 { // at least we have to have type: 1 byte, and two bytes of offset
		n, e := r.gzipReader.Read(r.output[r.outputLength:])
		if n == 0 || e != nil {
			return nil
		}
		r.outputLength += n
	}

	m := &app.Control{}
	offset := 0
	//now here we can start deserializing the control messages
	//output[:n] contains the uncompressed buffer
	// +---------+----------+---------+
	// | type: 1 | offset:2 | data: l |
	// +---------+----------+---------+
	// where l is type-dependent
	controlMessageBuffer := r.output[:r.outputLength]
	switch controlMessageBuffer[offset] {
	case app.DelayMessage:
		offset++
		recordingOffset := binary.LittleEndian.Uint16(controlMessageBuffer[offset:])
		offset++
		offset++
		delayMilliSeconds := binary.LittleEndian.Uint16(controlMessageBuffer[offset:])
		offset++
		offset++
		m.Type = app.DelayMessage
		m.Offset = recordingOffset
		m.DelayMs = delayMilliSeconds
	case app.ResizeMessage:
		offset++
		recordingOffset := binary.LittleEndian.Uint16(controlMessageBuffer[offset:])
		offset++
		offset++
		width := binary.LittleEndian.Uint16(controlMessageBuffer[offset:])
		offset++
		offset++
		height := binary.LittleEndian.Uint16(controlMessageBuffer[offset:])
		offset++
		offset++
		m.Type = app.DelayMessage
		m.Offset = recordingOffset
		m.TerminalWidth = width
		m.TerminalHeight = height
	default:
		return nil
	}

	return m
}

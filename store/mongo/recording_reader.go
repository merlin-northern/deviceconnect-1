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
	"errors"
	"io"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/mendersoftware/deviceconnect/model"
)

var (
	ErrNoData = errors.New("no data read")
)

type RecordingReader struct {
	ctx           context.Context
	currentOffset uint64
	buffer        bytes.Buffer
	c             *mongo.Cursor
	output        []byte
	gzipReader    *gzip.Reader
}

func NewRecordingReader(ctx context.Context, c *mongo.Cursor) *RecordingReader {
	r := &RecordingReader{
		ctx:           ctx,
		currentOffset: 0,
		buffer:        bytes.Buffer{},
		c:             c,
	}

	return r
}

func (rr *RecordingReader) Read(buffer []byte) error {
	bufferLength := len(buffer)
	if len(rr.output) < 1 {
		// the buffer is empty, either we are at the beginning or all of the buffer was sent
		if rr.gzipReader != nil {
			// this means that not whole buffer was decompressed, but the the fact that recordingReadBufferSize
			// did not have capacity to hold all of decompressed data, we have to continue reading from the gzip stream
			rr.output = make([]byte, recordingReadBufferSize)
			n, e := rr.gzipReader.Read(rr.output)

			if e != nil && e != io.EOF {
				rr.output=[]byte{}
				rr.gzipReader.Close()
				rr.gzipReader = nil
				return e
			}

			if n == 0 || e == io.EOF {
				rr.output=[]byte{}
				rr.gzipReader.Close()
				rr.gzipReader = nil
				//this is EOF from the gzip stream, which means we decompressed it all
				return ErrNoData
			}

			if bufferLength > n {
				copy(buffer, rr.output[:n])
				rr.output = []byte{}
			} else {
				copy(buffer, rr.output[:bufferLength])
				rr.output = rr.output[bufferLength:n]
			}

			return nil
		}

		hasNext := rr.c.Next(rr.ctx)
		if !hasNext {
			//this means that there are no records to read, in here we definitely read all there was
			return io.EOF
		}

		var r model.Recording
		err := rr.c.Decode(&r)
		if err != nil {
			return err
		}

		rr.buffer.Reset()
		n, e := rr.buffer.Write(r.Recording)

		gzipReader, err := gzip.NewReader(&rr.buffer)
		if err != nil {
			return err
		}

		rr.output = make([]byte, recordingReadBufferSize)
		n, e = gzipReader.Read(rr.output)

		if e != nil && e != io.EOF {
			rr.output=[]byte{}
			rr.gzipReader = nil
			gzipReader.Close()
			return e
		}

		if n == 0 {
			rr.output=[]byte{}
			rr.gzipReader = nil
			gzipReader.Close()
			return io.EOF
		}

		rr.gzipReader = gzipReader
		if bufferLength > n {
			copy(buffer, rr.output[:n])
			rr.output = []byte{}
		} else {
			copy(buffer, rr.output[:bufferLength])
			rr.output = rr.output[bufferLength:n]
		}
	} else {
		if bufferLength > len(rr.output) {
			copy(buffer, rr.output)
			rr.output = []byte{}
		} else {
			copy(buffer, rr.output[:bufferLength])
			rr.output = rr.output[bufferLength:]
		}
	}

	return nil
}

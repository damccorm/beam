// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package exec

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

type UserStateAdapter interface {
	NewStateProvider(ctx context.Context, reader StateReader, w typex.Window, element interface{}) (stateProvider, error)
}

type userStateAdapter struct {
	sid            StreamID
	wc             WindowEncoder
	kc             ElementEncoder
	ec             ElementDecoder
	stateIdToCoder map[string]*coder.Coder
	c              *coder.Coder
}

// NewUserStateAdapter returns a user state adapter for the given StreamID and coder.
// It expects a W<V> or W<KV<K,V>> coder, because the protocol requires windowing information.
func NewUserStateAdapter(sid StreamID, c *coder.Coder, stateIdToCoder map[string]*coder.Coder) UserStateAdapter {
	if !coder.IsW(c) {
		panic(fmt.Sprintf("expected WV coder for user state %v: %v", sid, c))
	}

	wc := MakeWindowEncoder(c.Window)
	var kc ElementEncoder
	var ec ElementDecoder
	// TODO - revisit this logic - does this work? Maybe more descriptive vars
	if coder.IsKV(coder.SkipW(c)) {
		kc = MakeElementEncoder(coder.SkipW(c).Components[0])
		ec = MakeElementDecoder(coder.SkipW(c).Components[1])
	} else {
		ec = MakeElementDecoder(coder.SkipW(c))
	}
	return &userStateAdapter{sid: sid, wc: wc, kc: kc, ec: ec, c: c, stateIdToCoder: stateIdToCoder}
}

// TODO - header comment
func (s *userStateAdapter) NewStateProvider(ctx context.Context, reader StateReader, w typex.Window, element interface{}) (stateProvider, error) {
	// TODO - revisit this with the above. Does this work in all cases?
	elementKey, err := EncodeElement(s.kc, element.(*MainInput).Key.Elm) // Failing here
	if err != nil {
		return stateProvider{}, err
	}

	win, err := EncodeWindow(s.wc, w)
	if err != nil {
		return stateProvider{}, err
	}
	sp := stateProvider{
		ctx:                ctx,
		sr:                 reader,
		SID:                s.sid,
		elementKey:         elementKey,
		window:             win,
		transactionsByKey:  make(map[string][]state.Transaction),
		initialValueByKey:  make(map[string]interface{}),
		readerWritersByKey: make(map[string]io.ReadWriteCloser),
		codersByKey:        s.stateIdToCoder,
	}

	return sp, nil
}

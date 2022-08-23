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

// Package state contains structs for reading and manipulating pipeline state.
package state

import (
	"fmt"
	"reflect"
)

// TransactionTypeEnum represents the type of state transaction (e.g. set, clear)
type TransactionTypeEnum int32

// StateTypeEnum represents the type of a state instance (e.g. value, bag, etc...)
type StateTypeEnum int32

const (
	// TransactionTypeSet is the set transaction type
	TransactionTypeSet TransactionTypeEnum = 0
	// TransactionTypeClear is the set transaction type
	TransactionTypeClear TransactionTypeEnum = 1
	// TransactionTypeAppend is the append transaction type
	TransactionTypeAppend TransactionTypeEnum = 2
	// StateTypeValue represents a value state
	StateTypeValue StateTypeEnum = 0
	// StateTypeBag represents a bag state
	StateTypeBag StateTypeEnum = 1
	// StateTypeCombining represents a combining state
	StateTypeCombining StateTypeEnum = 2
)

var (
	// ProviderType is the state provider type
	ProviderType = reflect.TypeOf((*Provider)(nil)).Elem()
)

// TODO(#20510) - add other forms of state (MapState, BagState, CombiningState), prefetch, and clear.

// Transaction is used to represent a pending state transaction. This should not be manipulated directly;
// it is primarily used for implementations of the Provider interface to talk to the various State objects.
type Transaction struct {
	Key  string
	Type TransactionTypeEnum
	Val  interface{}
}

// Provider represents the DoFn parameter used to get and manipulate pipeline state
// stored as key value pairs (https://beam.apache.org/documentation/programming-guide/#state-and-timers).
// This should not be manipulated directly. Instead it should be used as a parameter
// to functions on State objects like state.Value.
type Provider interface {
	ReadValueState(id string) (interface{}, []Transaction, error)
	WriteValueState(val Transaction) error
	ReadBagState(id string) ([]interface{}, []Transaction, error)
	WriteBagState(val Transaction) error
}

// PipelineState is an interface representing different kinds of PipelineState (currently just state.Value).
// It is primarily meant for Beam packages to use and is probably not useful for most pipeline authors.
type PipelineState interface {
	StateKey() string
	CoderType() reflect.Type
	StateType() StateTypeEnum
}

// Value is used to read and write global pipeline state representing a single value.
// Key represents the key used to lookup this state.
type Value[T any] struct {
	Key string
}

// Write is used to write this instance of global pipeline state representing a single value.
func (s *Value[T]) Write(p Provider, val T) error {
	return p.WriteValueState(Transaction{
		Key:  s.Key,
		Type: TransactionTypeSet,
		Val:  val,
	})
}

// Read is used to read this instance of global pipeline state representing a single value.
// When a value is not found, returns the 0 value and false.
func (s *Value[T]) Read(p Provider) (T, bool, error) {
	// This replays any writes that have happened to this value since we last read
	// For more detail, see "State Transactionality" below for buffered transactions
	cur, bufferedTransactions, err := p.ReadValueState(s.Key)
	if err != nil {
		var val T
		return val, false, err
	}
	for _, t := range bufferedTransactions {
		switch t.Type {
		case TransactionTypeSet:
			cur = t.Val
		case TransactionTypeClear:
			cur = nil
		}
	}
	if cur == nil {
		var val T
		return val, false, nil
	}
	return cur.(T), true, nil
}

// StateKey returns the key for this pipeline state entry.
func (s Value[T]) StateKey() string {
	if s.Key == "" {
		// TODO(#22736) - infer the state from the member variable name during pipeline construction.
		panic("Value state exists on struct but has not been initialized with a key.")
	}
	return s.Key
}

// CoderType returns the type of the value state which should be used for a coder.
func (s Value[T]) CoderType() reflect.Type {
	var t T
	return reflect.TypeOf(t)
}

// StateType returns the type of the state (in this case always Value).
func (s Value[T]) StateType() StateTypeEnum {
	return StateTypeValue
}

// MakeValueState is a factory function to create an instance of ValueState with the given key.
func MakeValueState[T any](k string) Value[T] {
	return Value[T]{
		Key: k,
	}
}

// Bag is used to read and write global pipeline state representing a collection of values.
// Key represents the key used to lookup this state.
type Bag[T any] struct {
	Key string
}

// Add is used to write append to the bag pipeline state.
func (s *Bag[T]) Add(p Provider, val T) error {
	return p.WriteBagState(Transaction{
		Key:  s.Key,
		Type: TransactionTypeAppend,
		Val:  val,
	})
}

// Read is used to read this instance of global pipeline state representing a bag.
// When a value is not found, returns an empty list and false.
func (s *Bag[T]) Read(p Provider) ([]T, bool, error) {
	// This replays any writes that have happened to this value since we last read
	// For more detail, see "State Transactionality" below for buffered transactions
	initialValue, bufferedTransactions, err := p.ReadBagState(s.Key)
	if err != nil {
		var val []T
		return val, false, err
	}
	cur := []T{}
	for _, v := range initialValue {
		cur = append(cur, v.(T))
	}
	for _, t := range bufferedTransactions {
		switch t.Type {
		case TransactionTypeAppend:
			cur = append(cur, t.Val.(T))
		case TransactionTypeClear:
			cur = []T{}
		}
	}
	if len(cur) == 0 {
		return cur, false, nil
	}
	return cur, true, nil
}

// StateKey returns the key for this pipeline state entry.
func (s Bag[T]) StateKey() string {
	if s.Key == "" {
		// TODO(#22736) - infer the state from the member variable name during pipeline construction.
		panic("Value state exists on struct but has not been initialized with a key.")
	}
	return s.Key
}

// CoderType returns the type of the bag state which should be used for a coder.
func (s Bag[T]) CoderType() reflect.Type {
	var t T
	return reflect.TypeOf(t)
}

// StateType returns the type of the state (in this case always Bag).
func (s Bag[T]) StateType() StateTypeEnum {
	return StateTypeBag
}

// MakeBagState is a factory function to create an instance of BagState with the given key.
func MakeBagState[T any](k string) Bag[T] {
	return Bag[T]{
		Key: k,
	}
}

// CreateAccumulator represents a struct that implements the CreateAccumulator Combine operation
type CreateAccumulator[T1 any] interface {
	CreateAccumulator() T1
}

// AddInput represents a struct that implements the AddInput Combine operation
type AddInput[T1, T2 any] interface {
	AddInput(T1, T2) T1
}

// ExtractOutput represents a struct that implements the ExtractOutput Combine operation
type ExtractOutput[T1, T3 any] interface {
	ExtractOutput(T1) T3
}

// Combining is used to read and write global pipeline state representing a single combined value.
// It uses 3 generic values, [T1, T2, T3], to represent the accumulator, input, and output types respectively.
// Key represents the key used to lookup this state.
type Combining[T1, T2, T3 any] struct {
	Key                     string
	createAccumulatorStruct *CreateAccumulator[T1]
	addInputStruct          *AddInput[T1, T2]
	extractOutputStruct     *ExtractOutput[T1, T3]
	AddInputFn              func(T1, T2) T1
}

// Add is used to write add an element to the combining pipeline state.
func (s *Combining[T1, T2, T3]) Add(p Provider, val T2) error {
	// We will always maintain a single accumulated value as a value state.
	// Therefore, when we add we must first read the current accumulator so that we can add to it.
	acc, ok, err := s.readAccumulator(p)
	if err != nil {
		return err
	}
	if !ok {
		// If no accumulator, that means that the CreateAccumulator function doesn't exist
		// and our value is our initial accumulator.
		return p.WriteValueState(Transaction{
			Key:  s.Key,
			Type: TransactionTypeSet,
			Val:  acc,
		})
	}

	if s.addInputStruct != nil {
		return p.WriteValueState(Transaction{
			Key:  s.Key,
			Type: TransactionTypeSet,
			Val:  (*s.addInputStruct).AddInput(acc.(T1), val),
		})
	} else if s.AddInputFn != nil {
		return p.WriteValueState(Transaction{
			Key:  s.Key,
			Type: TransactionTypeSet,
			Val:  s.AddInputFn(acc.(T1), val),
		})
	}
	panic(fmt.Sprintf("AddInput must be defined on accumulator %v", s))
}

// Read is used to read this instance of global pipeline state representing a combiner.
// When a value is not found, returns an empty list and false.
func (s *Combining[T1, T2, T3]) Read(p Provider) (T3, bool, error) {
	acc, ok, err := s.readAccumulator(p)
	if !ok || err != nil {
		var val T3
		return val, ok, err
	}

	if s.extractOutputStruct != nil {
		return (*s.extractOutputStruct).ExtractOutput(acc.(T1)), true, nil
	}

	return acc.(T3), true, nil
}

func (s *Combining[T1, T2, T3]) readAccumulator(p Provider) (interface{}, bool, error) {
	// This replays any writes that have happened to this value since we last read
	// For more detail, see "State Transactionality" below for buffered transactions
	cur, bufferedTransactions, err := p.ReadValueState(s.Key)
	if err != nil {
		var val T1
		return val, false, err
	}
	for _, t := range bufferedTransactions {
		switch t.Type {
		case TransactionTypeSet:
			cur = t.Val
		case TransactionTypeClear:
			cur = nil
		}
	}
	if cur == nil {
		if s.createAccumulatorStruct != nil {
			return (*s.createAccumulatorStruct).CreateAccumulator(), false, nil
		}
		var val T1
		return val, false, nil
	}

	return cur, true, nil
}

// StateKey returns the key for this pipeline state entry.
func (s Combining[T1, T2, T3]) StateKey() string {
	if s.Key == "" {
		// TODO(#22736) - infer the state from the member variable name during pipeline construction.
		panic("Value state exists on struct but has not been initialized with a key.")
	}
	return s.Key
}

// CoderType returns the type of the bag state which should be used for a coder.
func (s Combining[T1, T2, T3]) CoderType() reflect.Type {
	var t T1
	return reflect.TypeOf(t)
}

// StateType returns the type of the state (in this case always Bag).
func (s Combining[T1, T2, T3]) StateType() StateTypeEnum {
	return StateTypeCombining
}

// MakeCombiningState is a factory function to create an instance of Combining state with the given key and combiner
// when the combiner may have different types for its accumulator, input, and output.
// Takes 3 generic constraints [T1, T2, T3 any] representing the accumulator/input/output types respectively.
// If no accumulator or output types are defined, use the input type.
func MakeCombiningState[T1, T2, T3 any](k string, combiner interface{}) Combining[T1, T2, T3] {
	if cmb, ok := combiner.(func(T1, T2) T1); ok {
		return Combining[T1, T2, T3]{
			Key:        k,
			AddInputFn: cmb,
		}
	}

	c := Combining[T1, T2, T3]{
		Key: k,
	}

	if ca, ok := combiner.(CreateAccumulator[T1]); ok {
		c.createAccumulatorStruct = &ca
	} else {
		// TODO - validate that no create accumulator function is defined
	}
	if ai, ok := combiner.(AddInput[T1, T2]); ok {
		c.AddInputFn = ai.AddInput
	} else {
		var t1 T1
		var t2 T2
		type1 := reflect.TypeOf(t1)
		type2 := reflect.TypeOf(t2)
		panic(fmt.Sprintf("Combiner struct %v must have AddInput(%v, %v) %v defined", combiner, type1, type2, type1))
	}
	if ea, ok := combiner.(ExtractOutput[T1, T3]); ok {
		// fn := ea.ExtractOutput
		c.extractOutputStruct = &ea
	} else {
		// TODO - validate that no extractoutput function is defined
	}

	return c
}

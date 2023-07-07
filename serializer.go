package beeorm

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"unsafe"
)

type serializer struct {
	scratch [binary.MaxVarintLen64]byte
	buffer  *bytes.Buffer
}

func (s *serializer) Read() []byte {
	b := make([]byte, s.buffer.Len())
	copy(b, s.buffer.Bytes())
	s.buffer.Reset()
	return b
}

func (s *serializer) Reset(p []byte) {
	s.buffer.Reset()
	s.buffer.Write(p)
}

func (s *serializer) SerializeUInteger(v uint64) {
	ln := binary.PutUvarint(s.scratch[:binary.MaxVarintLen64], v)
	_, _ = s.buffer.Write(s.scratch[0:ln])
}

func (s *serializer) SerializeInteger(v int64) {
	ln := binary.PutVarint(s.scratch[:binary.MaxVarintLen64], v)
	_, _ = s.buffer.Write(s.scratch[0:ln])
}

func (s *serializer) SerializeBool(v bool) {
	if v {
		s.scratch[0] = 1
		_, _ = s.buffer.Write(s.scratch[:1])
		return
	}
	s.scratch[0] = 0
	_, _ = s.buffer.Write(s.scratch[:1])
}

func (s *serializer) SerializeFloat(v float64) {
	s.SerializeUInteger(math.Float64bits(v))
}

func (s *serializer) SerializeString(v string) {
	str := s.str2Bytes(v)
	l := len(str)
	s.SerializeUInteger(uint64(l))
	if l > 0 {
		_, _ = s.buffer.Write(str)
	}
}

func (s *serializer) SerializeBytes(val []byte) {
	l := len(val)
	s.SerializeUInteger(uint64(l))
	if l > 0 {
		_, _ = s.buffer.Write(val)
	}
}

func (s *serializer) DeserializeBool() bool {
	v, _ := s.ReadByte()
	return v == 1
}

func (s *serializer) DeserializeUInteger() uint64 {
	v, _ := binary.ReadUvarint(s)
	return v
}

func (s *serializer) DeserializeInteger() int64 {
	v, _ := binary.ReadVarint(s)
	return v
}

func (s *serializer) DeserializeFloat() float64 {
	return math.Float64frombits(s.DeserializeUInteger())
}

func (s *serializer) DeserializeFixed(ln int) []byte {
	return s.buffer.Next(ln)
}

func (s *serializer) DeserializeString() string {
	l := s.DeserializeUInteger()
	if l == 0 {
		return ""
	}
	val := string(s.DeserializeFixed(int(l)))
	return val
}

func (s *serializer) DeserializeBytes() []byte {
	l := s.DeserializeUInteger()
	if l == 0 {
		return nil
	}
	return s.DeserializeFixed(int(l))
}

func (s *serializer) ReadByte() (byte, error) {
	_, _ = s.buffer.Read(s.scratch[:1])
	return s.scratch[0], nil
}

func (s *serializer) str2Bytes(str string) []byte {
	if len(str) == 0 {
		return nil
	}
	var b []byte
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteHeader.Data = (*reflect.StringHeader)(unsafe.Pointer(&str)).Data
	l := len(str)
	byteHeader.Len = l
	byteHeader.Cap = l
	return b
}

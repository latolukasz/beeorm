package beeorm

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"strconv"
	"unsafe"
)

func (e *Engine) bufferInit(p []byte) {
	if e.buffer == nil {
		e.buffer = bytes.NewBuffer(p)
	} else {
		e.buffer.Reset()
		if p != nil {
			e.buffer.Write(p)
		}
	}
}

func (e *Engine) bufferInitString(p string) {
	e.bufferInit([]byte(p))
}

func (e *Engine) bufferWrite(p []byte) *Engine {
	e.buffer.Write(p)
	return e
}

func (e *Engine) bufferWriteString(p string) *Engine {
	e.buffer.WriteString(p)
	return e
}

func (e *Engine) bufferWriteUint(p uint64) *Engine {
	e.buffer.WriteString(strconv.FormatUint(p, 10))
	return e
}

func (e *Engine) bufferReadString() string {
	val := e.buffer.String()
	e.buffer.Reset()
	return val
}

func (e *Engine) bufferRead() []byte {
	b := make([]byte, e.buffer.Len())
	copy(b, e.buffer.Bytes())
	e.buffer.Reset()
	return b
}

func (e *Engine) serializeUInteger(v uint64) {
	ln := binary.PutUvarint(e.scratch[:binary.MaxVarintLen64], v)
	_, _ = e.buffer.Write(e.scratch[0:ln])
}

func (e *Engine) serializeInteger(v int64) {
	ln := binary.PutVarint(e.scratch[:binary.MaxVarintLen64], v)
	_, _ = e.buffer.Write(e.scratch[0:ln])
}

func (e *Engine) serializeBool(v bool) {
	if v {
		e.scratch[0] = 1
		_, _ = e.buffer.Write(e.scratch[:1])
		return
	}
	e.scratch[0] = 0
	_, _ = e.buffer.Write(e.scratch[:1])
}

func (e *Engine) serializeFloat(v float64) {
	e.serializeUInteger(math.Float64bits(v))
}

func (e *Engine) serializeString(v string) {
	str := e.str2Bytes(v)
	l := len(str)
	e.serializeUInteger(uint64(l))
	if l > 0 {
		_, _ = e.buffer.Write(str)
	}
}

func (e *Engine) serializeBytes(val []byte) {
	l := len(val)
	e.serializeUInteger(uint64(l))
	if l > 0 {
		_, _ = e.buffer.Write(val)
	}
}

func (e *Engine) deserializeBool() bool {
	v, _ := e.ReadByte()
	return v == 1
}

func (e *Engine) deserializeUInteger() uint64 {
	v, _ := binary.ReadUvarint(e)
	return v
}

func (e *Engine) deserializeInteger() int64 {
	v, _ := binary.ReadVarint(e)
	return v
}

func (e *Engine) deserializeFloat() float64 {
	return math.Float64frombits(e.deserializeUInteger())
}

func (e *Engine) deserializeFixed(ln int) []byte {
	buf := make([]byte, ln)
	_, _ = e.buffer.Read(buf)
	return buf
}

func (e *Engine) deserializeString() string {
	l := e.deserializeUInteger()
	if l == 0 {
		return ""
	}
	return string(e.deserializeFixed(int(l)))
}

func (e *Engine) deserializeBytes() []byte {
	l := e.deserializeUInteger()
	if l == 0 {
		return nil
	}
	return e.deserializeFixed(int(l))
}

func (e *Engine) ReadByte() (byte, error) {
	_, _ = e.buffer.Read(e.scratch[:1])
	return e.scratch[0], nil
}

func (e *Engine) str2Bytes(str string) []byte {
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

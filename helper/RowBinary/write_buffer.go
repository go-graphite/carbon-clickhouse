package RowBinary

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
)

var writeBufferPool = sync.Pool{
	New: func() interface{} {
		return &WriteBuffer{}
	},
}

const WriteBufferSize = 524288

type WriteBuffer struct {
	Used      int
	Body      [WriteBufferSize]byte
	wg        *sync.WaitGroup
	errorChan chan error
}

func GetWriteBuffer() *WriteBuffer {
	return writeBufferPool.Get().(*WriteBuffer).Reset()
}

func GetWriterBufferWithConfirm(wg *sync.WaitGroup, errorChan chan error) *WriteBuffer {
	b := GetWriteBuffer()
	b.wg = wg
	if wg != nil {
		wg.Add(1)
	}
	b.errorChan = errorChan
	return b
}

func (wb *WriteBuffer) ConfirmRequired() bool {
	return wb.wg != nil
}

func (wb *WriteBuffer) Fail(err error) {
	select {
	case wb.errorChan <- err:
	default:
	}
	wb.wg.Done()
}

func (wb *WriteBuffer) Confirm() {
	wb.wg.Done()
}

func (wb *WriteBuffer) Reset() *WriteBuffer {
	wb.Used = 0
	wb.wg = nil
	wb.errorChan = nil
	return wb
}

func (wb *WriteBuffer) Len() int {
	return wb.Used
}

func (wb *WriteBuffer) FreeSize() int {
	return len(wb.Body) - wb.Used
}

func (wb *WriteBuffer) Empty() bool {
	return wb.Used == 0
}

func (wb *WriteBuffer) Bytes() []byte {
	return wb.Body[:wb.Used]
}

func (wb *WriteBuffer) Release() {
	writeBufferPool.Put(wb)
}

func (wb *WriteBuffer) WriteBytes(p []byte) {
	wb.Used += binary.PutUvarint(wb.Body[wb.Used:], uint64(len(p)))
	wb.Used += copy(wb.Body[wb.Used:], p)
}

func (wb *WriteBuffer) WriteTagged(tagged []string) {
	l := len(tagged) - 1
	if l < 0 {
		return
	}
	for i := 0; i < len(tagged); i++ {
		l += len(tagged[i])
	}
	wb.Used += binary.PutUvarint(wb.Body[wb.Used:], uint64(l))
	for i := 0; i < len(tagged); i++ {
		if i > 0 {
			if i == 1 {
				wb.Body[wb.Used] = '?'
			} else {
				if i%2 == 1 {
					wb.Body[wb.Used] = '&'
				} else {
					wb.Body[wb.Used] = '='
				}
			}
			wb.Used++
		}
		wb.Used += copy(wb.Body[wb.Used:], tagged[i])
	}
}

func (wb *WriteBuffer) WriteString(s string) {
	wb.Used += binary.PutUvarint(wb.Body[wb.Used:], uint64(len(s)))
	wb.Used += copy(wb.Body[wb.Used:], []byte(s))
}

func (wb *WriteBuffer) WriteUVarint(v uint64) {
	wb.Used += binary.PutUvarint(wb.Body[wb.Used:], v)
}

func (wb *WriteBuffer) WriteReversePath(p []byte) {
	wb.Used += binary.PutUvarint(wb.Body[wb.Used:], uint64(len(p)))

	var index int
	for {
		index = bytes.LastIndexByte(p, '.')
		if index < 0 {
			wb.Used += copy(wb.Body[wb.Used:], p)
			break
		}

		wb.Used += copy(wb.Body[wb.Used:], p[index+1:])
		wb.Body[wb.Used] = '.'
		wb.Used++
		p = p[:index]
	}
}

func (wb *WriteBuffer) WriteFloat64(value float64) {
	binary.LittleEndian.PutUint64(wb.Body[wb.Used:], math.Float64bits(value))
	wb.Used += 8
}

func (wb *WriteBuffer) WriteUint16(value uint16) {
	binary.LittleEndian.PutUint16(wb.Body[wb.Used:], value)
	wb.Used += 2
}

func (wb *WriteBuffer) WriteUint32(value uint32) {
	binary.LittleEndian.PutUint32(wb.Body[wb.Used:], value)
	wb.Used += 4
}

func (wb *WriteBuffer) WriteUint64(value uint64) {
	binary.LittleEndian.PutUint64(wb.Body[wb.Used:], value)
	wb.Used += 8
}

func (wb *WriteBuffer) Write(p []byte) {
	wb.Used += copy(wb.Body[wb.Used:], p)
}

func (wb *WriteBuffer) WriteGraphitePoint(name []byte, value float64, timestamp uint32, version uint32) {
	wb.WriteBytes(name)
	wb.WriteFloat64(value)
	wb.WriteUint32(timestamp)
	wb.WriteUint16(TimestampToDays(timestamp))
	wb.WriteUint32(version)
}

func (wb *WriteBuffer) WriteGraphitePointTagged(labels []string, value float64, timestamp uint32, version uint32) {
	wb.WriteTagged(labels)
	wb.WriteFloat64(value)
	wb.WriteUint32(timestamp)
	wb.WriteUint16(TimestampToDays(timestamp))
	wb.WriteUint32(version)
}

func (wb *WriteBuffer) CanWriteGraphitePoint(metricLen int) bool {
	// required (maxvarint{5}, name{metricLen}, value{8}, timestamp{4}, days(date){2}, version{4})
	return (WriteBufferSize - wb.Used) > (metricLen + 23)
}

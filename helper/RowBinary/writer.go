package RowBinary

import (
	"context"
	"encoding/binary"
	"io"
	"time"
)

type Writer struct {
	ctx           context.Context
	writeChan     chan *WriteBuffer
	wb            *WriteBuffer
	pointsWritten uint32
	writeErrors   uint32
	now           uint32
}

func WriteUint16(w io.Writer, value uint16) error {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], value)
	_, err := w.Write(buf[:])
	return err
}

func WriteUint32(w io.Writer, value uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	_, err := w.Write(buf[:])
	return err
}

func WriteBytes(w io.Writer, p []byte) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(p)))
	_, err := w.Write(buf[:n])
	if err != nil {
		return err
	}
	_, err = w.Write(p)
	return err
}

func NewWriter(ctx context.Context, writeChan chan *WriteBuffer) *Writer {
	return &Writer{
		writeChan: writeChan,
		wb:        nil,
		now:       uint32(time.Now().Unix()),
		ctx:       ctx,
	}
}

func (w *Writer) Now() uint32 {
	return w.now
}

func (w *Writer) Flush() {
	if w.wb != nil {
		if w.wb.Empty() {
			w.wb.Release()
		} else {
			select {
			case w.writeChan <- w.wb:
				// pass
			case <-w.ctx.Done():
				// pass
			}
		}
		w.wb = nil
	}
}

func (w *Writer) WritePoint(metric string, value float64, timestamp int64) {
	if w.wb == nil {
		w.wb = GetWriteBuffer()
	}
	if !w.wb.CanWriteGraphitePoint(len(metric)) {
		w.Flush()
		if len(metric) > WriteBufferSize-50 {
			w.writeErrors++
			return
			// return fmt.Error("metric too long (%d bytes)", len(name))
		}
		w.wb = GetWriteBuffer()
	}

	w.wb.WriteGraphitePoint(
		[]byte(metric),
		value,
		uint32(timestamp),
		w.now,
	)

	w.pointsWritten++
}

func (w *Writer) PointsWritten() uint32 {
	return w.pointsWritten
}

func (w *Writer) WriteErrors() uint32 {
	return w.writeErrors
}

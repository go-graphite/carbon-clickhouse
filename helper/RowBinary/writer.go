package RowBinary

import (
	"encoding/binary"
	"io"
)

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

package RowBinary

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/pierrec/lz4"
)

// Read all good records from unfinished RowBinary file.
type Reader struct {
	fd          *os.File
	reader      *bufio.Reader
	offset      int
	size        int
	eof         bool
	line        [524288]byte
	isReverse   bool
	zeroVersion bool
}

func (r *Reader) SetZeroVersion(v bool) {
	r.zeroVersion = v
}

func (r *Reader) Timestamp() uint32 {
	return binary.LittleEndian.Uint32(r.line[r.size-10 : r.size-6])
}

func (r *Reader) Days() uint16 {
	return binary.LittleEndian.Uint16(r.line[r.size-6 : r.size-4])
}

func (r *Reader) DaysString() string {
	return time.Unix(int64(r.Days())*24*60*60, 0).UTC().Format("2006-01-02")
}

func (r *Reader) Value() float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(r.line[r.size-18 : r.size-10]))
}

func (r *Reader) Version() uint32 {
	return binary.LittleEndian.Uint32(r.line[r.size-4 : r.size])
}

func ReverseBytes(target []byte) []byte {
	r := make([]byte, len(target))
	copy(r, target)
	reverseMetricInplace(r)
	return r
}

func reverse(m []byte) {
	i := 0
	j := len(m) - 1
	for i < j {
		m[i], m[j] = m[j], m[i]
		i++
		j--
	}
}

func reverseMetricInplace(m []byte) {
	reverse(m)

	var a, b int
	l := len(m)
	for b = 0; b < l; b++ {
		if m[b] == '.' {
			reverse(m[a:b])
			a = b + 1
		}
	}
	reverse(m[a:b])
}

func ReverseBytesTo(dst []byte, src []byte) {
	var a, b int
	l := len(src)
	p := l - 1
	for b = 0; b < l; b++ {
		if src[b] == '.' {
			copy(dst[p+1:], src[a:b])
			dst[p] = '.'
			a = b + 1
		}
		p--
	}
	if a == 0 {
		copy(dst, src)
	} else if a < b+1 {
		copy(dst[p+1:], src[a:b])
	}
}

func (r *Reader) readRecord() ([]byte, error) {
	r.size = 0
	r.offset = 0

	// read name
	namelen, err := binary.ReadUvarint(r.reader)
	if err != nil {
		return nil, err
	}

	// TODO: check namelen
	r.size = binary.PutUvarint(r.line[:], namelen)

	n, err := io.ReadFull(r.reader, r.line[r.size:r.size+int(namelen)])
	if err != nil {
		return nil, fmt.Errorf("name truncated: %s", err.Error())
	}
	if n != int(namelen) {
		return nil, errors.New("name truncated")
	}

	if r.isReverse && bytes.IndexByte(r.line[r.size:r.size+n], '?') < 0 {
		reverseMetricInplace(r.line[r.size : r.size+n])
	}

	name := r.line[r.size : r.size+n]
	r.size += n

	// read 8+4+2+4 (value{8}, timestamp{4}, days(date){2}, version{4})
	n, err = io.ReadFull(r.reader, r.line[r.size:r.size+18])
	if err != nil {
		return nil, fmt.Errorf("record truncated: %s", err.Error())
	}
	if n != 18 {
		return nil, errors.New("record truncated")
	}
	r.size += 18

	if r.zeroVersion {
		r.line[r.size-4] = '\x00'
		r.line[r.size-3] = '\x00'
		r.line[r.size-2] = '\x00'
		r.line[r.size-1] = '\x00'
	}

	if r.Days() != TimestampToDays(r.Timestamp()) {
		return nil, errors.New("date and timestamp mismatch")
	}

	return name, nil
}

func (r *Reader) ReadRecord() ([]byte, error) {
	if r.eof {
		return nil, io.EOF
	}

	p, err := r.readRecord()
	if err != nil {
		r.eof = true
		r.size = 0
		r.offset = 0
	}

	return p, err
}

func (r *Reader) Close() {
	r.fd.Close()
}

func (r *Reader) Read(p []byte) (int, error) {
	readed := 0

	for {
		if len(p) == 0 {
			return readed, nil
		}

		if r.size > r.offset {
			n := copy(p, r.line[r.offset:r.size])
			r.offset += n
			p = p[n:]
			readed += n
		} else {
			_, err := r.ReadRecord()
			if err != nil {
				if readed > 0 {
					return readed, nil
				} else {
					return 0, io.EOF
				}
			}
		}
	}
}

func NewReader(filename string, reverse bool) (*Reader, error) {
	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	var rdr io.Reader = fd
	if strings.HasSuffix(filename, lz4.Extension) {
		rdr = lz4.NewReader(fd)
	}

	return &Reader{
		fd:        fd,
		isReverse: reverse,
		reader:    bufio.NewReader(rdr),
	}, nil
}

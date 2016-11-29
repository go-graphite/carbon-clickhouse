package RowBinary

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/days1970"
)

// Read all good records from unfinished RowBinary file.
type Reader struct {
	now    uint32
	fd     *os.File
	reader *bufio.Reader
	offset int
	size   int
	eof    bool
	days   days1970.Days
	line   [524288]byte
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

	if r.Days() != r.days.TimestampWithNow(r.Timestamp(), r.now) {
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

func NewReader(filename string) (*Reader, error) {
	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return &Reader{
		fd:     fd,
		reader: bufio.NewReader(fd),
		now:    uint32(time.Now().Unix()),
	}, nil
}

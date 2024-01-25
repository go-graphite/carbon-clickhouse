package uploader

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"

	"go.uber.org/zap"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
)

type Points struct {
	*Base
	blacklist *Blacklist
	reverse   bool
}

func NewPoints(base *Base, reverse bool) *Points {
	u := &Points{Base: base}
	u.Base.handler = u.upload
	u.reverse = reverse
	u.query = fmt.Sprintf("%s (Path, Value, Time, Date, Timestamp)", u.config.TableName)

	if len(u.config.IgnoredPatterns) > 0 {
		u.blacklist = NewBlacklist(u.config.IgnoredPatterns)
	}

	return u
}

// parseAndFilter reads points data and excludes those ones which match blacklist
func (u *Points) parseAndFilter(filename string, out io.Writer) (uint64, error) {
	var n uint64

	reader, err := RowBinary.NewReader(filename, u.reverse)
	if err != nil {
		return n, err
	}

	defer reader.Close()
	reader.SetZeroVersion(u.config.ZeroTimestamp)

	wb := RowBinary.GetWriteBuffer()
	defer wb.Release()

	for {
		name, err := reader.ReadRecord()
		if err != nil { // io.EOF or corrupted file
			break
		}

		blacklisted := false
		if u.blacklist != nil && bytes.IndexByte(name, '?') == -1 { // tagged series don't undergo blacklist searching
			blacklisted = u.blacklist.Contains(unsafeString(name), u.reverse)
		}

		if !blacklisted {
			wb.Reset()
			wb.WriteGraphitePoint(name, reader.Value(), reader.Timestamp(), reader.Version())

			_, err = out.Write(wb.Bytes())
			if err != nil {
				return n, err
			}
			n++
		}
	}

	return n, nil
}

func (u *Points) upload(ctx context.Context, logger *zap.Logger, filename string) (uint64, error) {
	var (
		err, uploadErr error
		uploadResult   chan error
		n              uint64
	)

	pipeReader, pipeWriter := io.Pipe()
	out := bufio.NewWriter(pipeWriter)
	uploadResult = make(chan error, 1)

	u.Go(func(ctx context.Context) {
		logger := u.logger.With(zap.String("filename", filename))
		logger.Debug("insertRowBinary started")

		err := u.insertRowBinary(
			u.query,
			pipeReader,
			filename,
		)

		logger.With(zap.Error(err)).Debug("insertRowBinary finished")

		uploadResult <- err
		if err != nil {
			_ = pipeReader.CloseWithError(err)
		}
	})

	n, err = u.parseAndFilter(filename, out)
	if err == nil {
		err = out.Flush()
	}
	_ = pipeWriter.CloseWithError(err)

	select {
	case uploadErr = <-uploadResult:
		// pass
	case <-ctx.Done():
		return n, fmt.Errorf("upload aborted")
	}

	if err != nil {
		return n, err
	} else if uploadErr != nil {
		return n, uploadErr
	} else {
		return n, nil
	}
}

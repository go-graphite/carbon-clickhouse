package uploader

import (
	"bufio"
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
	u.blacklist = NewBlacklist(u.config.IgnoredPatterns)
	u.reverse = reverse
	u.query = fmt.Sprintf("%s (Path, Value, Time, Date, Timestamp)", u.config.TableName)
	return u
}

// parseAndFilter reads points data and excludes those ones which match blacklist
func (u *Points) parseAndFilter(filename string, out io.Writer) error {
	reader, err := RowBinary.NewReader(filename, u.reverse)
	if err != nil {
		return err
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

		wb.Reset()
		if !u.blacklist.Contains(unsafeString(name), u.reverse) {
			wb.WriteGraphitePoint(name, reader.Value(), reader.Timestamp(), reader.Version())

			_, err = out.Write(wb.Bytes())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (u *Points) upload(ctx context.Context, logger *zap.Logger, filename string) error {
	var (
		err, uploadErr error
		uploadResult   chan error
	)

	pipeReader, pipeWriter := io.Pipe()
	out := bufio.NewWriter(pipeWriter)
	uploadResult = make(chan error, 1)

	u.Go(func(ctx context.Context) {
		err := u.insertRowBinary(
			u.query,
			pipeReader,
		)
		uploadResult <- err
		if err != nil {
			_ = pipeReader.CloseWithError(err)
		}
	})

	err = u.parseAndFilter(filename, out)
	if err == nil {
		err = out.Flush()
	}
	_ = pipeWriter.CloseWithError(err)

	select {
	case uploadErr = <-uploadResult:
		// pass
	case <-ctx.Done():
		return fmt.Errorf("upload aborted")
	}

	if err != nil {
		return err
	} else if uploadErr != nil {
		return uploadErr
	} else {
		return nil
	}
}

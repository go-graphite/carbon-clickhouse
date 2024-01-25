package uploader

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type DebugCacheDumper interface {
	CacheDump(io.Writer)
}

type cached struct {
	*Base
	existsCache CMap // store known keys and don't load it to clickhouse tree
	parser      func(filename string, out io.Writer) (uint64, map[string]bool, error)
	expired     uint32 // atomic counter
}

func newCached(base *Base) *cached {
	u := &cached{Base: base}
	u.Base.handler = u.upload
	u.existsCache = NewCMap()
	u.query = fmt.Sprintf("%s (Date, Level, Path, Version)", u.config.TableName)
	return u
}

func (u *cached) Stat(send func(metric string, value float64)) {
	u.Base.Stat(send)

	send("cacheSize", float64(u.existsCache.Count()))

	expired := atomic.SwapUint32(&u.expired, 0)
	send("expired", float64(expired))
}

func (u *cached) Start() error {
	err := u.Base.Start()
	if err != nil {
		return err
	}

	if u.config.CacheTTL.Value() != 0 {
		u.Go(func(ctx context.Context) {
			u.existsCache.ExpireWorker(ctx, u.config.CacheTTL.Value(), &u.expired)
		})
	}

	return nil
}

func (u *cached) Reset() {
	u.existsCache.Clear()
	debug.FreeOSMemory()
}

func (u *cached) upload(ctx context.Context, logger *zap.Logger, filename string) (uint64, error) {
	var n uint64
	var err error
	var newSeries map[string]bool

	pipeReader, pipeWriter := io.Pipe()
	writer := bufio.NewWriter(pipeWriter)
	startTime := time.Now()

	uploadResult := make(chan error, 1)

	u.Go(func(ctx context.Context) {
		err = u.insertRowBinary(
			u.query,
			pipeReader,
			filename,
		)
		uploadResult <- err
		if err != nil {
			pipeReader.CloseWithError(err)
		}
	})

	n, newSeries, err = u.parser(filename, writer)
	if err == nil {
		err = writer.Flush()
	}
	pipeWriter.CloseWithError(err)

	var uploadErr error

	select {
	case uploadErr = <-uploadResult:
		// pass
	case <-ctx.Done():
		return n, fmt.Errorf("upload aborted")
	}

	if err != nil {
		return n, err
	}

	if uploadErr != nil {
		return n, uploadErr
	}

	// commit new series
	u.existsCache.Merge(newSeries, startTime.Unix())

	return n, nil
}

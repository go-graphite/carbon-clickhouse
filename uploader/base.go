package uploader

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/carbon-clickhouse/helper/stop"
)

type Base struct {
	stop.Struct
	sync.Mutex
	name    string
	path    string
	config  *Config
	queue   chan string
	inQueue map[string]bool
	logger  *zap.Logger
	handler func(ctx context.Context, logger *zap.Logger, filename string) (uint64, error) // upload single file
	query   string

	stat struct {
		uploaded        uint32
		uploadedMetrics uint64
		uploadTime      uint64
		errors          uint32
		delay           int64
		unhandled       uint32 // @TODO: maxUnhandled
	}
}

func (u *Base) Stat(send func(metric string, value float64)) {
	uploaded := atomic.SwapUint32(&u.stat.uploaded, 0)
	send("uploaded", float64(uploaded))

	uploadedMetrics := atomic.SwapUint64(&u.stat.uploadedMetrics, 0)
	send("uploaded_metrics", float64(uploadedMetrics))

	uploadTime := atomic.SwapUint64(&u.stat.uploadTime, 0)
	send("upload_time", float64(uploadTime))

	errors := atomic.SwapUint32(&u.stat.errors, 0)
	send("errors", float64(errors))

	delay := atomic.LoadInt64(&u.stat.delay)
	send("delay", float64(delay))

	send("unhandled", float64(atomic.LoadUint32(&u.stat.unhandled)))
}

func (u *Base) scanDir(ctx context.Context) {
	var delay int64
	now := time.Now().Unix()
	flist, err := ioutil.ReadDir(u.path)
	if err != nil {
		u.logger.Error("ReadDir failed", zap.Error(err))
		return
	}

	files := make([]string, 0)
	for _, f := range flist {
		if f.IsDir() {
			continue
		}
		if !strings.HasPrefix(f.Name(), "default.") {
			continue
		}

		fileName := filepath.Join(u.path, f.Name())
		stat, err := os.Lstat(fileName)
		if err != nil {
			u.logger.Error("Lstat failed", zap.Error(err))
			return
		}
		d := now - stat.ModTime().Unix()
		if delay < d {
			delay = d
		}
		files = append(files, fileName)
	}

	if delay >= 0 {
		atomic.StoreInt64(&u.stat.delay, delay)
	}
	n := uint32(len(files))
	atomic.StoreUint32(&u.stat.unhandled, n)

	if len(files) == 0 {
		return
	}

	// TODO (msaf1980): maybe load newest files first ?
	sort.Strings(files)

	for _, fn := range files {
		u.Lock()
		if u.inQueue[fn] {
			u.Unlock()
			continue
		} else {
			u.inQueue[fn] = true
		}
		u.Unlock()

		select {
		case u.queue <- fn:
			n--
			atomic.StoreUint32(&u.stat.unhandled, n)
			// pass
		case <-ctx.Done():
			return
		}
	}
}

func (u *Base) watchWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			u.scanDir(ctx)
		}
	}
}

func (u *Base) MarkAsFinished(filename string) {
	dn, fn := filepath.Split(filename)
	err := os.Rename(filename, filepath.Join(dn, "_"+fn))
	if err != nil {
		u.logger.Error("rename file failed",
			zap.String("filename", filename),
			zap.Error(err),
		)
	}
}

func (u *Base) RemoveFromQueue(filename string) {
	u.Lock()
	delete(u.inQueue, filename)
	u.Unlock()
}

func (u *Base) Start() error {
	return u.StartFunc(func() error {
		u.Go(u.watchWorker)

		for i := 0; i < u.config.Threads; i++ {
			u.Go(func(ctx context.Context) {
				u.uploadWorker(ctx)
			})
		}

		return nil
	})
}

func (u *Base) uploadWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case filename := <-u.queue:
			startTime := time.Now()
			logger := u.logger.With(zap.String("filename", filename))
			logger.Info("start handle")

			n, err := u.handler(ctx, logger, filename)

			duration := time.Since(startTime)
			atomic.AddUint64(&u.stat.uploadTime, uint64(duration.Milliseconds()))

			if err != nil {
				atomic.AddUint32(&u.stat.errors, 1)
				logger.Error("handle failed",
					zap.Uint64("metrics", n),
					zap.Error(err),
					zap.Duration("time", duration),
				)

				time.Sleep(time.Second)
			} else {
				atomic.AddUint32(&u.stat.uploaded, 1)
				atomic.AddUint64(&u.stat.uploadedMetrics, n)
				logger.Info("handle success",
					zap.Uint64("metrics", n),
					zap.Duration("time", duration),
				)
			}

			if err == nil {
				u.MarkAsFinished(filename)
			}
			u.RemoveFromQueue(filename)
		}
	}
}

func compress(data *io.PipeReader) io.Reader {
	pr, pw := io.Pipe()
	gw := gzip.NewWriter(pw)

	go func() {
		_, err := io.Copy(gw, data)
		if err != nil {
			pw.CloseWithError(err)
			data.CloseWithError(err)
			return
		}

		err = gw.Close()
		if err != nil {
			pw.CloseWithError(err)
			data.CloseWithError(err)
			return
		}

		pw.Close()
		data.Close()
	}()

	return pr
}

func (u *Base) insertRowBinary(table string, data *io.PipeReader) error {
	p, err := url.Parse(u.config.URL)
	if err != nil {
		return err
	}

	q := p.Query()

	q.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", table))
	p.RawQuery = q.Encode()
	queryURL := p.String()

	var req *http.Request

	if u.config.CompressData {
		req, err = http.NewRequest("POST", queryURL, compress(data))
		req.Header.Add("Content-Encoding", "gzip")
	} else {
		req, err = http.NewRequest("POST", queryURL, data)
	}

	if err != nil {
		return err
	}
	resp, err := u.config.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	if exceptionCode := resp.Header.Get("X-Clickhouse-Exception-Code"); exceptionCode != "" && exceptionCode != "0" {
		return fmt.Errorf("clickhouse exception code %s, response status %d: %s", exceptionCode, resp.StatusCode, string(body))
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("clickhouse response status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

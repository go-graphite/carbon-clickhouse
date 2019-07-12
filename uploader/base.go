package uploader

import (
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

	"github.com/lomik/carbon-clickhouse/helper/stop"
	"go.uber.org/zap"
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
	handler func(ctx context.Context, logger *zap.Logger, filename string) error // upload single file
	query   string

	stat struct {
		uploaded  uint32
		errors    uint32
		unhandled uint32 // @TODO: maxUnhandled
	}
}

func (u *Base) Stat(send func(metric string, value float64)) {
	uploaded := atomic.LoadUint32(&u.stat.uploaded)
	atomic.AddUint32(&u.stat.uploaded, -uploaded)
	send("uploaded", float64(uploaded))

	errors := atomic.LoadUint32(&u.stat.errors)
	atomic.AddUint32(&u.stat.errors, -errors)
	send("errors", float64(errors))

	send("unhandled", float64(atomic.LoadUint32(&u.stat.unhandled)))
}

func (u *Base) scanDir(ctx context.Context) {
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

		files = append(files, filepath.Join(u.path, f.Name()))
	}

	atomic.StoreUint32(&u.stat.unhandled, uint32(len(files)))

	if len(files) == 0 {
		return
	}

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

			err := u.handler(ctx, logger, filename)

			if err != nil {
				atomic.AddUint32(&u.stat.errors, 1)
				logger.Error("handle failed",
					zap.Error(err),
					zap.Duration("time", time.Since(startTime)),
				)

				time.Sleep(time.Second)
			} else {
				atomic.AddUint32(&u.stat.uploaded, 1)
				logger.Info("handle success",
					zap.Duration("time", time.Since(startTime)),
				)
			}

			if err == nil {
				u.MarkAsFinished(filename)
			}
			u.RemoveFromQueue(filename)
		}
	}
}

func (u *Base) insertRowBinary(table string, data io.Reader) error {
	p, err := url.Parse(u.config.URL)
	if err != nil {
		return err
	}

	q := p.Query()

	q.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", table))
	p.RawQuery = q.Encode()
	queryUrl := p.String()

	req, err := http.NewRequest("POST", queryUrl, data)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: u.config.Timeout.Value()}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		return fmt.Errorf("clickhouse response status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

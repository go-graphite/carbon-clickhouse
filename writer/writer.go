package writer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/config"
	"github.com/lomik/carbon-clickhouse/helper/stop"
	"github.com/lomik/zapwriter"
	"github.com/pierrec/lz4"
	"go.uber.org/zap"
)

type compWriter interface {
	Write([]byte) (int, error)
	Flush() error
	Close() error
}

// Writer dumps all received data in prepared for clickhouse format
type Writer struct {
	stop.Struct
	sync.RWMutex
	stat struct {
		writtenBytes  uint32
		unhandled     uint32
		chunkInterval uint32
	}
	inputChan    chan *RowBinary.WriteBuffer
	path         string
	maxSize      int64
	autoInterval *config.ChunkAutoInterval
	compAlgo     config.CompAlgo
	compLevel    int
	lz4Header    lz4.Header
	inProgress   map[string]bool // current writing files
	logger       *zap.Logger
	uploaders    []string
	onFinish     func(string) error
}

func New(in chan *RowBinary.WriteBuffer, path string, switchSize int64, autoInterval *config.ChunkAutoInterval, compAlgo config.CompAlgo, compLevel int, uploaders []string, onFinish func(string) error) *Writer {
	finishCallback := func(fn string) error {
		if err := Link(fn, uploaders); err != nil {
			return err
		}

		if onFinish != nil {
			return onFinish(fn)
		}

		return nil
	}

	wr := &Writer{
		inputChan:    in,
		path:         path,
		maxSize:      switchSize,
		autoInterval: autoInterval,
		compAlgo:     compAlgo,
		compLevel:    compLevel,
		inProgress:   make(map[string]bool),
		logger:       zapwriter.Logger("writer"),
		uploaders:    uploaders,
		onFinish:     finishCallback,
	}

	switch compAlgo {
	case config.CompAlgoLZ4:
		wr.lz4Header = lz4.Header{
			BlockMaxSize:     4 << 20,
			CompressionLevel: compLevel,
		}
	}

	return wr
}

func (w *Writer) Start() error {
	return w.StartFunc(func() error {
		// link pre-existing files
		if err := w.LinkAll(); err != nil {
			return err
		}
		if err := w.Cleanup(); err != nil {
			return err
		}
		w.Go(w.worker)
		w.Go(w.cleaner)
		return nil
	})
}

func (w *Writer) Stat(send func(metric string, value float64)) {
	writtenBytes := atomic.LoadUint32(&w.stat.writtenBytes)
	atomic.AddUint32(&w.stat.writtenBytes, -writtenBytes)
	send("writtenBytes", float64(writtenBytes))

	send("unhandled", float64(atomic.LoadUint32(&w.stat.unhandled)))
	send("chunkInterval_s", float64(atomic.LoadUint32(&w.stat.chunkInterval)))
}

func (w *Writer) IsInProgress(filename string) bool {
	w.RLock()
	v := w.inProgress[filename]
	w.RUnlock()
	return v
}

func (w *Writer) worker(ctx context.Context) {
	var out *os.File
	var cwr compWriter
	var outBuf *bufio.Writer
	var fn string // current filename
	var size int64
	var start time.Time
	var chunkInterval time.Duration

	cwrClose := func() {
		if cwr != nil {
			if err := cwr.Close(); err != nil {
				w.logger.Error("CompWriter close failed", zap.Error(err))
			}
		}
	}

	defer func() {
		if out != nil {
			outBuf.Flush()
			cwrClose()
			out.Close()

			w.logger.Info("chunk switched", zap.String("filename", fn), zap.Int64("size", size))
		}
	}()

	// close old file, open new
	rotateCheck := func() {
		if out != nil {
			if w.maxSize == 0 || size < w.maxSize {
				now := time.Now()
				prevInterval := w.autoInterval.GetDefault()
				u := int(atomic.LoadUint32(&w.stat.unhandled))
				interval := w.autoInterval.GetInterval(u)
				if interval != prevInterval {
					w.logger.Info("chunk interval changed", zap.String("interval", interval.String()))
					prevInterval = interval
					atomic.StoreUint32(&w.stat.chunkInterval, uint32(interval.Seconds()))
				}

				chunkInterval = now.Sub(start)
				if chunkInterval < interval {
					return
				}
			} else {
				chunkInterval = time.Since(start)
			}

			outBuf.Flush()
			cwrClose()
			out.Close()

			w.logger.Info("chunk switched", zap.String("filename", fn), zap.Int64("size", size), zap.Float64("time", float64(chunkInterval.Nanoseconds())/1000000000.0))

			out = nil
			cwr = nil
			outBuf = nil
			size = 0
		}

		var err error

	OpenLoop:
		for {
			go func(filename string) {
				if filename == "" || w.onFinish == nil {
					return
				}

				err = w.onFinish(filename)
				if err != nil {
					w.logger.Error("onFinish callback failed", zap.String("filename", filename), zap.Error(err))
				}
			}(fn)

			// replace fn in inProgress
			w.Lock()
			delete(w.inProgress, fn)

			var fileExtension string
			switch w.compAlgo {
			case config.CompAlgoLZ4:
				fileExtension = lz4.Extension
			}

			fn = path.Join(w.path, fmt.Sprintf("default.%d%s", time.Now().UnixNano(), fileExtension))
			w.inProgress[fn] = true
			w.Unlock()

			out, err = os.OpenFile(fn, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

			start = time.Now()

			if err != nil {
				w.logger.Error("create failed", zap.String("filename", fn), zap.Error(err))

				// check exit channel
				select {
				case <-ctx.Done():
					break OpenLoop
				default:
				}

				// try and spam to error log every second
				time.Sleep(time.Second)

				continue OpenLoop
			}

			var wr io.Writer
			switch w.compAlgo {
			case config.CompAlgoNone:
				wr = out
			case config.CompAlgoLZ4:
				lz4w := lz4.NewWriter(out)
				lz4w.Header = w.lz4Header
				cwr = lz4w
				wr = lz4w
			}

			outBuf = bufio.NewWriterSize(wr, 1024*1024)
			break OpenLoop
		}
	}

	// open first file
	rotateCheck()

	tickerC := make(chan struct{}, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				select {
				case tickerC <- struct{}{}:
					// pass
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	write := func(b *RowBinary.WriteBuffer) {
		_, err := outBuf.Write(b.Body[:b.Used])
		if b.ConfirmRequired() {
			if err != nil {
				b.Fail(err)
			} else {
				err := outBuf.Flush()
				if err != nil {
					b.Fail(err)
				} else {
					b.Confirm()
				}
			}
		}
		// @TODO: log error?
		size += int64(b.Used)
		atomic.AddUint32(&w.stat.writtenBytes, uint32(b.Used))
		b.Release()
	}

	for {
		select {
		case b := <-w.inputChan:
			write(b)
		case <-tickerC:
			rotateCheck()
		case <-ctx.Done():
			return
		default: // outBuf flush if nothing received
			outBuf.Flush()

			if cwr != nil {
				if err := cwr.Flush(); err != nil {
					w.logger.Error("CompWriter Flush() failed", zap.Error(err))
				}
			}

			select {
			case b := <-w.inputChan:
				write(b)
			case <-tickerC:
				rotateCheck()
			case <-ctx.Done():
				return
			}
		}
	}
}

func (w *Writer) cleaner(ctx context.Context) {
	ticker := time.NewTicker(w.autoInterval.GetDefault())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.Cleanup()
		}
	}
}

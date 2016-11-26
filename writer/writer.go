package writer

import (
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/lomik/carbon-clickhouse/receiver"
	"github.com/lomik/go-carbon/helper"
)

// Writer dumps all received data in prepared for clickhouse format
type Writer struct {
	helper.Stoppable
	sync.RWMutex
	inputChan    chan *receiver.WriteBuffer
	path         string
	fileInterval time.Duration
	writtenBytes uint64          // stat "bytes"
	openedFiles  uint64          // stat "files"
	inProgress   map[string]bool // current writing files
}

func New(in chan *receiver.WriteBuffer, path string, fileInterval time.Duration) *Writer {
	return &Writer{
		inputChan:    in,
		path:         path,
		fileInterval: fileInterval,
		inProgress:   make(map[string]bool),
	}
}

func (w *Writer) Start() error {
	return w.StartFunc(func() error {
		w.Go(w.worker)
		return nil
	})
}

func (w *Writer) IsInProgress(filename string) bool {
	w.RLock()
	v := w.inProgress[filename]
	w.RUnlock()
	return v
}

func (w *Writer) worker(exit chan bool) {
	var out *os.File
	var fn string // current filename

	defer func() {
		if out != nil {
			out.Close()
		}
	}()

	// close old file, open new
	rotate := func() {
		if out != nil {
			out.Close()
			out = nil
		}

		var err error

	OpenLoop:
		for {
			// replace fn in inProgress
			w.Lock()
			delete(w.inProgress, fn)
			fn = path.Join(w.path, fmt.Sprintf("default.%d", time.Now().UnixNano()))
			w.inProgress[fn] = true
			w.Unlock()

			out, err = os.OpenFile(fn, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

			if err != nil {
				logrus.Errorf("open %s: %s", fn, err.Error())

				// check exit channel
				select {
				case <-exit:
					break OpenLoop
				default:
				}

				// try and spam to error log every second
				time.Sleep(time.Second)

				continue OpenLoop
			}
			break OpenLoop
		}
	}

	// open first file
	rotate()

	ticker := time.NewTicker(w.fileInterval)
	for {
		select {
		case b := <-w.inputChan:
			out.Write(b.Body[:b.Used])
			receiver.WriteBufferPool.Put(b)
		case <-ticker.C:
			rotate()
		case <-exit:
			return
		}
	}
}

package writer

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"go.uber.org/zap"
)

func Cleanup(filename string, tables []string) (bool, error) {
	if len(tables) == 0 {
		return false, fmt.Errorf("upload destination list is empty")
	}

	d, fn := filepath.Split(filename)

	var err error

	for _, t := range tables {
		if _, err := os.Stat(filepath.Join(d, t, "_"+fn)); os.IsNotExist(err) {
			// file not finished
			return false, nil
		}
	}

	err = os.Remove(filename)
	if err != nil {
		return false, err
	}

	for _, t := range tables {
		err = os.Remove(filepath.Join(d, t, "_"+fn))
		if err != nil {
			return true, err
		}
	}

	return true, nil
}

func (w *Writer) Cleanup() error {
	// check and create table directories
	for _, t := range w.uploaders {
		if _, err := os.Stat(filepath.Join(w.path, t)); os.IsNotExist(err) {
			err = os.Mkdir(filepath.Join(w.path, t), 0755)
			if err != nil {
				return err
			}
		}
	}

	flist, err := ioutil.ReadDir(w.path)
	if err != nil {
		w.logger.Error("ReadDir failed", zap.Error(err))
		return err
	}

	unhandledList := make([]string, 0, len(flist))
	for _, f := range flist {
		if f.IsDir() {
			continue
		}

		if !strings.HasPrefix(f.Name(), "default.") {
			continue
		}

		unhandledList = append(unhandledList, f.Name())
	}

	unhandledCount := len(unhandledList)
	// remove finished files
	for _, fn := range unhandledList {
		removed, err := Cleanup(filepath.Join(w.path, fn), w.uploaders)
		if removed {
			unhandledCount--
		}
		if err != nil {
			atomic.StoreUint32(&w.stat.unhandled, uint32(unhandledCount))
			return err
		}
	}
	atomic.StoreUint32(&w.stat.unhandled, uint32(unhandledCount))

	// remove broken links
	for _, t := range w.uploaders {
		flist, err := ioutil.ReadDir(filepath.Join(w.path, t))
		if err != nil {
			w.logger.Error("ReadDir failed", zap.Error(err))
			return err
		}

		for _, f := range flist {
			full := filepath.Join(w.path, t, f.Name())
			_, err := filepath.EvalSymlinks(full)
			if err != nil {
				w.logger.Info("remove broken link", zap.String("filename", full))
				if err := os.Remove(full); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

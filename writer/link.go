package writer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
)

// Write symlink to file to all table subfolders
func Link(filename string, tables []string) error {
	d, fn := filepath.Split(filename)

	var err error

	for _, t := range tables {
		if _, err = os.Stat(filepath.Join(d, t)); os.IsNotExist(err) {
			err = os.Mkdir(filepath.Join(d, t), 0755)
			if err != nil {
				return err
			}
		}

		if _, err := os.Stat(filepath.Join(d, t, fn)); !os.IsNotExist(err) {
			// symlink or file already exists
			continue
		}

		if _, err := os.Stat(filepath.Join(d, t, "_"+fn)); !os.IsNotExist(err) {
			// finished symlink or file already exists
			continue
		}

		err = os.Symlink(filepath.Join("..", fn), filepath.Join(d, t, fn))
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) LinkAll() error {
	flist, err := ioutil.ReadDir(w.path)
	if err != nil {
		w.logger.Error("ReadDir failed", zap.Error(err))
		return err
	}

	for _, f := range flist {
		if f.IsDir() {
			continue
		}
		if !strings.HasPrefix(f.Name(), "default.") {
			continue
		}

		if err := Link(filepath.Join(w.path, f.Name()), w.uploaders); err != nil {
			return err
		}
	}

	return nil
}

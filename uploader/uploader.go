package uploader

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lomik/stop"
	"github.com/uber-go/zap"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
)

type Option func(u *Uploader)

func Path(path string) Option {
	return func(u *Uploader) {
		u.path = path
	}
}

func ClickHouse(dsn string) Option {
	return func(u *Uploader) {
		u.clickHouseDSN = dsn
	}
}

func DataTables(t []string) Option {
	return func(u *Uploader) {
		u.dataTables = t
	}
}

func DataTimeout(t time.Duration) Option {
	return func(u *Uploader) {
		u.dataTimeout = t
	}
}

func TreeTable(t string) Option {
	return func(u *Uploader) {
		u.treeTable = t
	}
}

func TreeDate(t time.Time) Option {
	return func(u *Uploader) {
		u.treeDate = t
	}
}

func TreeTimeout(t time.Duration) Option {
	return func(u *Uploader) {
		u.treeTimeout = t
	}
}

func InProgressCallback(cb func(string) bool) Option {
	return func(u *Uploader) {
		u.inProgressCallback = cb
	}
}

func Threads(t int) Option {
	return func(u *Uploader) {
		u.threads = t
	}
}

func Logger(logger zap.Logger) Option {
	return func(u *Uploader) {
		u.logger = logger
	}
}

// Uploader upload files from local directory to clickhouse
type Uploader struct {
	stop.Struct
	sync.Mutex
	stat struct {
		uploaded  uint32
		errors    uint32
		unhandled uint32 // @TODO: maxUnhandled
	}
	path               string
	clickHouseDSN      string
	dataTables         []string
	dataTimeout        time.Duration
	treeTable          string
	treeTimeout        time.Duration
	treeDate           time.Time
	threads            int
	inProgressCallback func(string) bool
	queue              chan string
	inQueue            map[string]bool // current uploading files
	treeExists         CMap            // store known keys and don't load it to clickhouse tree
	logger             zap.Logger
}

func New(options ...Option) *Uploader {

	u := &Uploader{
		path:               "/data/carbon-clickhouse/",
		dataTables:         []string{},
		treeTable:          "",
		dataTimeout:        time.Minute,
		treeTimeout:        time.Minute,
		treeDate:           time.Date(2016, 11, 1, 0, 0, 0, 0, time.Local),
		inProgressCallback: func(string) bool { return false },
		queue:              make(chan string, 1024),
		inQueue:            make(map[string]bool),
		threads:            1,
		treeExists:         NewCMap(),
		logger:             zap.New(zap.NullEncoder()),
	}

	for _, o := range options {
		o(u)
	}

	return u
}

func (u *Uploader) Start() error {
	return u.StartFunc(func() error {
		u.Go(u.watchWorker)

		for i := 0; i < u.threads; i++ {
			u.Go(u.uploadWorker)
		}

		return nil
	})
}

func (u *Uploader) Stat(send func(metric string, value float64)) {
	uploaded := atomic.LoadUint32(&u.stat.uploaded)
	atomic.AddUint32(&u.stat.uploaded, -uploaded)
	send("uploaded", float64(uploaded))

	errors := atomic.LoadUint32(&u.stat.errors)
	atomic.AddUint32(&u.stat.errors, -errors)
	send("errors", float64(errors))

	send("unhandled", float64(atomic.LoadUint32(&u.stat.unhandled)))

	send("treeExistsCacheSize", float64(u.treeExists.Count()))
}

func (u *Uploader) ClearTreeExistsCache() {
	u.treeExists.Clear()
}

func uploadData(chUrl string, table string, timeout time.Duration, data io.Reader) error {
	p, err := url.Parse(chUrl)
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

	client := &http.Client{Timeout: timeout}
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

func (u *Uploader) uploadDataTable(filename string, tablename string) error {
	logger := u.logger.With(zap.String("filename", filename))

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return err
	}

	if fi.Size() == 0 {
		logger.Info("file is empty")
		return nil
	}
	err = uploadData(
		u.clickHouseDSN,
		fmt.Sprintf("%s (Path, Value, Time, Date, Timestamp)", tablename),
		u.dataTimeout,
		file,
	)

	if err != nil {
		if strings.Index(err.Error(), "Code: 33, e.displayText() = DB::Exception: Cannot read all data") >= 0 {
			logger.Warn("file corrupted, try to recover")

			var reader *RowBinary.Reader
			reader, err = RowBinary.NewReader(filename)
			if err != nil {
				return err
			}

			// try slow read method with skip bad records
			err = uploadData(
				u.clickHouseDSN,
				fmt.Sprintf("%s (Path, Value, Time, Date, Timestamp)", tablename),
				u.dataTimeout,
				reader,
			)
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (u *Uploader) upload(exit chan struct{}, filename string) (err error) {
	startTime := time.Now()

	logger := u.logger.With(zap.String("filename", filename))
	logger.Info("start handle")

	defer func() {
		if err != nil {
			atomic.AddUint32(&u.stat.errors, 1)
			logger.Error("upload failed",
				zap.Error(err),
				zap.String("time", time.Now().Sub(startTime).String()),
			)
		} else {
			atomic.AddUint32(&u.stat.uploaded, 1)
			logger.Info("upload success",
				zap.String("time", time.Now().Sub(startTime).String()),
			)
		}
	}()

	for _, tablename := range u.dataTables {
		err = u.uploadDataTable(filename, tablename)
		if err != nil {
			return err
		}
	}

	if u.treeTable == "" { // don't make index in clickhouse
		return nil
	}

	// MAKE INDEX
	tree, err := u.MakeTree(filename)
	if err != nil {
		return err
	}

	if tree.data.Len() > 0 {
		err = uploadData(
			u.clickHouseDSN,
			fmt.Sprintf("%s (Date, Level, Path, Version)", u.treeTable),
			u.treeTimeout,
			tree.data,
		)
		if err != nil {
			return err
		}
	}

	tree.Success()

	return nil
}

func (u *Uploader) uploadWorker(exit chan struct{}) {
	for {
		select {
		case <-exit:
			return
		case filename := <-u.queue:
			err := u.upload(exit, filename)
			if err == nil {
				err := os.Remove(filename)
				if err != nil {
					u.logger.Error("file delete failed",
						zap.String("filename", filename),
						zap.Error(err),
					)
				} else {
					u.logger.Info("file deleted",
						zap.String("filename", filename),
					)
				}
			}
			u.Lock()
			delete(u.inQueue, filename)
			u.Unlock()
		}
	}
}

func (u *Uploader) watch(exit chan struct{}) {
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

		files = append(files, path.Join(u.path, f.Name()))
	}

	atomic.StoreUint32(&u.stat.unhandled, uint32(len(files)))

	if len(files) == 0 {
		return
	}

	sort.Strings(files)

	for _, fn := range files {
		if u.inProgressCallback(fn) { // write in progress
			continue
		}

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
		case <-exit:
			return
		}
	}
}

func (u *Uploader) watchWorker(exit chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-exit:
			return
		case <-t.C:
			u.watch(exit)
		}
	}
}

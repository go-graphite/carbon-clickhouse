package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"go.uber.org/zap"

	"github.com/lomik/carbon-clickhouse/helper/tests"
)

type InputType int

const (
	InputPlainTCP InputType = iota
)

var (
	inputStrings []string = []string{"tcp_plain"}

	preSQL = []string{
		"TRUNCATE TABLE IF EXISTS graphite_reverse",
		"TRUNCATE TABLE IF EXISTS graphite",
		"TRUNCATE TABLE IF EXISTS graphite_index",
		"TRUNCATE TABLE IF EXISTS graphite_tags",
	}
)

func (a *InputType) String() string {
	return inputStrings[*a]
}

func (a *InputType) Set(value string) error {
	switch value {
	case "plain_tcp":
		*a = InputPlainTCP
	default:
		return fmt.Errorf("invalid input type %s", value)
	}
	return nil
}

func (a *InputType) UnmarshalText(text []byte) error {
	return a.Set(string(text))
}

type Verify struct {
	Query  string   `yaml:"query"`
	Output []string `yaml:"output"`
}

type TestSchema struct {
	InputTypes []InputType `toml:"input_types"` // carbon-clickhouse input types

	Input      []string     `toml:"input"`           // carbon-clickhouse input
	ConfigTpl  string       `toml:"config_template"` // carbon-clickhouse config template
	Clickhouse []Clickhouse `yaml:"clickhouse"`

	Verify []Verify `yaml:"verify"`

	dir        string          `yaml:"-"`
	name       string          `yaml:"-"` // test alias (from config name)
	chVersions map[string]bool `toml:"-"`
}

func (schema *TestSchema) HasTLSSettings() bool {
	return strings.Contains(schema.dir, "tls")
}

func getFreeTCPPort(name string) (string, error) {
	if len(name) == 0 {
		name = "127.0.0.1:0"
	} else if !strings.Contains(name, ":") {
		name = name + ":0"
	}
	addr, err := net.ResolveTCPAddr("tcp", name)
	if err != nil {
		return name, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return name, err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

func sendPlain(network, address string, input []string) error {
	if conn, err := net.DialTimeout(network, address, time.Second); err != nil {
		return err
	} else {
		for _, m := range input {
			conn.SetDeadline(time.Now().Add(time.Second))
			if _, err = conn.Write([]byte(m + "\n")); err != nil {
				return err
			}
		}
		return conn.Close()
	}
}

func verifyOut(ch *Clickhouse, verify Verify) []string {
	chURL := ch.URL()
	var errs []string

	q := []byte(verify.Query)
	req, err := http.NewRequest("POST", chURL, bytes.NewBuffer(q))
	if err != nil {
		return []string{err.Error()}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return []string{err.Error()}
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []string{err.Error()}
	}
	s := strings.TrimRight(string(body), "\n")
	if resp.StatusCode != 200 {
		return []string{"response status is '" + resp.Status + "', " + s}
	}

	s = strings.ReplaceAll(s, "\t", " ")
	ss := strings.Split(s, "\n")
	if len(ss) == 1 && len(ss[0]) == 0 {
		ss = []string{} /* results is empthy */
	}

	maxLen := tests.Max(len(ss), len(verify.Output))
	for i := 0; i < maxLen; i++ {
		if i >= len(ss) {
			errs = append(errs, fmt.Sprintf("- [%d]: %s", i, verify.Output[i]))
		} else if i >= len(verify.Output) {
			errs = append(errs, fmt.Sprintf("+ [%d]: %s", i, ss[i]))
		} else if ss[i] != verify.Output[i] {
			errs = append(errs, fmt.Sprintf("- [%d]: %s", i, verify.Output[i]))
			errs = append(errs, fmt.Sprintf("+ [%d]: %s", i, ss[i]))
		}
	}
	return errs
}

func testCarbonClickhouse(
	inputType InputType, test *TestSchema, clickhouse *Clickhouse,
	testDir, rootDir string,
	verbose, breakOnError bool, logger *zap.Logger) (testSuccess bool) {

	testSuccess = true

	err := clickhouse.CheckConfig(rootDir)
	if err != nil {
		return false
	}
	absoluteDir, err := filepath.Abs(test.dir)
	if err != nil {
		return false
	}
	cch := CarbonClickhouse{
		ConfigTpl: testDir + "/" + test.ConfigTpl,
		TestDir:   absoluteDir,
	}
	var (
		tlsurl string
	)
	if test.HasTLSSettings() {
		tlsurl = clickhouse.TLSURL()
		if tlsurl == "" {
			logger.Error("test has tls settings but there is no clickhouse tls url")
			return false
		}
	}
	err = cch.Start(clickhouse.URL(), tlsurl)
	if err != nil {
		logger.Error("starting carbon-clickhouse",
			zap.String("config", test.name),
			zap.String("input", inputType.String()),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
		)
		testSuccess = false
	}

	if testSuccess {
		logger.Info("starting e2e test",
			zap.String("config", test.name),
			zap.String("input", inputType.String()),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
		)
		time.Sleep(2 * time.Second)
		// Run test

		if len(test.Input) > 0 {
			switch inputType {
			case InputPlainTCP:
				err = sendPlain("tcp", cch.address, test.Input)
			default:
				err = fmt.Errorf("input type not implemented")
			}
			if err != nil {
				logger.Error("send plain to carbon-clickhouse",
					zap.String("config", test.name),
					zap.String("input", inputType.String()),
					zap.String("clickhouse version", clickhouse.Version),
					zap.String("clickhouse config", clickhouse.Dir),
					zap.Error(err),
				)
				testSuccess = false
				if breakOnError {
					debug(test, clickhouse, &cch)
				}
			}
		}

		if testSuccess {
			verifyFailed := 0
			time.Sleep(10 * time.Second)
			for _, verify := range test.Verify {
				if errs := verifyOut(clickhouse, verify); len(errs) > 0 {
					testSuccess = false
					verifyFailed++
					for _, e := range errs {
						fmt.Fprintln(os.Stderr, e)
					}
					logger.Error("verify records in clickhouse",
						zap.String("config", test.name),
						zap.String("input", inputType.String()),
						zap.String("clickhouse version", clickhouse.Version),
						zap.String("clickhouse config", clickhouse.Dir),
						zap.String("verify", verify.Query),
					)
					if breakOnError {
						debug(test, clickhouse, &cch)
					}
				} else if verbose {
					logger.Info("verify records in clickhouse",
						zap.String("config", test.name),
						zap.String("input", inputType.String()),
						zap.String("clickhouse version", clickhouse.Version),
						zap.String("clickhouse config", clickhouse.Dir),
						zap.String("verify", verify.Query),
					)
				}
			}
			if verifyFailed > 0 {
				logger.Error("verify records in clickhouse",
					zap.String("config", test.name),
					zap.String("input", inputType.String()),
					zap.String("clickhouse version", clickhouse.Version),
					zap.String("clickhouse config", clickhouse.Dir),
					zap.Int("verify failed", verifyFailed),
					zap.Int("verify total", len(test.Verify)),
				)
			} else {
				logger.Info("verify records in clickhouse",
					zap.String("config", test.name),
					zap.String("input", inputType.String()),
					zap.String("clickhouse version", clickhouse.Version),
					zap.String("clickhouse config", clickhouse.Dir),
					zap.Int("verify success", len(test.Verify)),
					zap.Int("verify total", len(test.Verify)),
				)
			}
		}
	}

	err = cch.Stop()
	cch.Cleanup()
	if err != nil {
		logger.Error("stoping carbon-clickhouse",
			zap.String("config", test.name),
			zap.String("input", inputType.String()),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
		)
		testSuccess = false
	}

	if testSuccess {
		logger.Info("end e2e test",
			zap.String("config", test.name),
			zap.String("input", inputType.String()),
			zap.String("status", "success"),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
		)
	} else {
		logger.Error("end e2e test",
			zap.String("config", test.name),
			zap.String("input", inputType.String()),
			zap.String("status", "failed"),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
		)
	}

	return
}

func clickhouseStart(clickhouse *Clickhouse, logger *zap.Logger) bool {
	out, err := clickhouse.Start()
	if err != nil {
		logger.Error("starting clickhouse",
			zap.Any("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
			zap.String("out", out),
		)
		clickhouse.Stop(true)
		return false
	}
	return true
}

func clickhouseStop(clickhouse *Clickhouse, logger *zap.Logger) (result bool) {
	result = true
	if !clickhouse.Alive() {
		clickhouse.CopyLog(os.TempDir(), 10)
		result = false
	}

	out, err := clickhouse.Stop(true)
	if err != nil {
		logger.Error("stoping clickhouse",
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
			zap.String("out", out),
		)
		result = false
	}
	return result
}

func loadConfig(config string, rootDir string) (*MainConfig, error) {
	d, err := ioutil.ReadFile(config)
	if err != nil {
		return nil, err
	}

	var cfg = &MainConfig{}
	if _, err := toml.Decode(string(d), cfg); err != nil {
		return nil, err
	}

	cfg.Test.dir = path.Dir(config)
	cfg.Test.name = strings.ReplaceAll(config, rootDir+"/", "")
	if len(cfg.Test.InputTypes) == 0 {
		cfg.Test.InputTypes = []InputType{InputPlainTCP}
	}

	if len(cfg.Test.Input) == 0 {
		return nil, ErrNoInput
	}

	cfg.Test.chVersions = make(map[string]bool)
	for i := range cfg.Test.Clickhouse {
		if err := cfg.Test.Clickhouse[i].CheckConfig(rootDir); err == nil {
			cfg.Test.chVersions[cfg.Test.Clickhouse[i].Key()] = true
		} else {
			return nil, fmt.Errorf("[%d] %s", i, err.Error())
		}
	}
	return cfg, nil
}

func runTest(cfg *MainConfig, clickhouse *Clickhouse, rootDir string, verbose, breakOnError bool, logger *zap.Logger) (failed, total int) {
	for _, sql := range preSQL {
		if success, out := clickhouse.Exec(sql); !success {
			logger.Error("pre-execute",
				zap.String("config", cfg.Test.name),
				zap.Any("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouse.Dir),
				zap.String("sql", sql),
				zap.String("out", out),
			)
			return
		}
	}
	for _, inputType := range cfg.Test.InputTypes {
		total++
		if !testCarbonClickhouse(inputType, cfg.Test, clickhouse, cfg.Test.dir, rootDir, verbose, breakOnError, logger) {
			failed++
		}
	}

	return
}

func debug(test *TestSchema, ch *Clickhouse, cch *CarbonClickhouse) {
	for {
		fmt.Printf("carbon-clickhouse URL: %s , clickhouse URL: %s\n",
			cch.Address(), cch.Address())
		fmt.Println("Some queries was failed, press y for continue after debug test, k for kill carbon-clickhouse:")
		in := bufio.NewScanner(os.Stdin)
		in.Scan()
		s := in.Text()
		if s == "y" || s == "Y" {
			break
		} else if s == "k" || s == "K" {
			cch.Stop()
		}
	}
}

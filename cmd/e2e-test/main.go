package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"go.uber.org/zap"
)

type TestSchema struct {
	Input      []string     `toml:"input"`           // carbon-clickhouse input
	ConfigTpl  string       `toml:"config_template"` // carbon-clickhouse config template
	Clickhouse []Clickhouse `yaml:"clickhouse"`

	Verify []Verify `yaml:"verify"`
}

type MainConfig struct {
	Test *TestSchema `toml:"test"`
}

func IsDir(filename string) (bool, error) {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return info.IsDir(), nil
}

func expandDir(dirname string, paths *[]string) error {
	files, err := ioutil.ReadDir(dirname)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			if err = expandDir(path.Join(dirname, file.Name()), paths); err != nil {
				return err
			}
		} else {
			ext := path.Ext(file.Name())
			if ext == ".toml" {
				*paths = append(*paths, path.Join(dirname, file.Name()))
			}
		}
	}

	return nil
}

func expand(filename string, paths *[]string) error {
	if len(filename) == 0 {
		return nil
	}
	isDir, err := IsDir(filename)
	if err == nil {
		if isDir {
			if err = expandDir(filename, paths); err != nil {
				return err
			}
		} else {
			*paths = append(*paths, filename)
		}
	}
	return err
}

func main() {
	_, filename, _, _ := runtime.Caller(0)
	rootDir := path.Dir(path.Dir(path.Dir(filename))) // carbon-clickhouse repositiry root dir

	config := flag.String("config", "", "toml configuration file or dir where toml files is searched (recursieve)")
	verbose := flag.Bool("verbose", false, "verbose")
	flag.Parse()
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}

	var configs []string
	err = expand(*config, &configs)
	if err != nil {
		logger.Fatal(
			"config",
			zap.Error(err),
		)
	}
	if len(configs) == 0 {
		logger.Fatal("config should be non-null")
	}

	failed := 0
	total := 0
	for _, config := range configs {
		d, err := ioutil.ReadFile(config)
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", config),
				zap.Error(err),
			)
			failed++
			total++
			continue
		}

		testDir := path.Dir(config)
		confShort := strings.ReplaceAll(config, rootDir+"/", "")

		var cfg = MainConfig{}
		if _, err := toml.Decode(string(d), &cfg); err != nil {
			logger.Fatal("failed to decode config",
				zap.String("config", confShort),
				zap.Error(err),
			)
		}

		if len(cfg.Test.Input) == 0 {
			logger.Fatal("input not set",
				zap.String("config", confShort),
			)
		}

		for _, clickhouse := range cfg.Test.Clickhouse {
			total++
			testFail := false
			clickhouseDir := clickhouse.Dir // for logging
			clickhouse.Dir = rootDir + "/" + clickhouse.Dir
			err, out := clickhouse.Start()
			if err != nil {
				logger.Error("starting clickhouse",
					zap.String("config", confShort),
					zap.Any("clickhouse version", clickhouse.Version),
					zap.String("clickhouse config", clickhouseDir),
					zap.Error(err),
					zap.String("out", out),
				)
				failed++
				clickhouse.Stop(true)
				continue
			}

			cch := CarbonClickhouse{
				ConfigTpl: testDir + "/" + cfg.Test.ConfigTpl,
			}
			err = cch.Start(clickhouse.Address())
			if err != nil {
				logger.Error("starting carbon-clickhouse",
					zap.String("config", confShort),
					zap.String("clickhouse version", clickhouse.Version),
					zap.String("clickhouse config", clickhouseDir),
					zap.Error(err),
					zap.String("out", out),
				)
				testFail = true
			}

			if !testFail {
				logger.Info("starting e2e test",
					zap.String("config", confShort),
					zap.String("clickhouse version", clickhouse.Version),
					zap.String("clickhouse config", clickhouseDir),
				)
				time.Sleep(2 * time.Second)
				// Run test

				if len(cfg.Test.Input) > 0 {
					if err = sendPlain("tcp", cch.address, cfg.Test.Input); err != nil {
						logger.Error("send plain to carbon-clickhouse",
							zap.String("config", confShort),
							zap.String("clickhouse version", clickhouse.Version),
							zap.String("clickhouse config", clickhouseDir),
							zap.Error(err),
							zap.String("out", out),
						)
						testFail = true
					}
				}

				if !testFail {
					verifyFailed := 0
					time.Sleep(10 * time.Second)
					for _, verify := range cfg.Test.Verify {
						if errs := verifyOut(clickhouse.Address(), verify); len(errs) > 0 {
							testFail = true
							verifyFailed++
							for _, e := range errs {
								logger.Error(e)
							}
							logger.Error("verify records in clickhouse",
								zap.String("config", confShort),
								zap.String("clickhouse version", clickhouse.Version),
								zap.String("clickhouse config", clickhouseDir),
								zap.String("verify", verify.Query),
							)
						} else if *verbose {
							logger.Info("verify records in clickhouse",
								zap.String("config", confShort),
								zap.String("clickhouse version", clickhouse.Version),
								zap.String("clickhouse config", clickhouseDir),
								zap.String("verify", verify.Query),
							)
						}
					}
					if verifyFailed > 0 {
						logger.Error("verify records in clickhouse",
							zap.String("config", confShort),
							zap.String("clickhouse version", clickhouse.Version),
							zap.String("clickhouse config", clickhouseDir),
							zap.Int("verify failed", verifyFailed),
							zap.Int("verify total", len(cfg.Test.Verify)),
						)
					} else {
						logger.Info("verify records in clickhouse",
							zap.String("config", confShort),
							zap.String("clickhouse version", clickhouse.Version),
							zap.String("clickhouse config", clickhouseDir),
							zap.Int("verify success", len(cfg.Test.Verify)),
							zap.Int("verify total", len(cfg.Test.Verify)),
						)
					}
				}
			}

			err = cch.Stop()
			cch.Cleanup()
			if err != nil {
				logger.Error("stoping carbon-clickhouse",
					zap.String("config", confShort),
					zap.String("clickhouse version", clickhouse.Version),
					zap.String("clickhouse config", clickhouseDir),
					zap.Error(err),
					zap.String("out", out),
				)
				testFail = true
			}

			err, out = clickhouse.Stop(true)
			if err != nil {
				logger.Error("stoping clickhouse",
					zap.String("config", confShort),
					zap.String("clickhouse version", clickhouse.Version),
					zap.String("clickhouse config", clickhouseDir),
					zap.Error(err),
					zap.String("out", out),
				)
				testFail = true
			}

			if testFail {
				failed++
			} else {
				logger.Info("end e2e test",
					zap.String("config", confShort),
					zap.String("clickhouse version", clickhouse.Version),
					zap.String("clickhouse config", clickhouseDir),
				)
			}
		}
	}

	if failed > 0 {
		logger.Error("tests ended",
			zap.Bool("success", false),
			zap.Int("count", total),
			zap.Int("failed", failed),
			zap.Int("configs", len(configs)),
		)
		os.Exit(1)
	} else {
		logger.Info("tests ended",
			zap.Bool("success", true),
			zap.Int("count", total),
			zap.Int("configs", len(configs)),
		)
	}
}

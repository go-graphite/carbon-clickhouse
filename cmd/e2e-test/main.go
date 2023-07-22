package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"time"

	"go.uber.org/zap"
)

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

func expandFilename(filename string, paths *[]string) error {
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
	breakOnError := flag.Bool("break", false, "break and wait user response if request failed")
	abortOnError := flag.Bool("abort", false, "abort tests if test failed")
	cleanup := flag.Bool("cleanup", false, "delete containers if exists before start")
	rmi := flag.Bool("rmi", false, "delete images after test end (for low space usage))")
	flag.Parse()
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}

	DockerBinary = os.Getenv("DOCKER_E2E")
	if DockerBinary == "" {
		DockerBinary = "docker"
	}

	if *cleanup {
		if exist, _ := containerExist(ClickhouseContainerName); exist {
			if ok, out := containerRemove(ClickhouseContainerName); !ok {
				logger.Fatal("failed to cleanup",
					zap.String("container", ClickhouseContainerName),
					zap.String("error", out),
				)
				os.Exit(1)
			}
		}
		if len(*config) == 0 {
			return
		}
	}

	var allConfigs []string
	err = expandFilename(*config, &allConfigs)
	if err != nil {
		logger.Fatal(
			"config",
			zap.Error(err),
		)
	}
	if len(allConfigs) == 0 {
		logger.Fatal("config should be non-null")
	}

	chVersions := make(map[string]Clickhouse)
	configs := make([]*MainConfig, 0, len(allConfigs))
	for _, config := range allConfigs {
		cfg, err := loadConfig(config, rootDir)
		if err == nil {
			configs = append(configs, cfg)
			for _, ch := range cfg.Test.Clickhouse {
				chVersions[ch.Key()] = ch
			}
		} else {
			logger.Error("failed to read config",
				zap.String("config", config),
				zap.Error(err),
				zap.Any("decode", cfg),
			)
		}
	}

	failed := 0
	total := 0
	for chVersion, ch := range chVersions {
		if exist, out := containerExist(ClickhouseContainerName); exist {
			logger.Error("clickhouse already exist",
				zap.String("container", ClickhouseContainerName),
				zap.String("out", out),
			)
			os.Exit(1)
		}

		logger.Info("clickhouse",
			zap.Any("clickhouse image", ch.DockerImage),
			zap.Any("clickhouse version", ch.Version),
			zap.String("clickhouse config", ch.Dir),
			zap.String("tz", ch.TZ),
		)
		if clickhouseStart(&ch, logger) {
			time.Sleep(100 * time.Millisecond)
			for i := 200; i < 3000; i += 200 {
				if ch.Alive() {
					break
				}
				time.Sleep(time.Duration(i) * time.Millisecond)
			}
			if !ch.Alive() {
				logger.Error("starting clickhouse",
					zap.Any("clickhouse version", ch.Version),
					zap.String("clickhouse config", ch.Dir),
					zap.String("error", "clickhouse is down"),
				)
				failed++
				total++
			} else {
				for _, config := range configs {
					if config.Test.chVersions[chVersion] {
						testFailed, testTotal := runTest(config, &ch, rootDir, *verbose, *breakOnError, logger)
						failed += testFailed
						total += testTotal
					}
				}
			}
			if !clickhouseStop(&ch, logger) {
				failed++
			}
		}

		if *rmi {
			if success, out := imageDelete(ch.DockerImage, ch.Version); !success {
				logger.Error("docker remove image",
					zap.Any("clickhouse version", ch.Version),
					zap.String("clickhouse config", ch.Dir),
					zap.String("out", out),
				)
			}
		}
		if *abortOnError && failed > 0 {
			break
		}
	}

	if failed > 0 {
		logger.Error("tests ended",
			zap.String("status", "failed"),
			zap.Int("count", total),
			zap.Int("failed", failed),
			zap.Int("configs", len(configs)),
		)
		os.Exit(1)
	} else {
		logger.Info("tests ended",
			zap.String("status", "success"),
			zap.Int("count", total),
			zap.Int("configs", len(configs)),
		)
	}
}

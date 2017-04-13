package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/lomik/carbon-clickhouse/carbon"
	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/zapwriter"
	"github.com/uber-go/zap"

	_ "net/http/pprof"
)

// Version of carbon-clickhouse
const Version = "0.4"

func httpServe(addr string) (func(), error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}

	go http.Serve(listener, nil)
	return func() { listener.Close() }, nil
}

func main() {
	var err error

	/* CONFIG start */

	configFile := flag.String("config", "/etc/carbon-clickhouse/carbon-clickhouse.conf", "Filename of config")
	printDefaultConfig := flag.Bool("config-print-default", false, "Print default config")
	checkConfig := flag.Bool("check-config", false, "Check config and exit")
	printVersion := flag.Bool("version", false, "Print version")
	cat := flag.String("cat", "", "Print RowBinary file in TabSeparated format")
	bincat := flag.String("recover", "", "Read all good records from corrupted data file. Write binary data to stdout")

	flag.Parse()

	if *printVersion {
		fmt.Print(Version)
		return
	}

	if *cat != "" {
		reader, err := RowBinary.NewReader(*cat)
		if err != nil {
			log.Fatal(err)
		}

		for {
			metric, err := reader.ReadRecord()
			if err != nil {
				if err == io.EOF {
					return
				}
				log.Fatal(err)
			}

			fmt.Printf("%s\t%#v\t%d\t%s\t%d\n",
				string(metric),
				reader.Value(),
				reader.Timestamp(),
				reader.DaysString(),
				reader.Version(),
			)
		}
	}

	if *bincat != "" {
		reader, err := RowBinary.NewReader(*bincat)
		if err != nil {
			log.Fatal(err)
		}

		io.Copy(os.Stdout, reader)
		return
	}

	cfg := carbon.NewConfig()

	if *printDefaultConfig {
		if err = carbon.PrintConfig(cfg); err != nil {
			log.Fatal(err)
		}
		return
	}

	if err = carbon.ParseConfig(*configFile, cfg); err != nil {
		log.Fatal(err)
	}

	if *checkConfig {
		// check before logging init
		if _, err = carbon.New(cfg, zap.New(zap.NullEncoder())); err != nil {
			log.Fatal(err)
		}
		return
	}

	zapOutput, err := zapwriter.New(cfg.Logging.File)
	if err != nil {
		log.Fatal(err)
	}

	var logLevel zap.Level
	if err = logLevel.UnmarshalText([]byte(cfg.Logging.Level)); err != nil {
		log.Fatal(err)
	}

	dynamicLevel := zap.DynamicLevel()
	dynamicLevel.SetLevel(logLevel)

	logger := zap.New(
		zapwriter.NewMixedEncoder(),
		zap.AddCaller(),
		zap.Output(zapOutput),
		dynamicLevel,
	)

	app, err := carbon.New(cfg, logger)

	/* CONFIG end */

	// pprof
	if cfg.Pprof.Enabled {
		_, err = httpServe(cfg.Pprof.Listen)
		if err != nil {
			logger.Fatal("pprof listen failed", zap.Error(err))
		}
	}

	if err = app.Start(); err != nil {
		logger.Fatal("app start failed", zap.Error(err))
	} else {
		logger.Info("app started")
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGUSR1)

		for {
			<-c
			logger.Info("USR1 received. Clear tree cache")
			app.ClearTreeExistsCache()
		}
	}()

	app.Loop()

	logger.Info("app stopped")
}

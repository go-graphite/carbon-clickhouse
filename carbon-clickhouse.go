package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/lomik/carbon-clickhouse/carbon"
	"github.com/lomik/zapwriter"
	"github.com/uber-go/zap"
)

import _ "net/http/pprof"

// Version of carbon-clickhouse
const Version = "0.1"

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

	flag.Parse()

	if *printVersion {
		fmt.Print(Version)
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
		if _, err = carbon.New(cfg); err != nil {
			log.Fatal(err)
		}
		return
	}

	zapOutput, err := zapwriter.New(cfg.Common.LogFile)
	if err != nil {
		log.Fatal(err)
	}

	logger := zap.New(
		zapwriter.NewMixedEncoder(),
		zap.AddCaller(),
		zap.Output(zapOutput),
	)
	logger.SetLevel(cfg.Common.LogLevel)

	app, err := carbon.New(cfg)

	/* CONFIG end */

	// pprof
	// if cfg.Pprof.Enabled {
	// 	_, err = httpServe(cfg.Pprof.Listen)
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// }

	if err = app.Start(); err != nil {
		logger.Fatal("app start failed", zap.Error(err))
	} else {
		logger.Info("app started")
	}

	// go func() {
	// 	c := make(chan os.Signal, 1)
	// 	signal.Notify(c, syscall.SIGHUP)
	// 	for {
	// 		<-c
	// 		logrus.Info("HUP received. Reload config")
	// 		if err := app.ReloadConfig(); err != nil {
	// 			logrus.Errorf("Config reload failed: %s", err.Error())
	// 		} else {
	// 			logrus.Info("Config successfully reloaded")
	// 		}
	// 	}
	// }()

	app.Loop()

	logger.Info("app stopped")
}

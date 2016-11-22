package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/lomik/carbon-clickhouse/carbon"
	"github.com/lomik/go-carbon/logging"
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

	configFile := flag.String("config", "", "Filename of config")
	printDefaultConfig := flag.Bool("config-print-default", false, "Print default config")
	checkConfig := flag.Bool("check-config", false, "Check config and exit")

	printVersion := flag.Bool("version", false, "Print version")

	flag.Parse()

	if *printVersion {
		fmt.Print(Version)
		return
	}

	if *printDefaultConfig {
		if err = carbon.PrintConfig(carbon.NewConfig()); err != nil {
			log.Fatal(err)
		}
		return
	}

	app := carbon.New(*configFile)

	if err = app.ParseConfig(); err != nil {
		log.Fatal(err)
	}

	cfg := app.Config

	if err := logging.SetLevel(cfg.Common.LogLevel); err != nil {
		log.Fatal(err)
	}

	// config parsed successfully. Exit in check-only mode
	if *checkConfig {
		return
	}

	if err := logging.PrepareFile(cfg.Common.LogFile, nil); err != nil {
		logrus.Fatal(err)
	}

	if err := logging.SetFile(cfg.Common.LogFile); err != nil {
		logrus.Fatal(err)
	}

	runtime.GOMAXPROCS(cfg.Common.MaxCPU)

	/* CONFIG end */

	// pprof
	if cfg.Pprof.Enabled {
		_, err = httpServe(cfg.Pprof.Listen)
		if err != nil {
			logrus.Fatal(err)
		}
	}

	if err = app.Start(); err != nil {
		logrus.Fatal(err)
	} else {
		logrus.Info("started")
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGHUP)
		for {
			<-c
			logrus.Info("HUP received. Reload config")
			if err := app.ReloadConfig(); err != nil {
				logrus.Errorf("Config reload failed: %s", err.Error())
			} else {
				logrus.Info("Config successfully reloaded")
			}
		}
	}()

	app.Loop()

	logrus.Info("stopped")
}

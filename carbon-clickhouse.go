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
	"runtime/debug"
	"syscall"
	"time"

	"github.com/lomik/carbon-clickhouse/carbon"
	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	_ "net/http/pprof"
)

// Version of carbon-clickhouse
const Version = "0.11.7"

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
	exactConfig := flag.Bool("exact-config", false, "Ensure that all config params are contained in the target struct.")
	printVersion := flag.Bool("version", false, "Print version")
	cat := flag.String("cat", "", "Print RowBinary file in TabSeparated format")
	bincat := flag.String("recover", "", "Read all good records from corrupted data file. Write binary data to stdout")

	flag.Parse()

	if *printVersion {
		fmt.Print(Version)
		return
	}

	if *cat != "" {
		reader, err := RowBinary.NewReader(*cat, false)
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
		reader, err := RowBinary.NewReader(*bincat, false)
		if err != nil {
			log.Fatal(err)
		}

		io.Copy(os.Stdout, reader)
		return
	}

	if *printDefaultConfig {
		if err = carbon.PrintDefaultConfig(); err != nil {
			log.Fatal(err)
		}
		return
	}

	app := carbon.New(*configFile)

	if err = app.ParseConfig(*exactConfig); err != nil {
		log.Fatal(err)
	}

	// config parsed successfully. Exit in check-only mode
	if *checkConfig {
		return
	}

	cfg := app.Config

	if err = zapwriter.ApplyConfig(cfg.Logging); err != nil {
		log.Fatal(err)
	}

	mainLogger := zapwriter.Logger("main")

	/* CONFIG end */

	// pprof
	if cfg.Pprof.Enabled {
		_, err = httpServe(cfg.Pprof.Listen)
		if err != nil {
			mainLogger.Fatal("pprof listen failed", zap.Error(err))
		}
	}

	if err = app.Start(); err != nil {
		mainLogger.Fatal("app start failed", zap.Error(err))
	} else {
		mainLogger.Info("app started")
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)

		for {
			s := <-c
			switch s {
			case syscall.SIGUSR1:
				mainLogger.Info("SIGUSR1 received. Clear tree cache")
				app.Reset()
			case syscall.SIGUSR2:
				mainLogger.Info("SIGUSR2 received. Ignoring")
			case syscall.SIGHUP:
				mainLogger.Info("SIGHUP received. Ignoring")
			case syscall.SIGTERM, syscall.SIGINT:
				mainLogger.Info("shutting down")
				app.Stop()
			}
		}
	}()

	go func() {
		for {
			debug.FreeOSMemory()
			time.Sleep(30 * time.Minute)
		}
	}()

	app.Loop()

	mainLogger.Info("app stopped")
}

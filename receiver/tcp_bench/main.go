package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/lomik/carbon-clickhouse/receiver"

	_ "net/http/pprof"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	inCh := make(chan *receiver.Buffer, 128)
	outCh := make(chan *receiver.WriteBuffer, 128)

	workers := 4
	for i := 0; i < workers; i++ {
		go receiver.PlainParser(inCh, outCh)
	}

	// out channel cleaner
	go func() {
		for {
			w := <-outCh
			receiver.WriteBufferPool.Put(w)
		}
	}()

	rcv, err := receiver.New(
		"tcp://127.0.0.1:0",
		receiver.HandleBuffer(func(b *receiver.Buffer) { inCh <- b }),
	)
	defer rcv.Stop()

	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.Dial("tcp", rcv.(*receiver.TCP).Addr().String())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	var buf bytes.Buffer

	chunk := 50
	for i := 0; i < chunk; i++ {
		buf.Write([]byte("carbon.agents.localhost.cache.size 1412351 1479933789\n"))
	}

	printEvery := 1000000 / chunk

	body := buf.Bytes()

	cnt := 0
	t := time.Now()
	for {
		if _, err := conn.Write(body); err != nil {
			log.Fatal(err)
		}

		cnt++
		if cnt%printEvery == 0 {
			fmt.Printf("%.2f p/s\n", 1000000.0/time.Since(t).Seconds())
			t = time.Now()
		}
	}
}

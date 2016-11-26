package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	_ "net/http/pprof"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	conn, err := net.Dial("tcp", "127.0.0.1:2003")
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

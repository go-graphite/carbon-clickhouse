package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"time"
	"math/rand"
	"flag"
)

func body(hosts int, plugins int, values int, hostStart int)([]([]byte)) {

	out := make([][]byte, hosts*plugins)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	bufcount := 0
	for host := 0; host < hosts; host++ {
		for plugin  := 0; plugin < plugins; plugin++ {
			var buf bytes.Buffer
			for value := 0; value < values; value++ {
				buf.Write(
					[]byte(
						fmt.Sprintf(
							"loadtest.host%d.plugin%d.stuff%d.value %f %d\n",
							(host + hostStart),
							plugin,
							value,
							r.NormFloat64(),
							time.Now().Unix())))
			}
			out[bufcount] = buf.Bytes()
			bufcount++
		}
	}
	return out
}

func main() {
	hostFactor := flag.Int("hostfactor", 1, "factor to multiply the host number with")
	flag.Parse()

	conn, err := net.Dial("tcp", "127.0.0.1:2003")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	cnt := 0
	t := time.Now()

	hosts := 1000
	plugins := 50
	values := 10
	printEvery := 1
	hostStart := (*hostFactor - 1) * hosts
	for {
		body := body(hosts, plugins, values, hostStart)
		for i :=0; i < len(body); i++ {
			if _, err := conn.Write(body[i]); err != nil {
				log.Fatal(err)
			}
		}

		cnt++
		if cnt%printEvery == 0 {
			fmt.Printf("%.2f p/s\n", float64(printEvery * hosts * plugins * values)/time.Since(t).Seconds())
			t = time.Now()
		}
	}
}

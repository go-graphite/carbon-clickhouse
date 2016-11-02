package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	filename := flag.String("file", "", "Filename")
	flag.Parse()

	file, err := os.Open(*filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1024*1024)

	uniq := make(map[string]bool)

	var key string
	var level int
	var exists bool
	var date string

LineLoop:
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		row := strings.Split(string(line), "\t")
		metric := row[0]

		if _, exists = uniq[metric]; exists {
			continue LineLoop
		}

		date = row[3][:8] + "01" // first day of month

		offset := 0
		for level = 1; ; level++ {
			p := strings.IndexByte(metric[offset:], '.')
			if p < 0 {
				break
			}
			key = metric[:offset+p+1]

			if _, exists := uniq[key]; !exists {
				uniq[key] = true
				fmt.Println(date, level, key)
			}

			offset += p + 1
		}
		// words := strings.Split(metric, ".")
		// w := len(words)

		// for i := 0; i < w-1; i++ {
		// 	k := strings.Join(words[:i+1], ".") + "."
		// 	_, exists := uniq[k]
		// 	if !exists {
		// 		uniq[k] = true
		// 		fmt.Println(k)
		// 	}
		// }

		uniq[metric] = true
		fmt.Println(date, level, metric)

		// _, exists := uniq[metric]
		// if !exists {
		// uniq[metric] = true
		// }
	}

	fmt.Println(len(uniq))
	// fmt.Println(x)

	time.Sleep(time.Minute)
}

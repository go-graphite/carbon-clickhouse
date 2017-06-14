package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"google.golang.org/grpc"

	pb "github.com/lomik/carbon-clickhouse/grpc"
)

func makeRequests(hosts int, plugins int, values int, hostStart int) []*pb.Payload {

	out := make([]*pb.Payload, hosts*plugins)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	bufcount := 0
	for host := 0; host < hosts; host++ {
		for plugin := 0; plugin < plugins; plugin++ {

			request := &pb.Payload{
				Metrics: make([]*pb.Metric, 0, values),
			}

			for value := 0; value < values; value++ {
				request.Metrics = append(request.Metrics, &pb.Metric{
					Metric: fmt.Sprintf(
						"loadtest.host%d.plugin%d.stuff%d.value",
						(host + hostStart),
						plugin,
						value,
					),
					Points: []*pb.Point{
						&pb.Point{
							Timestamp: uint32(time.Now().Unix()),
							Value:     r.NormFloat64(),
						},
					},
				})
			}
			out[bufcount] = request
			bufcount++
		}
	}
	return out
}

func main() {
	hostFactor := flag.Int("hostfactor", 1, "factor to multiply the host number with")
	flag.Parse()

	// conn, err := net.Dial("tcp", "127.0.0.1:2003")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer conn.Close()

	conn, err := grpc.Dial("127.0.0.1:2005", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewCarbonClient(conn)

	cnt := 0
	t := time.Now()

	hosts := 1000
	plugins := 50
	values := 10
	printEvery := 1
	hostStart := (*hostFactor - 1) * hosts
	queries := 0

	for {
		requests := makeRequests(hosts, plugins, values, hostStart)

		for i := 0; i < len(requests); i++ {
			// fmt.Println(requests[i])
			_, err := c.StoreSync(context.Background(), requests[i])
			if err != nil {
				log.Fatalf("could not store: %v", err)
			}
			queries++
		}

		cnt++
		if cnt%printEvery == 0 {
			d := time.Since(t).Seconds()
			fmt.Printf("%.2f p/s %.2f q/s\n",
				float64(printEvery*hosts*plugins*values)/d,
				float64(queries)/d,
			)
			t = time.Now()
			queries = 0
		}
	}
}

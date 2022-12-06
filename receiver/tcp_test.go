package receiver

import (
	"bytes"
	"context"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/RowBinary/reader"
	"github.com/lomik/carbon-clickhouse/helper/tags"
	"github.com/lomik/carbon-clickhouse/helper/tests"
	"github.com/msaf1980/go-stringutils"
)

func TestTCP(t *testing.T) {
	writeChan := make(chan *RowBinary.WriteBuffer)
	address, err := tests.GetFreeTCPPort("")
	if err != nil {
		t.Fatal(err)
	}

	tcp, err := New(
		"tcp://"+address,
		tags.DisabledTagConfig(),
		ParseThreads(runtime.GOMAXPROCS(-1)*2),
		WriteChan(writeChan),
		DropFuture(uint32(0)),
		DropPast(uint32(0)),
		DropLongerThan(0),
		ReadTimeout(uint32(120)),
	)
	if err != nil {
		t.Fatal(err)
	}

	var sb stringutils.Builder

	now := uint32(time.Now().Unix())

	points_part1 := []reader.Point{
		{
			Path:      "carbon.agents.carbon-clickhouse.writer.writtenBytes",
			Value:     1.0,
			Timestamp: 1559465760,
			Days:      18049,
		},
		{
			Path:      "carbon.agents.carbon-clickhouse.writer.writtenBytes",
			Value:     2.0,
			Timestamp: 1559465800,
			Days:      18049,
		},
		{
			Path:      "carbon.agents.carbon-clickhouse.tcp.receiver.errors",
			Value:     2.0,
			Timestamp: 1559465800,
			Days:      18049,
		},
		{
			// next day
			Path:      "carbon.agents.carbon-clickhouse.tcp.receiver.errors",
			Value:     1.0,
			Timestamp: 1662098177,
			Days:      19237,
		},
		{
			// tagged metric
			Path:      "errors?app=carbon-clickhouse&scope=tcp",
			Value:     1.0,
			Timestamp: 1662098177,
			Days:      19237,
		},
	}
	for _, p := range points_part1 {
		pointToBuf(&sb, &p)
	}

	points_part2 := "carbon.age"
	sb.WriteString(points_part2)

	bytes1 := append([]byte{}, sb.Bytes()...)
	sb.Reset()

	// send after 1 sec delay, so increment version
	points_part3 := reader.Point{
		Path:      "nts.carbon-clickhouse.udp.receiver.errors",
		Value:     2.0,
		Timestamp: 1662098177,
		Days:      19237,
	}
	pointToBuf(&sb, &points_part3)

	points_part4 := []reader.Point{
		{
			Path:      "carbon.agents.carbon-clickhouse.writer.writtenBytes",
			Value:     1.1,
			Timestamp: 1559465761,
			Days:      18049,
		},
		{
			Path:      "carbon.agents.carbon-clickhouse.writer.writtenBytes",
			Value:     2.1,
			Timestamp: 1559465801,
			Days:      18049,
		},
		{
			Path:      "carbon.agents.carbon-clickhouse.tcp.receiver.errors",
			Value:     2.1,
			Timestamp: 1559465801,
			Days:      18049,
		},
		{
			// next day
			Path:      "carbon.agents.carbon-clickhouse.tcp.receiver.errors",
			Value:     1.1,
			Timestamp: 1662098178,
			Days:      19237,
		},
		{
			// tagged metric
			Path:      "errors?app=carbon-clickhouse&scope=tcp",
			Value:     1.1,
			Timestamp: 1662098178,
			Days:      19237,
		},
	}
	for _, p := range points_part4 {
		pointToBuf(&sb, &p)
	}

	wantPoints := append([]reader.Point{}, points_part1...)
	wantPoints = append(wantPoints, reader.Point{
		Path:      points_part2 + points_part3.Path,
		Value:     points_part3.Value,
		Timestamp: points_part3.Timestamp,
		Days:      points_part3.Days,
		Version:   points_part3.Version,
	})
	wantPoints = append(wantPoints, points_part4...)

	// simulate writer
	var rawBuf bytes.Buffer
	rawBuf.Grow(524288)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case b := <-writeChan:
				rawBuf.Write(b.Bytes())
				b.Release()
			case <-ctx.Done():
				return
			}
		}
	}()

	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		tcp.Stop()
		cancel()
		t.Fatal(err)
	}

	conn.SetDeadline(time.Now().Add(time.Second))
	if _, err = conn.Write(bytes1); err != nil {
		conn.Close()
		tcp.Stop()
		cancel()
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	conn.SetDeadline(time.Now().Add(time.Second))
	if _, err = conn.Write(sb.Bytes()); err != nil {
		conn.Close()
		tcp.Stop()
		cancel()
		t.Fatal(err)
	}

	if err = conn.Close(); err != nil {
		tcp.Stop()
		cancel()
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	tcp.Stop()
	cancel()
	wg.Wait()

	verifyIndexUploaded(t, &rawBuf, wantPoints, now, uint32(time.Now().Unix()))
}

package receiver

import (
	"encoding/binary"
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/golang/protobuf/ptypes/empty"
	pb "github.com/lomik/carbon-clickhouse/grpc"
	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/days1970"
	"github.com/lomik/stop"
	"go.uber.org/zap"
)

// GRPC receive metrics from GRPC connections
type GRPC struct {
	stop.Struct
	stat struct {
		metricsReceived uint32 // atomic
		errors          uint32 // atomic
		active          int32  // atomic
	}
	listener     *net.TCPListener
	parseThreads int
	writeChan    chan *RowBinary.WriteBuffer
	logger       *zap.Logger
}

// Addr returns binded socket address. For bind port 0 in tests
func (g *GRPC) Addr() net.Addr {
	if g.listener == nil {
		return nil
	}
	return g.listener.Addr()
}

func (g *GRPC) Stat(send func(metric string, value float64)) {
	metricsReceived := atomic.LoadUint32(&g.stat.metricsReceived)
	atomic.AddUint32(&g.stat.metricsReceived, -metricsReceived)
	send("metricsReceived", float64(metricsReceived))

	errors := atomic.LoadUint32(&g.stat.errors)
	atomic.AddUint32(&g.stat.errors, -errors)
	send("errors", float64(errors))
}

// Listen bind port. Receive messages and send to out channel
func (g *GRPC) Listen(addr *net.TCPAddr) error {
	return g.StartFunc(func() error {

		tcpListener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return err
		}

		s := grpc.NewServer()
		pb.RegisterCarbonServer(s, g)
		// Register reflection service on gRPC server.
		reflection.Register(s)

		if err := s.Serve(tcpListener); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}

		g.Go(func(exit chan struct{}) {
			<-exit
			s.Stop()
		})

		g.Go(func(exit chan struct{}) {
			defer s.Stop()

			if err := s.Serve(tcpListener); err != nil {
				g.logger.Error("failed to serve", zap.Error(err))
			}

		})

		g.listener = tcpListener

		return nil
	})
}

func (g *GRPC) doStore(ctx context.Context, in *pb.Message, confirmRequired bool) error {
	// validate
	if in == nil {
		return nil
	}

	if in.Data == nil {
		return nil
	}

	pointsCount := uint32(0)

	for i := 0; i < len(in.Data); i++ {
		m := in.Data[i]

		if m == nil {
			return errors.New("metric is empty")
		}

		if m.Name == nil || len(m.Name) == 0 {
			return errors.New("name is empty")
		}

		if len(m.Name) > 16384 {
			return errors.New("name too long")
		}

		if m.Timestamps == nil || len(m.Timestamps) == 0 {
			return errors.New("timestamps is empty")
		}

		if m.Values == nil || len(m.Values) == 0 {
			return errors.New("values is empty")
		}

		if len(m.Values) != len(m.Timestamps) {
			return errors.New("len(values) != len(timestamps)")
		}

		pointsCount += uint32(len(m.Values))
	}

	// save to buffers
	now := uint32(time.Now().Unix())

	version := make([]byte, 4)
	binary.LittleEndian.PutUint32(version, now)

	var days days1970.Days

	var wg *sync.WaitGroup
	var errorChan chan error

	if confirmRequired {
		wg = new(sync.WaitGroup)
		errorChan = make(chan error, 1)
	}

	var exit chan struct{}
	// hack for get private exit channel
	g.WithExit(func(e chan struct{}) {
		exit = e
	})

	wb := RowBinary.GetWriterBufferWithConfirm(wg, errorChan)
	for i := 0; i < len(in.Data); i++ {
		m := in.Data[i]

		for j := 0; j < len(m.Values); j++ {
			if !wb.CanWriteGraphitePoint(len(m.Name)) {
				select {
				case g.writeChan <- wb:
					// pass
				case <-exit:
					return errors.New("receiver stopped")
				}

				wb = RowBinary.GetWriterBufferWithConfirm(wg, errorChan)
			}

			wb.WriteBytes(m.Name)
			wb.WriteFloat64(m.Values[j])
			wb.WriteUint32(m.Timestamps[j])
			wb.WriteUint16(days.TimestampWithNow(m.Timestamps[j], now))
			wb.Write(version)
		}
	}

	if wb.Empty() {
		wb.Release()
	} else {
		select {
		case g.writeChan <- wb:
			// pass
		case <-exit:
			return errors.New("receiver stopped")
		}
	}

	if confirmRequired {
		wg.Wait()

		select {
		case err := <-errorChan:
			return err
		default:
		}
	}

	return nil
}

func (g *GRPC) Store(ctx context.Context, in *pb.Message) (*empty.Empty, error) {
	err := g.doStore(ctx, in, false)
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (g *GRPC) StoreSync(ctx context.Context, in *pb.Message) (*empty.Empty, error) {
	err := g.doStore(ctx, in, true)
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

package receiver

import (
	"encoding/binary"
	"errors"
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
	"github.com/lomik/carbon-clickhouse/helper/tags"
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

		g.Go(func(exit chan struct{}) {
			<-exit
			s.Stop()
		})

		g.Go(func(exit chan struct{}) {
			defer s.Stop()

			if err := s.Serve(tcpListener); err != nil {
				g.logger.Fatal("failed to serve", zap.Error(err))
			}

		})

		g.listener = tcpListener

		return nil
	})
}

func (g *GRPC) doStore(ctx context.Context, in *pb.Payload, confirmRequired bool) error {
	// validate
	if in == nil {
		return nil
	}

	if in.Metrics == nil {
		return nil
	}

	pointsCount := uint32(0)

	for i := 0; i < len(in.Metrics); i++ {
		m := in.Metrics[i]

		if m == nil {
			return errors.New("metric is empty")
		}

		if len(m.Metric) == 0 {
			return errors.New("name is empty")
		}

		if len(m.Metric) > 16384 {
			return errors.New("name too long")
		}

		if m.Points == nil || len(m.Points) == 0 {
			return errors.New("points is empty")
		}

		name, err := tags.Graphite(m.Metric)
		if err != nil {
			return err
		}
		m.Metric = name

		pointsCount += uint32(len(m.Points))
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
	for i := 0; i < len(in.Metrics); i++ {
		m := in.Metrics[i]

		for j := 0; j < len(m.Points); j++ {
			if !wb.CanWriteGraphitePoint(len(m.Metric)) {
				select {
				case g.writeChan <- wb:
					// pass
				case <-exit:
					return errors.New("receiver stopped")
				}

				wb = RowBinary.GetWriterBufferWithConfirm(wg, errorChan)
			}

			wb.WriteBytes([]byte(m.Metric))
			wb.WriteFloat64(m.Points[j].Value)
			wb.WriteUint32(m.Points[j].Timestamp)
			wb.WriteUint16(days.TimestampWithNow(m.Points[j].Timestamp, now))
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

func (g *GRPC) Store(ctx context.Context, in *pb.Payload) (*empty.Empty, error) {
	err := g.doStore(ctx, in, false)
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (g *GRPC) StoreSync(ctx context.Context, in *pb.Payload) (*empty.Empty, error) {
	err := g.doStore(ctx, in, true)
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

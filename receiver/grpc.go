package receiver

import (
	"errors"
	"net"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/golang/protobuf/ptypes/empty"
	pb "github.com/lomik/carbon-clickhouse/grpc"
	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/tags"
	"go.uber.org/zap"
)

// GRPC receive metrics from GRPC connections
type GRPC struct {
	Base
	listener *net.TCPListener
}

// Addr returns binded socket address. For bind port 0 in tests
func (g *GRPC) Addr() net.Addr {
	if g.listener == nil {
		return nil
	}
	return g.listener.Addr()
}

func (g *GRPC) Stat(send func(metric string, value float64)) {
	g.SendStat(send, "metricsReceived", "errors")
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

		g.Go(func(ctx context.Context) {
			<-ctx.Done()
			s.Stop()
		})

		g.Go(func(ctx context.Context) {
			defer s.Stop()

			if err := s.Serve(tcpListener); err != nil {
				g.logger.Fatal("failed to serve", zap.Error(err))
			}

		})

		g.listener = tcpListener

		return nil
	})
}

func (g *GRPC) doStore(requestCtx context.Context, in *pb.Payload, confirmRequired bool) error {
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

	var wg *sync.WaitGroup
	var errorChan chan error

	if confirmRequired {
		wg = new(sync.WaitGroup)
		errorChan = make(chan error, 1)
	}

	var receverCtx context.Context
	// hack for get private exit channel
	g.WithCtx(func(c context.Context) {
		receverCtx = c
	})

	wb := RowBinary.GetWriterBufferWithConfirm(wg, errorChan)
	for i := 0; i < len(in.Metrics); i++ {
		m := in.Metrics[i]

		for j := 0; j < len(m.Points); j++ {
			if !wb.CanWriteGraphitePoint(len(m.Metric)) {
				select {
				case g.writeChan <- wb:
					// pass
				case <-receverCtx.Done():
					return errors.New("receiver stopped")
				case <-requestCtx.Done():
					return errors.New("request canceled")
				}

				wb = RowBinary.GetWriterBufferWithConfirm(wg, errorChan)
			}

			wb.WriteGraphitePoint(
				[]byte(m.Metric),
				m.Points[j].Value,
				m.Points[j].Timestamp,
				now,
			)
		}
	}

	if wb.Empty() {
		wb.Release()
	} else {
		select {
		case g.writeChan <- wb:
			// pass
		case <-receverCtx.Done():
			return errors.New("receiver stopped")
		case <-requestCtx.Done():
			return errors.New("request canceled")
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

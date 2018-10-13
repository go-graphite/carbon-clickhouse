package receiver

import (
	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/stop"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type Base struct {
	stop.Struct
	stat         stat
	ctx          context.Context
	ctxCancel    context.CancelFunc
	parseThreads int
	writeChan    chan *RowBinary.WriteBuffer
	logger       *zap.Logger
}

func NewBase(logger *zap.Logger) Base {
	return Base{logger: logger}
}

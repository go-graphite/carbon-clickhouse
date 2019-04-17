package uploader

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
)

type PointsReverse struct {
	*Base
}

func NewPointsReverse(base *Base) *PointsReverse {
	u := &PointsReverse{Base: base}
	u.Base.handler = u.upload
	u.query = fmt.Sprintf("%s (Path, Value, Time, Date, Timestamp)", u.config.TableName)
	return u
}

func (u *PointsReverse) upload(ctx context.Context, logger *zap.Logger, filename string) error {
	reader, err := RowBinary.NewReverseReader(filename, u.config.CompAlgo)
	if err != nil {
		return err
	}

	reader.SetZeroVersion(u.config.ZeroTimestamp)
	return u.insertRowBinary(u.query, reader)
}

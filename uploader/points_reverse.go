package uploader

import (
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
	return u
}

func (u *PointsReverse) upload(exit chan struct{}, logger *zap.Logger, filename string) error {
	reader, err := RowBinary.NewReverseReader(filename)
	if err != nil {
		return err
	}

	return u.insertRowBinary(
		fmt.Sprintf("%s (Path, Value, Time, Date, Timestamp)", u.config.TableName),
		reader,
	)
}

package uploader

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
)

type Points struct {
	*Base
	reverse bool
}

func NewPoints(base *Base, reverse bool) *Points {
	u := &Points{Base: base}
	u.Base.handler = u.upload
	u.reverse = reverse
	u.query = fmt.Sprintf("%s (Path, Value, Time, Date, Timestamp)", u.config.TableName)
	return u
}

func (u *Points) upload(ctx context.Context, logger *zap.Logger, filename string) error {
	reader, err := RowBinary.NewReader(filename, u.reverse)
	if err != nil {
		return err
	}

	reader.SetZeroVersion(u.config.ZeroTimestamp)
	return u.insertRowBinary(u.query, reader)
}

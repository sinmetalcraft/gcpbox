package times

import (
	"context"
	"time"
)

type TimeService struct {
	JST *time.Location
}

func NewService(ctx context.Context) (*TimeService, error) {
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		return nil, err
	}
	return &TimeService{jst}, nil
}

// JSTDayChangeTime is 渡した時刻の0:00:00時点をJSTで返す
func (s *TimeService) JSTDayChangeTime(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, s.JST)
}

// UTCDayChangeTime is 渡した時刻の0:00:00時点をUTCで返す
func (s *TimeService) UTCDayChangeTime(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

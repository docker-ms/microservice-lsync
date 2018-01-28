package util

import (
  "time"
)

func GetEpochMilliseconds() int64 {
  return time.Now().UnixNano() / int64(time.Millisecond)
}

func GetSnapshotPoint() int64 {
  now := time.Now()
  fixedTimePoint := time.Date(now.Year(), now.Month(), now.Day(), 4, 0, 0, 0, time.UTC)
  return fixedTimePoint.UnixNano() / int64(time.Millisecond)
}



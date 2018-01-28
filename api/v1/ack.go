package v1

import (
	"sync"

	"lsync/boltdb"
	"lsync/constant"
	"lsync/util"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"

	pbMs "microservice"
	pbMsLsyncV1 "microservice/lsync/v1"
)

func Ack(in *pbMsLsyncV1.AckReq) (*pbMs.BoolRes, error) {
	var wg sync.WaitGroup

	wg.Add(1)
	go removeFrozenRecords(&wg, in.DeviceId)

	wg.Add(1)
	go updateLastSeen(&wg, in.UserId, in.DeviceId)

	wg.Wait()

	return &pbMs.BoolRes{
		Key: &pbMs.BoolRes_Success{
			Success: true,
		},
	}, nil
}

func removeFrozenRecords(wg *sync.WaitGroup, deviceId string) {
	defer wg.Done()

	boltdb.UserBucketsDB.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(constant.BN_DEVICES)).Bucket([]byte(deviceId))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var dbc pbMsLsyncV1.DeviceBucketContent
			if e := proto.Unmarshal(v, &dbc); e == nil {
				if dbc.IsRecordFrozen {
					if e0 := b.Delete(k); e0 != nil {
						return e0
					}
				}
			}
		}

		return nil
	})
}

func updateLastSeen(wg *sync.WaitGroup, userId, deviceId string) {
	defer wg.Done()

	now := util.GetEpochMilliseconds()

	boltdb.UserBucketsDB.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(constant.BN_USER_DEVICES_BUCKETS_MAP)).Bucket([]byte(userId))
		c := b.Cursor()
		k, v := c.First()

		var udbm pbMsLsyncV1.UserDevicesBucketsMap
		if e := proto.Unmarshal(v, &udbm); e == nil {
			if udbm.LastSeen < now {
				udbm.LastSeen = now

				if buf, e0 := udbm.Marshal(); e0 == nil {
					if e1 := b.Put(k, buf); e1 != nil {
						return e1
					}
				} else {
					return e0
				}
			}
		}

		return nil
	})
}

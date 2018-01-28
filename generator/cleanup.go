package generator

import (
	"sync"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"gopkg.in/mgo.v2/bson"

	"lsync/boltdb"
	"lsync/constant"
	"lsync/mongodb"

	pbMsLsyncV1 "microservice/lsync/v1"
)

func TransformDeviceBucketStatus(wg *sync.WaitGroup) {
	defer wg.Done()

	var pairs []interface{}

	boltdb.UserBucketsDB.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(constant.BN_USER_DEVICES_BUCKETS_MAP))
		b0 := tx.Bucket([]byte(constant.BN_DEVICES))

		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			ib := b.Bucket(k)
			ic := ib.Cursor()
			ik, iv := ic.First()
			var udbm pbMsLsyncV1.UserDevicesBucketsMap
			if e := proto.Unmarshal(iv, &udbm); e == nil {
				for did, dbs := range udbm.DeviceId2DeviceBucketStatus {
					if dbs {
						dbs = false
					} else {
						delete(udbm.DeviceId2DeviceBucketStatus, did)
						b0.DeleteBucket(ik)
					}
				}

				if len(udbm.DeviceId2DeviceBucketStatus) == 0 {
					b.DeleteBucket(k)

					pairs = append(pairs, bson.M{
						"userId": string(k[:]),
					}, bson.M{
						"$set": bson.M{
							"lastSeen": udbm.LastSeen,
						},
					})

					if len(pairs)%constant.MGO_MAX_BULK_PAIRS_SIZE == 0 {
						wg.Add(1)
						go mongodb.BulkUpdate(wg, constant.CN_USERS, pairs)

						pairs = pairs[:0]
					}
				} else {
					if buf, e0 := udbm.Marshal(); e0 == nil {
						if e1 := b.Put(k, buf); e1 != nil {
							return e1
						}
					} else {
						return e0
					}
				}
			} else {
				return e
			}
		}

		if len(pairs) > 0 {
			wg.Add(1)
			go mongodb.BulkUpdate(wg, constant.CN_USERS, pairs)
		}

		return nil
	})
}

package boltdb

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"sync"

	"github.com/boltdb/bolt"

	"lsync/constant"

	pbMsLsyncV1 "microservice/lsync/v1"
)

func SnapshotDeltaBulkInsert(wg *sync.WaitGroup, bucketName string, docs interface{}) {
	defer wg.Done()

	if err := SnapshotDeltaDB.Update(func(tx *bolt.Tx) error {

		switch data := docs.(type) {
		case map[string]*pbMsLsyncV1.SnapshotByConv:
			bucket := tx.Bucket([]byte(bucketName))
			for _, doc := range data {
				if buf, marshalErr := doc.Marshal(); marshalErr != nil {
					return marshalErr
				} else {
					if putErr := bucket.Put(Uint64ToB(doc.Id), buf); putErr != nil {
						return putErr
					}
				}
			}
		case map[string]*pbMsLsyncV1.UserSnapshotsMap:
			bucket := tx.Bucket([]byte(bucketName))
			for _, doc := range data {
				id, _ := bucket.NextSequence()
				doc.Id = id
				if buf, marshalErr := doc.Marshal(); marshalErr != nil {
					return marshalErr
				} else {
					if putErr := bucket.Put(Uint64ToB(doc.Id), buf); putErr != nil {
						return putErr
					}
				}
			}
		case map[int64]map[string]*pbMsLsyncV1.Delta:
			var buffer bytes.Buffer
			buffer.WriteString(constant.BN_DELTAS_BUCKETS_PREFIX)
			for ver, convDeltaMaps := range data {
				buffer.WriteString(strconv.FormatInt(ver, 10))
				bucket, err := tx.CreateBucketIfNotExists([]byte(buffer.String()))
				buffer.Reset()

				for _, delta := range convDeltaMaps {
					if buf, marshalErr := delta.Marshal(); marshalErr != nil {
						return err
					} else {
						if putErr := bucket.Put(Uint64ToB(delta.Id), buf); putErr != nil {
							return err
						}
					}
				}
			}
		}

		return nil
	}); err != nil {
		panic(err)
	}
}

func Uint64ToB(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

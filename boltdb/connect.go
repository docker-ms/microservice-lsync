package boltdb

import (
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"

	"lsync/constant"
)

var SnapshotDeltaDB, UserBucketsDB *bolt.DB

var BoltdbSnapshotDeltaDataFilePath, BoltdbUserBucketsDataFilePath string

func init() {
	BoltdbSnapshotDeltaDataFilePath = filepath.Join(os.Getenv("HOME"), ".bolt.sd.db")
	BoltdbUserBucketsDataFilePath = filepath.Join(os.Getenv("HOME"), ".bolt.ubs.db")
}

func InitBoltDB(dataFilePath string) {
	if conn, err := bolt.Open(dataFilePath, 0600, &bolt.Options{
		Timeout:         0,
		NoGrowSync:      false,
		ReadOnly:        false,
		InitialMmapSize: 0,
	}); err != nil {
		panic(err)
	} else {
		conn.MaxBatchSize = 1000

		if dataFilePath == BoltdbSnapshotDeltaDataFilePath {
			SnapshotDeltaDB = conn

			SnapshotDeltaDB.Update(func(tx *bolt.Tx) error {
				tx.CreateBucketIfNotExists([]byte(constant.BN_SNAPSHOTS_BY_CONV))
				tx.CreateBucketIfNotExists([]byte(constant.BN_USER_SNAPSHOTS_MAPS))
				return nil
			})
		} else {
			UserBucketsDB = conn

			UserBucketsDB.Update(func(tx *bolt.Tx) error {
				tx.CreateBucketIfNotExists([]byte(constant.BN_USER_DEVICES_BUCKETS_MAP))
				tx.CreateBucketIfNotExists([]byte(constant.BN_DEVICES))
				return nil
			})
		}
	}

}

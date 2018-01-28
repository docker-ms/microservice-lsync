package v1

import (
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"gopkg.in/mgo.v2/bson"

	"lsync/boltdb"
	"lsync/constant"
	"lsync/mongodb"

	pbMs "microservice"
	pbMsLsyncV1 "microservice/lsync/v1"
)

func QueryLastSeen(in *pbMsLsyncV1.QueryLastSeenReq) (*pbMsLsyncV1.QueryLastSeenRes, error) {
	var goToMongo []interface{}

	res := pbMsLsyncV1.QueryLastSeenRes{
		UsersLastSeen: make(map[string]int64),
	}

	boltdb.UserBucketsDB.View(func(tx *bolt.Tx) error {
		for _, userId := range in.TargetUserIds {
			if b := tx.Bucket([]byte(constant.BN_USER_DEVICES_BUCKETS_MAP)).Bucket([]byte(userId)); b != nil {
				c := b.Cursor()
				_, v := c.First()
				var udbm pbMsLsyncV1.UserDevicesBucketsMap
				if e := proto.Unmarshal(v, &udbm); e == nil {
					res.UsersLastSeen[userId] = udbm.LastSeen
				} else {
					return e
				}
			} else {
				goToMongo = append(goToMongo, bson.M{
					"userId":            userId,
					"confirmedContacts": in.QuerierUserId,
				})
			}
		}

		return nil
	})

	if len(goToMongo) > 0 {
		session := mongodb.ConnSessions.PickRandomly().Copy()
		defer session.Close()

		var lastSeenData []*pbMs.ConfirmedContact

		session.DB("").C(constant.CN_USERS).Find(&bson.M{
			"$or": goToMongo,
		}).Select(bson.M{
			"userId":   1,
			"lastSeen": 1,
		}).All(&lastSeenData)

		for _, item := range lastSeenData {
			res.UsersLastSeen[item.UserId] = item.LastSeen
		}
	}

	return &res, nil

}

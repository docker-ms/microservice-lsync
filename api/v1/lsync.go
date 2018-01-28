package v1

import (
	"bytes"
	"strconv"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"gopkg.in/mgo.v2/bson"

	"lsync/boltdb"
	"lsync/constant"
	"lsync/model"
	"lsync/mongodb"
	"lsync/util"

	pbMs "microservice"
	pbMsLsync "microservice/lsync"
	pbMsLsyncV1 "microservice/lsync/v1"
)

func Lsync(in *pbMsLsyncV1.LsyncReq, stream pbMsLsync.Lsync_LsyncV1Server) error {

	gap := int((util.SnapshotPoint - in.Version) / constant.ONE_DAY_IN_MILLISECONDS)

	switch {
	case gap == 0:
		// This device is up to date.
		// Server will return: the data in this device's bucket if has.
		chanBucketContents := make(chan *pbMsLsyncV1.DeviceBucketContent)
		go bucketQuery(in.DeviceId, chanBucketContents)

		for data := range chanBucketContents {
			res := pbMsLsyncV1.LsyncRes{
				LsyncDataPackType: pbMs.LsyncDataPackTypes_BUCKET,
				BucketContent:     data,
			}
			if e := stream.Send(&res); e != nil {
				return e
			}
		}

	case gap == 1:
		// There is inactive bucket for this device, so:
		//   - Activate the bucket.
		//   - Do the bucket query,
		//   - Do the complement query.
		var wg sync.WaitGroup
		wg.Add(1)
		go initOrActivateBucket4Device(&wg, in.UserId, in.DeviceId)

		chanComplementQuery := make(chan *pbMsLsyncV1.DeviceBucketContent)
		go complementQuery(in.UserId, chanComplementQuery)

		chanBucketQuery := make(chan *pbMsLsyncV1.DeviceBucketContent, 2)
		go bucketQuery(in.DeviceId, chanBucketQuery)

		for {
			select {
			case data, more := <-chanComplementQuery:
				if !more {
					chanComplementQuery = nil
				}

				if e := stream.Send(&pbMsLsyncV1.LsyncRes{
					NewVersion:        util.SnapshotPoint,
					LsyncDataPackType: pbMs.LsyncDataPackTypes_BUCKET,
					Complement:        data,
				}); e != nil {
					return e
				}
			case data, more := <-chanBucketQuery:
				if !more {
					chanBucketQuery = nil
				}

				if e := stream.Send(&pbMsLsyncV1.LsyncRes{
					NewVersion:        util.SnapshotPoint,
					LsyncDataPackType: pbMs.LsyncDataPackTypes_BUCKET,
					BucketContent:     data,
				}); e != nil {
					return e
				}
			}
			if chanComplementQuery == nil && chanBucketQuery == nil {
				break
			}
		}

		wg.Wait()

	case gap > 1 && gap <= constant.HISTORY_DELTA_COVER_DAYS:
		// Deltas cover this device, so:
		//   - Init bucket for this device.
		//   - Do the deltas query.
		//   - Do the complement query.
		var wg sync.WaitGroup
		wg.Add(1)
		go initOrActivateBucket4Device(&wg, in.UserId, in.DeviceId)

		chanComplementQuery := make(chan *pbMsLsyncV1.DeviceBucketContent)
		go complementQuery(in.UserId, chanComplementQuery)

		chanDeltaQuery := make(chan *pbMsLsyncV1.Delta)
		go deltaQuery(in.UserId, in.Version, chanDeltaQuery)

		data := <-chanComplementQuery

		if data != nil {
			if e := stream.Send(&pbMsLsyncV1.LsyncRes{
				NewVersion:        util.SnapshotPoint,
				LsyncDataPackType: pbMs.LsyncDataPackTypes_DELTA,
				Complement:        data,
			}); e != nil {
				return e
			}
		}

		for delta := range chanDeltaQuery {
			if data != nil && delta.Conversation != nil {
				if _, ok := data.Updated.UpdatesOnConversation[delta.Conversation.ConversationId].Users.Left[in.UserId]; ok {
					continue
				}
			}
			if data != nil && delta.Group != nil {
				if _, ok := data.Updated.UpdatesOnGroup[delta.Group.GroupId].Users.Left[in.UserId]; ok {
					continue
				}
			}
			if e := stream.Send(&pbMsLsyncV1.LsyncRes{
				LsyncDataPackType: pbMs.LsyncDataPackTypes_DELTA,
				Delta:             delta,
			}); e != nil {
				return e
			}
		}

	default:
		// No version found, or this device has not been online more than `constant.HISTORY_DELTA_COVER_DAYS` days.
		// Server will do:
		//   - Init bucket for this device.
		//   - Do the snapshot query.
		//   - Do the complement query.
		var wg sync.WaitGroup
		wg.Add(1)
		go initOrActivateBucket4Device(&wg, in.UserId, in.DeviceId)

		chanComplementQuery := make(chan *pbMsLsyncV1.DeviceBucketContent)
		go complementQuery(in.UserId, chanComplementQuery)

		chanSnapshotQuery := make(chan *pbMsLsyncV1.SnapshotByConv)
		go snapshotQuery(in.UserId, chanSnapshotQuery)

		data := <-chanComplementQuery

		if data != nil {
			if e := stream.Send(&pbMsLsyncV1.LsyncRes{
				NewVersion:        util.SnapshotPoint,
				LsyncDataPackType: pbMs.LsyncDataPackTypes_COMPLEMENT,
				Complement:        data,
			}); e != nil {
				return e
			}
		}

		for snapshot := range chanSnapshotQuery {

			if snapshot.Conversation.ConversationType == pbMs.ConversationTypes_GROUP {
				if data != nil && data.Updated.UpdatesOnGroup[snapshot.Group.GroupId] != nil {
					// If in the complement query find that this user already be kicked out from this group,
					// then don't send this snapshot.
					if _, ok := data.Updated.UpdatesOnGroup[snapshot.Group.GroupId].Users.Left[in.UserId]; ok {
						continue
					}
				}
			} else {
				if data != nil && data.Updated.UpdatesOnConversation[snapshot.Conversation.ConversationId] != nil {
					// If in the complement query find that this user already be kicked out from this conversation,
					// then don't send this snapshot.
					if _, ok := data.Updated.UpdatesOnConversation[snapshot.Conversation.ConversationId].Users.Left[in.UserId]; ok {
						continue
					}
				}
			}

			if e := stream.Send(&pbMsLsyncV1.LsyncRes{
				LsyncDataPackType: pbMs.LsyncDataPackTypes_SNAPSHOT,
				Snapshot:          snapshot,
			}); e != nil {
				return e
			}
		}

	}

	return nil

}

// Add non-exported stuffs below.

func initOrActivateBucket4Device(wg *sync.WaitGroup, userId, deviceId string) {
	defer wg.Done()

	boltdb.UserBucketsDB.Batch(func(tx *bolt.Tx) error {
		parentBucket := tx.Bucket([]byte(constant.BN_USER_DEVICES_BUCKETS_MAP))
		if b := parentBucket.Bucket([]byte(userId)); b == nil {
			b, _ := parentBucket.CreateBucket([]byte(userId))
			id, _ := b.NextSequence()
			udbm := pbMsLsyncV1.UserDevicesBucketsMap{
				DeviceId2DeviceBucketStatus: make(map[string]bool),
			}
			udbm.DeviceId2DeviceBucketStatus[deviceId] = true
			if buf, e := udbm.Marshal(); e == nil {
				if e0 := b.Put(boltdb.Uint64ToB(id), buf); e0 == nil {
					if _, e1 := tx.Bucket([]byte(constant.BN_DEVICES)).CreateBucket([]byte(deviceId)); e1 != nil {
						return e1
					}
				} else {
					return e0
				}
			} else {
				return e
			}
		} else {
			c := b.Cursor()
			k, v := c.First()
			var udbm pbMsLsyncV1.UserDevicesBucketsMap
			if e := proto.Unmarshal(v, &udbm); e == nil {
				val, ok := udbm.DeviceId2DeviceBucketStatus[deviceId]
				if !val {
					udbm.DeviceId2DeviceBucketStatus[deviceId] = true

					if buf, e0 := udbm.Marshal(); e0 == nil {
						if e1 := b.Put(k, buf); e1 == nil {
							if !ok {
								if _, e2 := tx.Bucket([]byte(constant.BN_DEVICES)).CreateBucket([]byte(deviceId)); e2 != nil {
									return e2
								}
							}
						} else {
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

		return nil
	})
}

func bucketQuery(bucketId string, ch chan<- *pbMsLsyncV1.DeviceBucketContent) {
	defer close(ch)

	boltdb.UserBucketsDB.Batch(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(constant.BN_DEVICES)).Bucket([]byte(bucketId)); b != nil {
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var dbc pbMsLsyncV1.DeviceBucketContent
				if e0 := proto.Unmarshal(v, &dbc); e0 == nil {
					ch <- &dbc
					if !dbc.IsRecordFrozen {
						dbc.IsRecordFrozen = true

						if buf, e1 := dbc.Marshal(); e1 == nil {
							if e2 := b.Put(k, buf); e2 != nil {
								return e2
							}
						} else {
							return e1
						}
					}
				} else {
					return e0
				}
			}
		} else {
			return grpc.Errorf(codes.Internal, constant.InvalidDeviceBucketId)
		}

		return nil
	})
}

func deltaQuery(userId string, version int64, ch chan *pbMsLsyncV1.Delta) {
	defer close(ch)

	boltdb.SnapshotDeltaDB.View(func(tx *bolt.Tx) error {
		buffer := bytes.NewBufferString(constant.BN_DELTAS_BUCKETS_PREFIX)
		buffer.WriteString(strconv.FormatInt(version, 10))
		b := tx.Bucket(buffer.Bytes())
		c := b.Cursor()
		for _, v := c.First(); v != nil; _, v = c.Next() {
			var delta pbMsLsyncV1.Delta
			if e := proto.Unmarshal(v, &delta); e == nil {
				if delta.Conversation != nil && delta.Conversation.Members != nil && delta.Conversation.Members[userId] != nil {
					ch <- &delta
					continue
				}
				if delta.Group != nil && delta.Group.Members != nil && delta.Group.Members[userId] != nil {
					ch <- &delta
				}
			} else {
				return e
			}
		}
		return nil
	})
}

func snapshotQuery(userId string, ch chan *pbMsLsyncV1.SnapshotByConv) {
	defer close(ch)

	boltdb.SnapshotDeltaDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(constant.BN_USER_SNAPSHOTS_MAPS))
		c := b.Cursor()
		for _, v := c.First(); v != nil; _, v = c.Next() {
			var userSnapshotsMap pbMsLsyncV1.UserSnapshotsMap
			if e0 := proto.Unmarshal(v, &userSnapshotsMap); e0 == nil {
				if userSnapshotsMap.UserId == userId {
					b := tx.Bucket([]byte(constant.BN_SNAPSHOTS_BY_CONV))
					for _, snapshotId := range userSnapshotsMap.SnapshotIds {
						var snapshotByConv pbMsLsyncV1.SnapshotByConv
						snapshot := b.Get(boltdb.Uint64ToB(snapshotId))
						if e1 := proto.Unmarshal(snapshot, &snapshotByConv); e1 == nil {
							ch <- &snapshotByConv
						} else {
							return e1
						}
					}
				}
			} else {
				return e0
			}
		}
		return nil
	})
}

// Query the missed ops for this user:
// from the snapshot/delta generation point to current.
func complementQuery(userId string, ch chan<- *pbMsLsyncV1.DeviceBucketContent) {
	defer close(ch)

	chanSelf := make(chan *pbMs.Self)
	go mongodb.QuerySelf(userId, chanSelf)

	res := &pbMsLsyncV1.DeviceBucketContent{
		New: &pbMsLsyncV1.ChangesOnNew{
			MemberUsers:         make(map[string]*pbMs.MemberUser),
			Groups:              make(map[string]*pbMs.Group),
			Conversations:       make(map[string]*pbMs.Conversation),
			Messages:            make(map[string]*pbMs.Message),
			MessageReadStatuses: make(map[string]*pbMs.MessageReadStatus),
		},
		Updated: &pbMsLsyncV1.ChangesOnUpdated{
			UpdatesOnGroup:        make(map[string]*pbMsLsyncV1.UpdatesOnGroupOrConversation),
			UpdatesOnConversation: make(map[string]*pbMsLsyncV1.UpdatesOnGroupOrConversation),
		},
		Deleted: &pbMsLsyncV1.ChangesOnDeleted{
			DeletedMessageIds: make(map[string]*pbMs.Empty),
		},
	}

	isResEmpty := true

	var (
		hasContacts bool

		convs         []*pbMs.Conversation
		convIds       []string
		groupIds      []string
		convMemberIds util.StrSlice

		tmpSharedUsers []*pbMs.MemberUser
	)

	chanContacts := make(chan []*pbMs.ConfirmedContact)

	self := <-chanSelf

	// Query this user's contacts' changes.
	if len(self.ConfirmedContacts) > 0 || len(self.UnconfirmedContacts) > 0 {
		hasContacts = true

		go mongodb.QueryContacts(&bson.M{
			"userId": bson.M{
				"$in": append(self.ConfirmedContacts, self.UnconfirmedContacts...),
			},
			"lastUpdate": bson.M{
				"$gte": util.SnapshotPoint,
			},
		}, chanContacts)
	} else {
		chanContacts = nil
	}

	if len(self.ActiveConversations) > 0 {

		mongodb.IterateConversations(&bson.M{
			"conversationId": bson.M{
				"$in": self.ActiveConversations,
			},
		}).All(&convs)

		userId2ConvIds := make(map[string][]string)
		newConvIds := make(map[string]*pbMs.Empty)

		for _, conv := range convs {
			convIds = append(convIds, conv.ConversationId)

			res.Updated.UpdatesOnConversation[conv.ConversationId] = &pbMsLsyncV1.UpdatesOnGroupOrConversation{
				Users: &pbMsLsyncV1.ChangesOnUser{
					Left:        make(map[string]*pbMs.Empty),
					NewJoinedIn: make(map[string]*pbMs.MemberUser),
					Updated:     make(map[string]*pbMs.MemberUser),
				},
			}

			if conv.CreateAt >= util.SnapshotPoint {
				res.New.Conversations[conv.ConversationId] = conv
				newConvIds[conv.ConversationId] = &pbMs.Empty{}

				isResEmpty = false
			} else if conv.LastUpdate >= util.SnapshotPoint {
				res.Updated.UpdatesOnConversation[conv.ConversationId].Conversation = conv

				isResEmpty = false
			}

			if conv.Members != nil {
				for memberUserId, memberDetails := range conv.Members {
					convMemberIds = append(convMemberIds, memberUserId)
					userId2ConvIds[memberUserId] = append(userId2ConvIds[memberUserId], conv.ConversationId)
					if memberDetails.LeaveAt != 0 {
						res.Updated.UpdatesOnConversation[conv.ConversationId].Users.Left[memberUserId] = &pbMs.Empty{}

						isResEmpty = false
					} else if memberDetails.JoinInAt >= util.SnapshotPoint {
						res.Updated.UpdatesOnConversation[conv.ConversationId].Users.NewJoinedIn[memberUserId] = nil

						isResEmpty = false
					}
				}
			} else {
				groupIds = append(groupIds, conv.ConversationId)
			}
		}

		var chanMemberUsers chan interface{}
		if len(convMemberIds) > 0 {
			chanMemberUsers = make(chan interface{})
			go mongodb.QueryMemberUsers(&bson.M{
				"userId": bson.M{
					"$in": convMemberIds,
				},
				"lastUpdate": bson.M{
					"$gte": util.SnapshotPoint,
				},
			}, chanMemberUsers)
		}

		var chanGroupsAndUsers chan interface{}
		if len(groupIds) > 0 {
			chanGroupsAndUsers = make(chan interface{})
			go mongodb.QueryGroupsAndUsers(&bson.M{
				"groupId": bson.M{
					"$in": groupIds,
				},
				"lastUpdate": bson.M{
					"$gte": util.SnapshotPoint,
				},
			}, convMemberIds, chanGroupsAndUsers)
		}

		chanMessages := make(chan interface{})
		go mongodb.QueryMessages(&bson.M{
			"toConversationId": bson.M{
				"$in": convIds,
			},
			"lastUpdate": bson.M{
				"$gte": util.SnapshotPoint,
			},
		}, chanMessages)

		chanMessageReadStatuses := make(chan interface{})
		go mongodb.QueryMessageReadStatuses(&bson.M{
			"toConversationId": bson.M{
				"$in": convIds,
			},
			"lastUpdate": bson.M{
				"$gte": util.SnapshotPoint,
			},
		}, chanMessageReadStatuses)

		for {
			select {
			case contacts, more := <-chanContacts:
				if !more {
					chanContacts = nil
				}
				if contacts != nil {
					changes := constructChangesOnContacts(self, <-chanContacts)
					if len(changes.NewCC) > 0 {
						res.New.ConfirmedContacts = changes.NewCC

						isResEmpty = false
					}
					if len(changes.UpdatedCC) > 0 {
						res.Updated.UpdatesOnConfirmedContacts = changes.UpdatedCC

						isResEmpty = false
					}
					if len(changes.NewUC) > 0 {
						res.New.UnconfirmedContacts = changes.NewUC

						isResEmpty = false
					}
					if len(changes.UpdatedUC) > 0 {
						res.Updated.UpdatesOnUnconfirmedContacts = changes.UpdatedUC

						isResEmpty = false
					}
				}
			case data, more := <-chanMemberUsers:
				if !more {
					chanMemberUsers = nil
				}
				if data != nil {
					users := data.([]*pbMs.MemberUser)

					tmpSharedUsers = users

					for _, user := range users {
						for _, convId := range userId2ConvIds[user.UserId] {
							if _, ok0 := newConvIds[convId]; ok0 {
								res.New.MemberUsers[user.UserId] = user

								isResEmpty = false
							} else {
								if _, ok1 := res.Updated.UpdatesOnConversation[convId].Users.Left[user.UserId]; !ok1 {
									if _, ok2 := res.Updated.UpdatesOnConversation[convId].Users.NewJoinedIn[user.UserId]; ok2 {
										res.Updated.UpdatesOnConversation[convId].Users.NewJoinedIn[user.UserId] = user
									} else {
										res.Updated.UpdatesOnConversation[convId].Users.Updated[user.UserId] = user
									}

									isResEmpty = false
								}
							}
						}
					}
				}
			case data, more := <-chanGroupsAndUsers:
				if !more {
					chanGroupsAndUsers = nil
				}
				if data != nil {
					groupsAndUsers := data.(*model.GroupsAndUsers)

					newGroupIds := make(map[string]*pbMs.Empty)

					for _, group := range groupsAndUsers.Groups {
						if group.CreateAt >= util.SnapshotPoint {
							res.New.Groups[group.GroupId] = group
							newGroupIds[group.GroupId] = &pbMs.Empty{}
						} else {
							res.Updated.UpdatesOnGroup[group.GroupId].Group = group

							for memberUserId, memberDetails := range group.Members {
								if memberDetails.LeaveAt != 0 {
									res.Updated.UpdatesOnGroup[group.GroupId].Users.Left[memberUserId] = &pbMs.Empty{}
								} else if memberDetails.JoinInAt >= util.SnapshotPoint {
									res.Updated.UpdatesOnGroup[group.GroupId].Users.NewJoinedIn[memberUserId] = nil
								}
							}
						}

						isResEmpty = false
					}

					for _, memberUser := range append(groupsAndUsers.MemberUsers, tmpSharedUsers...) {
						if groupsAndUsers.UserId2GroupIds[memberUser.UserId] != nil {
							for _, groupId := range groupsAndUsers.UserId2GroupIds[memberUser.UserId] {
								if _, ok0 := newGroupIds[groupId]; ok0 {
									res.New.MemberUsers[memberUser.UserId] = memberUser

									isResEmpty = false
								} else {
									if _, ok1 := res.Updated.UpdatesOnGroup[groupId].Users.Left[memberUser.UserId]; !ok1 {
										if _, ok2 := res.Updated.UpdatesOnGroup[groupId].Users.NewJoinedIn[memberUser.UserId]; ok2 {
											res.Updated.UpdatesOnGroup[groupId].Users.NewJoinedIn[memberUser.UserId] = memberUser
										} else {
											res.Updated.UpdatesOnGroup[groupId].Users.Updated[memberUser.UserId] = memberUser
										}

										isResEmpty = false
									}
								}
							}
							delete(groupsAndUsers.UserId2GroupIds, memberUser.UserId)
						}
					}

				}
			case data, more := <-chanMessages:
				if !more {
					chanMessages = nil
				}
				if data != nil {
					messages := data.([]*pbMs.Message)

					for _, message := range messages {
						if message.MessageStatus == pbMs.MessageStatuses_RECALLED {
							res.Deleted.DeletedMessageIds[message.MessageId] = &pbMs.Empty{}
						} else {
							res.New.Messages[message.MessageId] = message
						}

						isResEmpty = false
					}
				}
			case data, more := <-chanMessageReadStatuses:
				if !more {
					chanMessageReadStatuses = nil
				}
				if data != nil {
					messageReadStatuses := data.([]*pbMs.MessageReadStatus)

					for _, messageReadStatus := range messageReadStatuses {
						res.New.MessageReadStatuses[messageReadStatus.MessageId] = messageReadStatus

						isResEmpty = false
					}
				}
			}

			if chanContacts == nil && chanMemberUsers == nil && chanGroupsAndUsers == nil && chanMessages == nil && chanMessageReadStatuses == nil {
				break
			}
		}

	} else {
		if hasContacts {
			changes := constructChangesOnContacts(self, <-chanContacts)
			if len(changes.NewCC) > 0 {
				res.New.ConfirmedContacts = changes.NewCC

				isResEmpty = false
			}
			if len(changes.UpdatedCC) > 0 {
				res.Updated.UpdatesOnConfirmedContacts = changes.UpdatedCC

				isResEmpty = false
			}
			if len(changes.NewUC) > 0 {
				res.New.UnconfirmedContacts = changes.NewUC

				isResEmpty = false
			}
			if len(changes.UpdatedUC) > 0 {
				res.Updated.UpdatesOnUnconfirmedContacts = changes.UpdatedUC

				isResEmpty = false
			}
		}
	}

	if !isResEmpty {
		ch <- res
	}
}

type changesOnContacts struct {
	NewCC, UpdatedCC map[string]*pbMs.ConfirmedContact
	NewUC, UpdatedUC map[string]*pbMs.UnconfirmedContact
}

func constructChangesOnContacts(self *pbMs.Self, contacts []*pbMs.ConfirmedContact) *changesOnContacts {
	confirmedContacts := make(map[string]struct{})
	for _, userId := range self.ConfirmedContacts {
		confirmedContacts[userId] = struct{}{}
	}

	changes := changesOnContacts{
		NewCC:     make(map[string]*pbMs.ConfirmedContact),
		UpdatedCC: make(map[string]*pbMs.ConfirmedContact),
		NewUC:     make(map[string]*pbMs.UnconfirmedContact),
		UpdatedUC: make(map[string]*pbMs.UnconfirmedContact),
	}

	for _, contact := range contacts {
		if _, ok := confirmedContacts[contact.UserId]; ok {
			if contact.CreateAt >= util.SnapshotPoint {
				changes.NewCC[contact.UserId] = contact
			} else {
				changes.UpdatedCC[contact.UserId] = contact
			}
		} else {
			tmp := &pbMs.UnconfirmedContact{
				UserId:      contact.UserId,
				UserSetId:   contact.UserSetId,
				DisplayName: contact.DisplayName,
			}

			if contact.CreateAt >= util.SnapshotPoint {
				changes.NewUC[contact.UserId] = tmp
			} else {
				changes.UpdatedUC[contact.UserId] = tmp
			}
		}
	}

	return &changes
}

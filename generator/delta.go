package generator

import (
	"fmt"
	"sort"
	"sync"

	"gopkg.in/mgo.v2/bson"

	"lsync/boltdb"
	"lsync/constant"
	"lsync/model"
	"lsync/mongodb"
	"lsync/util"

	pbMs "microservice"
	pbMsLsyncV1 "microservice/lsync/v1"
)

func GenerateDelta(wg *sync.WaitGroup, chanData <-chan interface{}, info []*model.SnapshotGenGoroutinesGroup, idx int, baseId uint64) {
	defer wg.Done()

	deltas := make(map[int64]map[string]*pbMsLsyncV1.Delta)

	fromPoints := make([]int64, constant.HISTORY_DELTA_COVER_DAYS)
	// We don't need one day delta.
	for i, j := constant.HISTORY_DELTA_COVER_DAYS, 0; i > 1; i-- {
		fromPoints[j] = util.SnapshotPoint - int64(i)*constant.ONE_DAY_IN_MILLISECONDS
		j++
	}

	// Initialization.
	for _, fromPoint := range fromPoints {
		tmp := make(map[string]*pbMsLsyncV1.Delta)

		for n, conv := range info[idx].Conversations {
			tmp[conv.ConversationId] = &pbMsLsyncV1.Delta{
				Id: baseId + uint64(n),
				Users: &pbMsLsyncV1.ChangesOnUser{
					Left:                  make(map[string]*pbMs.Empty),
					NewJoinedIn:           make(map[string]*pbMs.MemberUser),
					Updated:               make(map[string]*pbMs.MemberUser),
					TmpNewJoinedInUserIds: make(map[string]*pbMs.Empty),
				},
				Messages:            make([]*pbMs.Message, 0),
				MessageReadStatuses: make([]*pbMs.MessageReadStatus, 0),
			}

			if conv.LastUpdate >= fromPoint {
				tmp[conv.ConversationId].Conversation = &conv
			}
		}

		deltas[fromPoint] = tmp
	}

	var convMemberLeftPairs []interface{}

	for _, conv := range info[idx].Conversations {
		if conv.Members != nil {
			for userId, memberDetail := range conv.Members {
				if memberDetail.LeaveAt != 0 {
					criticalP := sort.Search(constant.HISTORY_DELTA_COVER_DAYS, func(i int) bool {
						return fromPoints[i] > memberDetail.LeaveAt
					})
					if criticalP == constant.HISTORY_DELTA_COVER_DAYS {
						continue
					}
					for i := 0; i <= criticalP; i++ {
						deltas[fromPoints[i]][conv.ConversationId].Users.Left[userId] = &pbMs.Empty{}
					}

					convMemberLeftPairs = append(convMemberLeftPairs, bson.M{
						"ConversationId": conv.ConversationId,
					}, bson.M{
						"$unset": bson.M{
							fmt.Sprintf("members.%s", userId): "",
						},
					})
				} else {
					criticalP := sort.Search(constant.HISTORY_DELTA_COVER_DAYS, func(i int) bool {
						return fromPoints[i] > memberDetail.JoinInAt
					})
					if criticalP == constant.HISTORY_DELTA_COVER_DAYS {
						continue
					}
					for i := 0; i <= criticalP; i++ {
						deltas[fromPoints[i]][conv.ConversationId].Users.TmpNewJoinedInUserIds[userId] = &pbMs.Empty{}
					}
				}
			}
		}
	}

	if convMemberLeftPairs != nil {
		wg.Add(1)
		go mongodb.BulkUpdate(wg, constant.CN_CONVERSATIONS, convMemberLeftPairs...)
	}

	for {
		tmp, ok := <-chanData
		if !ok {
			break
		}
		switch data := tmp.(type) {
		case []*pbMs.Message:
			for _, message := range data {
				criticalP := sort.Search(constant.HISTORY_DELTA_COVER_DAYS, func(i int) bool {
					return fromPoints[i] > message.LastUpdate
				})
				if criticalP == constant.HISTORY_DELTA_COVER_DAYS {
					continue
				}
				for i := 0; i <= criticalP; i++ {
					deltas[fromPoints[i]][message.ToConversationId].Messages = append(
						deltas[fromPoints[i]][message.ToConversationId].Messages,
						message,
					)
				}
			}
		case []*pbMs.MessageReadStatus:
			for _, messageReadStatus := range data {
				criticalP := sort.Search(constant.HISTORY_DELTA_COVER_DAYS, func(i int) bool {
					return fromPoints[i] > messageReadStatus.LastUpdate
				})
				if criticalP == constant.HISTORY_DELTA_COVER_DAYS {
					continue
				}
				for i := 0; i <= criticalP; i++ {
					deltas[fromPoints[i]][messageReadStatus.ToConversationId].MessageReadStatuses = append(
						deltas[fromPoints[i]][messageReadStatus.ToConversationId].MessageReadStatuses,
						messageReadStatus,
					)
				}
			}
		case []*pbMs.MemberUser:
			for _, memberUser := range data {
				if fromPoints[0] > memberUser.LastUpdate {
					criticalP := sort.Search(constant.HISTORY_DELTA_COVER_DAYS, func(i int) bool {
						return fromPoints[i] > memberUser.LastUpdate
					})
					for i := 0; i <= criticalP; i++ {
						for _, convId := range info[idx].UserId2ConvIds[memberUser.UserId] {
							if deltas[fromPoints[i]][convId].Users.TmpNewJoinedInUserIds[memberUser.UserId] == nil {
								deltas[fromPoints[i]][convId].Users.Updated[memberUser.UserId] = memberUser
							} else {
								deltas[fromPoints[i]][convId].Users.NewJoinedIn[memberUser.UserId] = memberUser
							}
						}
					}
				}
			}
		case model.GroupsAndUsers:
			var groupMemberLeftPairs []interface{}

			for _, group := range data.Groups {
				criticalP := sort.Search(constant.HISTORY_DELTA_COVER_DAYS, func(i int) bool {
					return fromPoints[i] > group.LastUpdate
				})
				if criticalP == constant.HISTORY_DELTA_COVER_DAYS {
					continue
				}
				for i := 0; i <= criticalP; i++ {
					deltas[fromPoints[i]][group.GroupId].Group = group
				}
				for userId, memberDetail := range group.Members {
					if memberDetail.LeaveAt != 0 {
						criticalP := sort.Search(constant.HISTORY_DELTA_COVER_DAYS, func(i int) bool {
							return fromPoints[i] > memberDetail.LeaveAt
						})
						if criticalP == constant.HISTORY_DELTA_COVER_DAYS {
							continue
						}
						for i := 0; i <= criticalP; i++ {
							deltas[fromPoints[i]][group.GroupId].Users.Left[userId] = &pbMs.Empty{}
						}

						groupMemberLeftPairs = append(groupMemberLeftPairs, bson.M{
							"groupId": group.GroupId,
						}, bson.M{
							"$unset": bson.M{
								fmt.Sprintf("members.%s", userId): "",
							},
						})
					} else {
						criticalP := sort.Search(constant.HISTORY_DELTA_COVER_DAYS, func(i int) bool {
							return fromPoints[i] > memberDetail.JoinInAt
						})
						if criticalP == constant.HISTORY_DELTA_COVER_DAYS {
							continue
						}
						for i := 0; i <= criticalP; i++ {
							deltas[fromPoints[i]][group.GroupId].Users.TmpNewJoinedInUserIds[userId] = &pbMs.Empty{}
						}
					}
				}
			}

			if groupMemberLeftPairs != nil {
				wg.Add(1)
				go mongodb.BulkUpdate(wg, constant.CN_GROUPS, groupMemberLeftPairs...)
			}

			if data.MemberUsers != nil {
				for _, user := range data.MemberUsers {
					if fromPoints[0] > user.LastUpdate {
						criticalP := sort.Search(constant.HISTORY_DELTA_COVER_DAYS, func(i int) bool {
							return fromPoints[i] > user.LastUpdate
						})
						for i := 0; i <= criticalP; i++ {
							for _, convId := range info[idx].UserId2ConvIds[user.UserId] {
								if deltas[fromPoints[i]][convId].Users.TmpNewJoinedInUserIds[user.UserId] == nil {
									deltas[fromPoints[i]][convId].Users.Updated[user.UserId] = user
								} else {
									deltas[fromPoints[i]][convId].Users.NewJoinedIn[user.UserId] = user
								}
							}
						}
					}
				}
			}
		}
	}

	wg.Add(1)
	go boltdb.SnapshotDeltaBulkInsert(wg, constant.BN_DELTAS_BUCKETS_PREFIX, deltas)
}

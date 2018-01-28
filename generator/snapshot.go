package generator

import (
	"reflect"
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

func StartSnapshotGenGoroutinesGroup(wg *sync.WaitGroup, ch chan<- map[string]*pbMsLsyncV1.UserSnapshotsMap,
	info []*model.SnapshotGenGoroutinesGroup, idx int, baseId uint64) {

	defer wg.Done()

	chanData := make(chan interface{}, 5)
	wg.Add(1)
	go GenerateDelta(wg, chanData, info, idx, baseId)

	chans := make([]chan interface{}, 0)
	chanMessages := make(chan interface{})
	chanMessageReadStatuses := make(chan interface{})
	chanUsers := make(chan interface{})
	chanGroupsAndUsers := make(chan interface{})

	chans = append(chans, chanMessages)
	go mongodb.QueryMessages(&bson.M{
		"toConversationId": bson.M{
			"$in": info[idx].ConversationIds,
		},
		"messageStatus": bson.M{
			"$ne": pbMs.MessageStatuses_RECALLED,
		},
		"lastUpdate": bson.M{
			"$lt": util.SnapshotPoint,
		},
	}, chanMessages)

	chans = append(chans, chanMessageReadStatuses)
	go mongodb.QueryMessageReadStatuses(&bson.M{
		"toConversationId": bson.M{
			"$in": info[idx].ConversationIds,
		},
		"lastUpdate": bson.M{
			"$lt": util.SnapshotPoint,
		},
	}, chanMessageReadStatuses)

	var userIds util.StrSlice

	if info[idx].UserId2ConvIds != nil {
		for userId, _ := range info[idx].UserId2ConvIds {
			userIds = append(userIds, userId)
		}

		chans = append(chans, chanUsers)
		go mongodb.QueryMemberUsers(&bson.M{
			"userId": bson.M{
				"$in": userIds,
			},
			"userStatus": bson.M{
				"$ne": pbMs.UserStatuses_DELETED,
			},
			"lastUpdate": bson.M{
				"$lt": util.SnapshotPoint,
			},
		}, chanUsers)
	}

	if info[idx].GroupIds != nil {
		chans = append(chans, chanGroupsAndUsers)
		go mongodb.QueryGroupsAndUsers(&bson.M{
			"groupId": bson.M{
				"$in": info[idx].GroupIds,
			},
			"lastUpdate": bson.M{
				"$lt": util.SnapshotPoint,
			},
		}, userIds, chanGroupsAndUsers)
	}

	snapshots := make(map[string]*pbMsLsyncV1.SnapshotByConv)
	userSnapshotsMaps := make(map[string]*pbMsLsyncV1.UserSnapshotsMap)

	for i := 0; i < len(info[idx].ConversationIds); i++ {
		conv := info[idx].Conversations[i]

		if conv.ForGroupId == "" {
			for userId, _ := range conv.Members {
				if userSnapshotsMaps[userId] == nil {
					snapshotIdsMap := make(map[uint64]*pbMs.Empty)
					snapshotIdsMap[baseId+uint64(i)] = &pbMs.Empty{}
					userSnapshotsMaps[userId] = &pbMsLsyncV1.UserSnapshotsMap{
						SnapshotIdsMap: snapshotIdsMap,
					}
				} else {
					userSnapshotsMaps[userId].SnapshotIdsMap[baseId+uint64(i)] = &pbMs.Empty{}
				}
			}
		}

		snapshots[conv.ConversationId] = &pbMsLsyncV1.SnapshotByConv{
			Id:                  baseId + uint64(i),
			Conversation:        &conv,
			MemberUsers:         make([]*pbMs.MemberUser, 0),
			Messages:            make([]*pbMs.Message, 0),
			MessageReadStatuses: make([]*pbMs.MessageReadStatus, 0),
		}
	}

	tmpUsers := make(map[string]*pbMs.MemberUser)
	tmpUserId2GroupIds := make(map[string][]string)

	cases := make([]reflect.SelectCase, len(chans))
	for i, ch := range chans {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}

	remaining := len(cases)
	for remaining > 0 {
		chosen, value, ok := reflect.Select(cases)
		if !ok {
			// The chosen channel has been closed, so zero out the channel to disable the case.
			cases[chosen].Chan = reflect.ValueOf(nil)
			remaining--
			continue
		}

		switch data := value.Interface().(type) {
		case []*pbMs.Message:
			if len(data) > 0 {
				chanData <- data

				for _, message := range data {
					snapshots[message.ToConversationId].Messages = append(
						snapshots[message.ToConversationId].Messages,
						message,
					)
				}
			}
		case []*pbMs.MessageReadStatus:
			if len(data) > 0 {
				chanData <- data

				for _, messageReadStatus := range data {
					snapshots[messageReadStatus.ToConversationId].MessageReadStatuses = append(
						snapshots[messageReadStatus.ToConversationId].MessageReadStatuses,
						messageReadStatus,
					)
				}
			}
		case []*pbMs.MemberUser:
			if len(data) > 0 {
				chanData <- data

				for _, user := range data {
					tmpUsers[user.UserId] = user
					for _, convId := range info[idx].UserId2ConvIds[user.UserId] {
						snapshots[convId].MemberUsers = append(
							snapshots[convId].MemberUsers,
							user,
						)
					}
				}
			}
		case *model.GroupsAndUsers:
			chanData <- data

			for _, group := range data.Groups {
				snapshots[group.GroupId].Group = group

				for userId, _ := range group.Members {
					for _, groupId := range data.UserId2GroupIds[userId] {
						if userSnapshotsMaps[userId] == nil {
							snapshotIdsMap := make(map[uint64]*pbMs.Empty)
							snapshotIdsMap[snapshots[groupId].Id] = &pbMs.Empty{}
							userSnapshotsMaps[userId] = &pbMsLsyncV1.UserSnapshotsMap{
								SnapshotIdsMap: snapshotIdsMap,
							}
						} else {
							userSnapshotsMaps[userId].SnapshotIdsMap[snapshots[groupId].Id] = &pbMs.Empty{}
						}
					}
				}
			}

			if data.MemberUsers != nil {
				for _, user := range data.MemberUsers {
					for _, groupId := range data.UserId2GroupIds[user.UserId] {
						snapshots[groupId].MemberUsers = append(
							snapshots[groupId].MemberUsers,
							user,
						)
					}
					delete(data.UserId2GroupIds, user.UserId)
				}
			}
			tmpUserId2GroupIds = data.UserId2GroupIds
		}
	}

	close(chanData)

	for userId, groupIds := range tmpUserId2GroupIds {
		for _, groupId := range groupIds {
			snapshots[groupId].MemberUsers = append(
				snapshots[groupId].MemberUsers,
				tmpUsers[userId],
			)
		}
	}

	wg.Add(1)
	go boltdb.SnapshotDeltaBulkInsert(wg, constant.BN_SNAPSHOTS_BY_CONV, snapshots)

	ch <- userSnapshotsMaps
}

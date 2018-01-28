package mongodb

import (
	"sync"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"lsync/constant"
	"lsync/model"
	"lsync/util"

	pbMs "microservice"
)

func IterateConversations(query interface{}) *mgo.Iter {
	if iter := ConnSessions.PickRandomly().DB("").C(constant.CN_CONVERSATIONS).Find(query).Select(bson.M{
		"conversationId":     1,
		"conversationType":   1,
		"members":            1,
		"forGroupId":         1,
		"conversationStatus": 1,
		"lastUpdate":         1,
		"createAt":           1,
	}).Iter(); iter == nil {
		util.Logger.Panic("Error happened when tried to query conversations.")
		panic("Error happened when tried to query conversations.")
	} else {
		return iter
	}
}

func QueryMemberUsers(query interface{}, ch chan<- interface{}) {
	defer close(ch)

	session := ConnSessions.PickRandomly().Copy()
	defer session.Close()

	var memberUser []*pbMs.MemberUser

	session.DB("").C(constant.CN_USERS).Find(query).Select(bson.M{
		"userId":      1,
		"userSetId":   1,
		"displayName": 1,
		"gender":      1,
		"lastUpdate":  1,
		"createAt":    1,
	}).All(&memberUser)

	ch <- memberUser
}

func QueryGroupsAndUsers(query interface{}, alreadyQueriedUserIds util.StrSlice, ch chan<- interface{}) {
	defer close(ch)

	session := ConnSessions.PickRandomly().Copy()
	defer session.Close()

	var groups []*pbMs.Group
	userId2GroupIds := make(map[string][]string)

	session.DB("").C(constant.CN_GROUPS).Find(query).Select(bson.M{
		"groupId":        1,
		"groupName":      1,
		"creator":        1,
		"managers":       1,
		"members":        1,
		"blockedMembers": 1,
		"groupStatus":    1,
		"lastUpdate":     1,
		"createAt":       1,
	}).All(&groups)

	var userIds util.StrSlice
	for _, group := range groups {
		for userId := range group.Members {
			userId2GroupIds[userId] = append(
				userId2GroupIds[userId],
				group.GroupId,
			)
			if !alreadyQueriedUserIds.Contains(userId) {
				userIds = append(userIds, userId)
			}
		}
	}

	if len(userIds) > 0 {
		chanUsers := make(chan interface{})
		go QueryMemberUsers(&bson.M{
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

		users := <-chanUsers

		ch <- &model.GroupsAndUsers{
			Groups:          groups,
			MemberUsers:     users.([]*pbMs.MemberUser),
			UserId2GroupIds: userId2GroupIds,
		}
	} else {
		ch <- &model.GroupsAndUsers{
			Groups: groups,
		}
	}
}

func QueryMessages(query interface{}, ch chan<- interface{}) {
	defer close(ch)

	session := ConnSessions.PickRandomly().Copy()
	defer session.Close()

	var messages []*pbMs.Message

	session.DB("").C(constant.CN_MESSAGES).Find(query).Select(bson.M{
		"messageId":         1,
		"messageType":       1,
		"messageStatus":     1,
		"content":           1,
		"resources":         1,
		"sender":            1,
		"toConversationId":  1,
		"mentionedUsers":    1,
		"mentionedMessages": 1,
		"lastUpdate":        1,
		"createAt":          1,
	}).All(&messages)

	ch <- messages
}

func QueryMessageReadStatuses(query interface{}, ch chan<- interface{}) {
	defer close(ch)

	session := ConnSessions.PickRandomly().Copy()
	defer session.Close()

	var messageReadStatuses []*pbMs.MessageReadStatus

	session.DB("").C(constant.CN_MESSAGEREADSTATUSES).Find(query).Select(bson.M{
		"messageId":        1,
		"toConversationId": 1,
		"reader":           1,
		"when":             1,
		"lastUpdate":       1,
		"createAt":         1,
	}).All(&messageReadStatuses)

	ch <- messageReadStatuses
}

func QuerySelf(userId string, ch chan<- *pbMs.Self) {
	defer close(ch)

	session := ConnSessions.PickRandomly().Copy()
	defer session.Close()

	var self *pbMs.Self
	session.DB("").C(constant.CN_USERS).Find(&bson.M{
		"userId": userId,
	}).Select(&bson.M{
		"confirmedContacts":   1,
		"unconfirmedContacts": 1,
		"activeConversations": 1,
	}).One(&self)

	ch <- self
}

func QueryContacts(query interface{}, ch chan<- []*pbMs.ConfirmedContact) {
	defer close(ch)

	session := ConnSessions.PickRandomly().Copy()
	defer session.Close()

	var contacts []*pbMs.ConfirmedContact

	session.DB("").C(constant.CN_USERS).Find(query).Select(bson.M{
		"userId":      1,
		"userSetId":   1,
		"displayName": 1,
		"realName":    1,
		"gender":      1,
		"email":       1,
		"mobilePhone": 1,
		"lastUpdate":  1,
		"createAt":    1,
	}).All(&contacts)

	ch <- contacts
}

func BulkUpdate(wg *sync.WaitGroup, collection string, pairs ...interface{}) {
	defer wg.Done()

	session := ConnSessions.PickRandomly().Copy()
	defer session.Close()

	bulk := session.DB("").C(collection).Bulk()
	bulk.Update(pairs)

	if _, e := bulk.Run(); e != nil {
		panic(e)
	}
}

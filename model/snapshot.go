package model

import (
	pbMs "microservice"
)

type SnapshotGenGoroutinesGroup struct {
	Conversations   []pbMs.Conversation
	UserId2ConvIds  map[string][]string
	GroupIds        []string
	ConversationIds []string
}

type GroupsAndUsers struct {
	Groups          []*pbMs.Group
	MemberUsers     []*pbMs.MemberUser
	UserId2GroupIds map[string][]string
}

package v1

import (
	"lsync/boltdb"
	"lsync/constant"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	pbMs "microservice"
	pbMsLsyncV1 "microservice/lsync/v1"
)

func Cache(in *pbMsLsyncV1.DeviceBucketContent) (*pbMs.BoolRes, error) {

	// Note: use `recover()` to recover from the possible panic caused by `bolt.Batch`.
	if e0 := boltdb.UserBucketsDB.Batch(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(constant.BN_DEVICES)).Bucket([]byte(in.DeviceId)); b != nil {
			c := b.Cursor()
			_, v := c.First()
			var original pbMsLsyncV1.DeviceBucketContent
			if e1 := proto.Unmarshal(v, &original); e1 == nil {
				if original.UserId == in.UserId && original.DeviceId == in.DeviceId {
					if original.IsRecordFrozen {
						_, v = c.Next()
						if v == nil {
							// Insert as new record.
							id, _ := b.NextSequence()
							if buf, e2 := in.Marshal(); e2 == nil {
								if e3 := b.Put(boltdb.Uint64ToB(id), buf); e3 != nil {
									return e3
								}
							} else {
								return e2
							}
						} else {
							if e4 := proto.Unmarshal(v, &original); e4 == nil {
								merge(in, &original)
							} else {
								return e4
							}
						}
					} else {
						merge(in, &original)
					}
				} else {
					return grpc.Errorf(codes.Internal, constant.InvalidDeviceBucketId)
				}
			} else {
				return e1
			}
		} else {
			return grpc.Errorf(codes.Internal, constant.InvalidDeviceBucketId)
		}

		return nil
	}); e0 == nil {
		return &pbMs.BoolRes{
			Key: &pbMs.BoolRes_Success{
				Success: true,
			},
		}, nil
	} else {
		return nil, e0
	}

}

// Add non-exported stuffs below.

func merge(in, original *pbMsLsyncV1.DeviceBucketContent) {
	// Merge 'ChangesOnNew'.
	if in.New != nil {
		if original.New != nil {
			// Merge 'New.ConfirmedContacts'.
			if in.New.ConfirmedContacts != nil {
				if original.New.ConfirmedContacts != nil {
					for userId, confirmedContact := range in.New.ConfirmedContacts {
						original.New.ConfirmedContacts[userId] = confirmedContact
					}
				} else {
					original.New.ConfirmedContacts = in.New.ConfirmedContacts
				}
			}
			// Merge 'New.UnconfirmedContacts'.
			if in.New.UnconfirmedContacts != nil {
				if original.New.UnconfirmedContacts != nil {
					for userId, unconfirmedContact := range in.New.UnconfirmedContacts {
						original.New.UnconfirmedContacts[userId] = unconfirmedContact
					}
				} else {
					original.New.UnconfirmedContacts = in.New.UnconfirmedContacts
				}
			}
			// Merge 'New.MemberUsers'.
			if in.New.MemberUsers != nil {
				if original.New.MemberUsers != nil {
					for userId, user := range in.New.MemberUsers {
						original.New.MemberUsers[userId] = user
					}
				} else {
					original.New.MemberUsers = in.New.MemberUsers
				}
			}
			// Merge 'New.Groups'.
			if in.New.Groups != nil {
				if original.New.Groups != nil {
					for groupId, group := range in.New.Groups {
						original.New.Groups[groupId] = group
					}
				} else {
					original.New.Groups = in.New.Groups
				}
			}
			// Merge 'New.Conversations'.
			if in.New.Conversations != nil {
				if original.New.Conversations != nil {
					for convId, conv := range in.New.Conversations {
						original.New.Conversations[convId] = conv
					}
				} else {
					original.New.Conversations = in.New.Conversations
				}
			}
			// Merge 'New.Messages'.
			if in.New.Messages != nil {
				if original.New.Messages != nil {
					for messageId, message := range in.New.Messages {
						original.New.Messages[messageId] = message
					}
				} else {
					original.New.Messages = in.New.Messages
				}
			}
			// Merge 'New.MessageReadStatuses'.
			if in.New.MessageReadStatuses != nil {
				if original.New.MessageReadStatuses != nil {
					for messageId, messageReadStatus := range in.New.MessageReadStatuses {
						original.New.MessageReadStatuses[messageId] = messageReadStatus
					}
				} else {
					original.New.MessageReadStatuses = in.New.MessageReadStatuses
				}
			}
		} else {
			original.New = in.New
		}
	}

	// Merge 'ChangesOnUpdated'.
	if in.Updated != nil {
		// Merge 'UpdatesOnGroup'.
		if in.Updated.UpdatesOnGroup != nil {
			for groupId, updatesOnGroupOrConversation := range in.Updated.UpdatesOnGroup {
				// Update on 'ChangesOnNew'?
				if original.New != nil && original.New.Groups != nil && original.New.Groups[groupId] != nil {
					original.New.Groups[groupId] = updatesOnGroupOrConversation.Group
					if updatesOnGroupOrConversation.Users != nil {
						if updatesOnGroupOrConversation.Users.Left != nil {
						Loop0:
							for userId, _ := range updatesOnGroupOrConversation.Users.Left {
								if original.New.Groups != nil {
									for _, group := range original.New.Groups {
										if _, ok := group.Members[userId]; ok {
											break Loop0
										}
									}
								}
								if original.New.Conversations != nil {
									for _, conv := range original.New.Conversations {
										if _, ok := conv.Members[userId]; ok {
											break Loop0
										}
									}
								}
								delete(original.New.MemberUsers, userId)
							}
						}
						if updatesOnGroupOrConversation.Users.NewJoinedIn != nil {
							for userId, user := range updatesOnGroupOrConversation.Users.NewJoinedIn {
								original.New.MemberUsers[userId] = user
							}
						}
						if updatesOnGroupOrConversation.Users.Updated != nil {
							for userId, user := range updatesOnGroupOrConversation.Users.Updated {
								original.New.MemberUsers[userId] = user
							}
						}
					}
				} else if original.Updated != nil && original.Updated.UpdatesOnGroup != nil && original.Updated.UpdatesOnGroup[groupId] != nil {
					// Merge 'Updated.UpdatesOnGroup.Group'.
					if updatesOnGroupOrConversation.Group != nil {
						original.Updated.UpdatesOnGroup[groupId].Group = updatesOnGroupOrConversation.Group
					}
					// Merge 'Updated.UpdatesOnGroup.Users'.
					if updatesOnGroupOrConversation.Users != nil {
						if original.Updated.UpdatesOnGroup[groupId].Users != nil {
							// Merge 'Updated.UpdatesOnGroup.Users.Left'.
							if updatesOnGroupOrConversation.Users.Left != nil {
								if original.Updated.UpdatesOnGroup[groupId].Users.Left != nil {
									for userId, _ := range updatesOnGroupOrConversation.Users.Left {
										original.Updated.UpdatesOnGroup[groupId].Users.Left[userId] = &pbMs.Empty{}
									}
								} else {
									original.Updated.UpdatesOnGroup[groupId].Users.Left = updatesOnGroupOrConversation.Users.Left
								}
							}
							// Merge 'Updated.UpdatesOnGroup.Users.NewJoinedIn'.
							if updatesOnGroupOrConversation.Users.NewJoinedIn != nil {
								if original.Updated.UpdatesOnGroup[groupId].Users.NewJoinedIn != nil {
									for userId, user := range updatesOnGroupOrConversation.Users.NewJoinedIn {
										original.Updated.UpdatesOnGroup[groupId].Users.NewJoinedIn[userId] = user
									}
								} else {
									original.Updated.UpdatesOnGroup[groupId].Users.NewJoinedIn = updatesOnGroupOrConversation.Users.NewJoinedIn
								}
							}
							// Merge 'Updated.UpdatesOnGroup.Users.Updated'.
							if updatesOnGroupOrConversation.Users.Updated != nil {
								if original.Updated.UpdatesOnGroup[groupId].Users.Updated != nil {
									for userId, user := range updatesOnGroupOrConversation.Users.Updated {
										original.Updated.UpdatesOnGroup[groupId].Users.Updated[userId] = user
									}
								} else {
									original.Updated.UpdatesOnGroup[groupId].Users.Updated = updatesOnGroupOrConversation.Users.Updated
								}
							}
						} else {
							original.Updated.UpdatesOnGroup[groupId].Users = updatesOnGroupOrConversation.Users
						}
					}
				} else {
					if original.Updated != nil {
						if original.Updated.UpdatesOnGroup == nil {
							original.Updated.UpdatesOnGroup = make(map[string]*pbMsLsyncV1.UpdatesOnGroupOrConversation)
						}
						original.Updated.UpdatesOnGroup[groupId] = updatesOnGroupOrConversation
					} else {
						original.Updated = &pbMsLsyncV1.ChangesOnUpdated{
							UpdatesOnGroup: make(map[string]*pbMsLsyncV1.UpdatesOnGroupOrConversation),
						}
						original.Updated.UpdatesOnGroup[groupId] = updatesOnGroupOrConversation
					}
				}
			}
		}

		// Merge 'UpdatesOnConversation'.
		if in.Updated.UpdatesOnConversation != nil {
			for convId, updatesOnGroupOrConversation := range in.Updated.UpdatesOnConversation {
				// Update on 'ChangesOnNew'?
				if original.New != nil && original.New.Conversations != nil && original.New.Conversations[convId] != nil {
					original.New.Conversations[convId] = updatesOnGroupOrConversation.Conversation
					if updatesOnGroupOrConversation.Users != nil {
						if updatesOnGroupOrConversation.Users.Left != nil {
						Loop1:
							for userId, _ := range updatesOnGroupOrConversation.Users.Left {
								if original.New.Groups != nil {
									for _, group := range original.New.Groups {
										if _, ok := group.Members[userId]; ok {
											break Loop1
										}
									}
								}
								if original.New.Conversations != nil {
									for _, conv := range original.New.Conversations {
										if _, ok := conv.Members[userId]; ok {
											break Loop1
										}
									}
								}
								delete(original.New.MemberUsers, userId)
							}
						}
						if updatesOnGroupOrConversation.Users.NewJoinedIn != nil {
							for userId, user := range updatesOnGroupOrConversation.Users.NewJoinedIn {
								original.New.MemberUsers[userId] = user
							}
						}
						if updatesOnGroupOrConversation.Users.Updated != nil {
							for userId, user := range updatesOnGroupOrConversation.Users.Updated {
								original.New.MemberUsers[userId] = user
							}
						}
					}
				} else if original.Updated != nil && original.Updated.UpdatesOnConversation != nil && original.Updated.UpdatesOnConversation[convId] != nil {
					// Merge 'Updated.UpdatesOnConversation.Group'.
					if updatesOnGroupOrConversation.Conversation != nil {
						original.Updated.UpdatesOnConversation[convId].Conversation = updatesOnGroupOrConversation.Conversation
					}
					// Merge 'Updated.UpdatesOnConversation.Users'.
					if updatesOnGroupOrConversation.Users != nil {
						if original.Updated.UpdatesOnConversation[convId].Users != nil {
							// Merge 'Updated.UpdatesOnConversation.Users.Left'.
							if updatesOnGroupOrConversation.Users.Left != nil {
								if original.Updated.UpdatesOnConversation[convId].Users.Left != nil {
									for userId, _ := range updatesOnGroupOrConversation.Users.Left {
										original.Updated.UpdatesOnConversation[convId].Users.Left[userId] = &pbMs.Empty{}
									}
								} else {
									original.Updated.UpdatesOnConversation[convId].Users.Left = updatesOnGroupOrConversation.Users.Left
								}
							}
							// Merge 'Updated.UpdatesOnConversation.Users.NewJoinedIn'.
							if updatesOnGroupOrConversation.Users.NewJoinedIn != nil {
								if original.Updated.UpdatesOnConversation[convId].Users.NewJoinedIn != nil {
									for userId, user := range updatesOnGroupOrConversation.Users.NewJoinedIn {
										original.Updated.UpdatesOnConversation[convId].Users.NewJoinedIn[userId] = user
									}
								} else {
									original.Updated.UpdatesOnConversation[convId].Users.NewJoinedIn = updatesOnGroupOrConversation.Users.NewJoinedIn
								}
							}
							// Merge 'Updated.UpdatesOnConversation.Users.Updated'.
							if updatesOnGroupOrConversation.Users.Updated != nil {
								if original.Updated.UpdatesOnConversation[convId].Users.Updated != nil {
									for userId, user := range updatesOnGroupOrConversation.Users.Updated {
										original.Updated.UpdatesOnConversation[convId].Users.Updated[userId] = user
									}
								} else {
									original.Updated.UpdatesOnConversation[convId].Users.Updated = updatesOnGroupOrConversation.Users.Updated
								}
							}
						} else {
							original.Updated.UpdatesOnConversation[convId].Users = updatesOnGroupOrConversation.Users
						}
					}
				} else {
					if original.Updated != nil {
						if original.Updated.UpdatesOnConversation == nil {
							original.Updated.UpdatesOnConversation = make(map[string]*pbMsLsyncV1.UpdatesOnGroupOrConversation)
						}
						original.Updated.UpdatesOnConversation[convId] = updatesOnGroupOrConversation
					} else {
						original.Updated = &pbMsLsyncV1.ChangesOnUpdated{
							UpdatesOnConversation: make(map[string]*pbMsLsyncV1.UpdatesOnGroupOrConversation),
						}
						original.Updated.UpdatesOnConversation[convId] = updatesOnGroupOrConversation
					}
				}
			}
		}

		// Merge 'UpdatesOnConfirmedContacts'
		if in.Updated.UpdatesOnConfirmedContacts != nil {
			for userId, user := range in.Updated.UpdatesOnConfirmedContacts {
				if original.New != nil && original.New.ConfirmedContacts != nil && original.New.ConfirmedContacts[userId] != nil {
					original.New.ConfirmedContacts[userId] = user
				} else if original.Updated != nil && original.Updated.UpdatesOnConfirmedContacts != nil && original.Updated.UpdatesOnConfirmedContacts[userId] != nil {
					original.Updated.UpdatesOnConfirmedContacts[userId] = user
				} else {
					if original.Updated != nil {
						if original.Updated.UpdatesOnConfirmedContacts == nil {
							original.Updated.UpdatesOnConfirmedContacts = make(map[string]*pbMs.ConfirmedContact)
						}
						original.Updated.UpdatesOnConfirmedContacts[userId] = user
					} else {
						original.Updated = &pbMsLsyncV1.ChangesOnUpdated{
							UpdatesOnConfirmedContacts: make(map[string]*pbMs.ConfirmedContact),
						}
						original.Updated.UpdatesOnConfirmedContacts[userId] = user
					}
				}
			}
		}

		// Merge 'UpdatesOnUnconfirmedContacts'
		if in.Updated.UpdatesOnUnconfirmedContacts != nil {
			for userId, user := range in.Updated.UpdatesOnUnconfirmedContacts {
				if original.New != nil && original.New.UnconfirmedContacts != nil && original.New.UnconfirmedContacts[userId] != nil {
					original.New.UnconfirmedContacts[userId] = user
				} else if original.Updated != nil && original.Updated.UpdatesOnUnconfirmedContacts != nil && original.Updated.UpdatesOnUnconfirmedContacts[userId] != nil {
					original.Updated.UpdatesOnUnconfirmedContacts[userId] = user
				} else {
					if original.Updated != nil {
						if original.Updated.UpdatesOnUnconfirmedContacts == nil {
							original.Updated.UpdatesOnUnconfirmedContacts = make(map[string]*pbMs.UnconfirmedContact)
						}
						original.Updated.UpdatesOnUnconfirmedContacts[userId] = user
					} else {
						original.Updated = &pbMsLsyncV1.ChangesOnUpdated{
							UpdatesOnUnconfirmedContacts: make(map[string]*pbMs.UnconfirmedContact),
						}
						original.Updated.UpdatesOnUnconfirmedContacts[userId] = user
					}
				}
			}
		}
	}

	// Merge 'ChangesOnDeleted'.
	if in.Deleted != nil {
		// Merge 'Deleted.DeletedGroupIds'.
		if in.Deleted.DeletedGroupIds != nil {
			// Deletion on 'New'?
			if original.New != nil {
				if original.New.Groups != nil {
					for groupId, _ := range in.Deleted.DeletedGroupIds {
						if original.New.Groups[groupId] != nil {
						Loop2:
							for userId, _ := range original.New.Groups[groupId].Members {
								if original.New.Groups != nil {
									for gid, group := range original.New.Groups {
										if gid != groupId {
											if _, ok := group.Members[userId]; ok {
												break Loop2
											}
										}
									}
								}
								if original.New.Conversations != nil {
									for convId, conv := range original.New.Conversations {
										if convId != groupId {
											if _, ok := conv.Members[userId]; ok {
												break Loop2
											}
										}
									}
								}
								delete(original.New.MemberUsers, userId)
							}
						}
						delete(original.New.Groups, groupId)
						delete(original.New.Conversations, groupId)

						delete(in.Deleted.DeletedConversationIds, groupId)
						if len(in.Deleted.DeletedConversationIds) == 0 {
							in.Deleted.DeletedConversationIds = nil
						}
					}
				}
			}
			// Deletion on 'Updated'?
			if original.Updated != nil {
				if original.Updated.UpdatesOnGroup != nil {
					for groupId, _ := range in.Deleted.DeletedGroupIds {
						delete(original.Updated.UpdatesOnGroup, groupId)
					}
				}
			}
			if original.Deleted != nil && original.Deleted.DeletedGroupIds != nil {
				if original.Deleted.DeletedGroupIds != nil {
					for groupId, _ := range in.Deleted.DeletedGroupIds {
						original.Deleted.DeletedGroupIds[groupId] = &pbMs.Empty{}
					}
				} else {
					original.Deleted.DeletedGroupIds = in.Deleted.DeletedGroupIds
				}
			}
		}

		// Merge 'Deleted.DeletedConversationIds'.
		if in.Deleted.DeletedConversationIds != nil {
			// Deletion on 'New'?
			if original.New != nil {
				if original.New.Conversations != nil {
					for convId, _ := range in.Deleted.DeletedConversationIds {
						if original.New.Conversations[convId] != nil {
						Loop3:
							for userId, _ := range original.New.Conversations[convId].Members {
								if original.New.Groups != nil {
									for groupId, group := range original.New.Groups {
										if groupId != convId && group.Members[userId] != nil {
											break Loop3
										}
									}
								}
								if original.New.Conversations != nil {
									for cid, conv := range original.New.Conversations {
										if cid != convId && conv.Members[userId] != nil {
											break Loop3
										}
									}
								}
								delete(original.New.MemberUsers, userId)
							}
						}
						delete(original.New.Conversations, convId)
					}
				}
			}
			// Deletion on 'Updated'?
			if original.Updated != nil {
				if original.Updated.UpdatesOnConversation != nil {
					for groupId, _ := range in.Deleted.DeletedGroupIds {
						delete(original.Updated.UpdatesOnConversation, groupId)
					}
				}
			}
			if original.Deleted != nil && original.Deleted.DeletedConversationIds != nil {
				if original.Deleted.DeletedConversationIds != nil {
					for convId, _ := range in.Deleted.DeletedConversationIds {
						original.Deleted.DeletedConversationIds[convId] = &pbMs.Empty{}
					}
				} else {
					original.Deleted.DeletedConversationIds = in.Deleted.DeletedConversationIds
				}
			}
		}

		// Merge 'Deleted.DeletedConfirmedContactIds'
		if in.Deleted.DeletedConfirmedContactIds != nil {
			// Deletion on 'New'?
			if original.New != nil {
				if original.New.ConfirmedContacts != nil {
					for userId, _ := range in.Deleted.DeletedConfirmedContactIds {
						delete(original.New.ConfirmedContacts, userId)
					}
				}
			}
			// Deletion on 'Updated'?
			if original.Updated != nil {
				if original.Updated.UpdatesOnConfirmedContacts != nil {
					for userId, _ := range in.Deleted.DeletedConfirmedContactIds {
						delete(original.Updated.UpdatesOnConfirmedContacts, userId)
					}
				}
			}
			if original.Deleted != nil && original.Deleted.DeletedConfirmedContactIds != nil {
				if original.Deleted.DeletedConfirmedContactIds != nil {
					for userId, _ := range in.Deleted.DeletedConfirmedContactIds {
						original.Deleted.DeletedConfirmedContactIds[userId] = &pbMs.Empty{}
					}
				} else {
					original.Deleted.DeletedConfirmedContactIds = in.Deleted.DeletedConfirmedContactIds
				}
			}
		}

		// Merge 'Deleted.DeletedUnconfirmedContactIds'
		if in.Deleted.DeletedUnconfirmedContactIds != nil {
			// Deletion on 'New'?
			if original.New != nil {
				if original.New.UnconfirmedContacts != nil {
					for userId, _ := range in.Deleted.DeletedUnconfirmedContactIds {
						delete(original.New.UnconfirmedContacts, userId)
					}
				}
			}
			// Deletion on 'Updated'?
			if original.Updated != nil {
				if original.Updated.UpdatesOnUnconfirmedContacts != nil {
					for userId, _ := range in.Deleted.DeletedUnconfirmedContactIds {
						delete(original.Updated.UpdatesOnUnconfirmedContacts, userId)
					}
				}
			}
			if original.Deleted != nil && original.Deleted.DeletedUnconfirmedContactIds != nil {
				if original.Deleted.DeletedUnconfirmedContactIds != nil {
					for userId, _ := range in.Deleted.DeletedUnconfirmedContactIds {
						original.Deleted.DeletedUnconfirmedContactIds[userId] = &pbMs.Empty{}
					}
				} else {
					original.Deleted.DeletedUnconfirmedContactIds = in.Deleted.DeletedUnconfirmedContactIds
				}
			}
		}

		// Merge 'Deleted.DeletedMessageIds'
		if in.Deleted.DeletedMessageIds != nil {
			// Deletion on 'New'?
			if original.New != nil {
				if original.New.Messages != nil {
					for messageId, _ := range in.Deleted.DeletedMessageIds {
						delete(original.New.Messages, messageId)
					}
				}
				if original.New.MessageReadStatuses != nil {
					for messageId, _ := range in.Deleted.DeletedMessageIds {
						delete(original.New.MessageReadStatuses, messageId)
					}
				}
			}
			if original.Deleted != nil && original.Deleted.DeletedMessageIds != nil {
				if original.Deleted.DeletedMessageIds != nil {
					for messageId, _ := range in.Deleted.DeletedMessageIds {
						original.Deleted.DeletedMessageIds[messageId] = &pbMs.Empty{}
					}
				} else {
					original.Deleted.DeletedMessageIds = in.Deleted.DeletedMessageIds
				}
			}
		}
	}
}

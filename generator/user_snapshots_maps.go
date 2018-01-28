package generator

import (
	"sync"

	"lsync/boltdb"
	"lsync/constant"

	pbMs "microservice"
	pbMsLsyncV1 "microservice/lsync/v1"
)

func InsertUserSnapshotsMaps(wg *sync.WaitGroup, chanTotalBatches <-chan int, ch <-chan map[string]*pbMsLsyncV1.UserSnapshotsMap) {
	defer wg.Done()

	total, processed, userSnapshotsMaps := -1, 0, make(map[string]*pbMsLsyncV1.UserSnapshotsMap)

	for total != processed {
		select {
		case totalBatches, more := <-chanTotalBatches:
			if more {
				total = totalBatches
			}
		case data := <-ch:
			if len(userSnapshotsMaps) == 0 {
				userSnapshotsMaps = data
			} else {
				for userId, userSnapshotsMap := range data {
					if userSnapshotsMaps[userId] == nil {
						userSnapshotsMaps[userId] = userSnapshotsMap
					} else {
						for snapshotId, _ := range userSnapshotsMap.SnapshotIdsMap {
							userSnapshotsMaps[userId].SnapshotIdsMap[snapshotId] = &pbMs.Empty{}
						}
					}
				}
			}
			processed++
		}
	}

	for userId, userSnapshotsMap := range userSnapshotsMaps {
		idx, snapshotIds := 0, make([]uint64, len(userSnapshotsMap.SnapshotIdsMap))
		for snapshotId, _ := range userSnapshotsMap.SnapshotIdsMap {
			snapshotIds[idx] = snapshotId
			idx++
		}
		userSnapshotsMap.UserId = userId
		userSnapshotsMap.SnapshotIds = snapshotIds
	}

	wg.Add(1)
	go boltdb.SnapshotDeltaBulkInsert(wg, constant.BN_USER_SNAPSHOTS_MAPS, userSnapshotsMaps)
}

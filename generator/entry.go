package generator

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/shirou/gopsutil/mem"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/robfig/cron.v2"

	"lsync/boltdb"
	"lsync/model"
	"lsync/mongodb"
	"lsync/util"

	pbMs "microservice"
	pbMsLsyncV1 "microservice/lsync/v1"
)

var IS_SNAPSHOT_DELTA_GEN_IP bool = false

func SetUpCronJob() {
	// Run snapshot/delta generation process once immediately first.
	startGenerator()

	// Then set up cron job.
	c := cron.New()
	c.AddFunc("0 * * * * *", startGenerator)
	c.Start()
}

// Add non-exported stuffs below.

const (
	batchSize     int           = 1000
	safeMemLeft   uint64        = 30 * 1024 * 1024
	checkInterval time.Duration = time.Microsecond * 500
)

func startGenerator() {

	IS_SNAPSHOT_DELTA_GEN_IP = true
	fmt.Println("Start: ", util.GetEpochMilliseconds())

	defer func() {
		IS_SNAPSHOT_DELTA_GEN_IP = false
		fmt.Println("End: ", util.GetEpochMilliseconds())
	}()

	util.InitSnapshotPoint()

	if boltdb.SnapshotDeltaDB != nil {
		if e := boltdb.SnapshotDeltaDB.Close(); e != nil {
			util.Logger.Panic("Failed to close boltdb for snapshot and deltas.")
			panic(e)
		}
	}

	if _, e := os.Stat(boltdb.BoltdbSnapshotDeltaDataFilePath); !os.IsNotExist(e) {
		if e0 := os.Remove(boltdb.BoltdbSnapshotDeltaDataFilePath); e0 != nil {
			util.Logger.Panic("Failed to remove old boltdb data file for snapshot and deltas.")
			panic(e0)
		}
	}

	boltdb.InitBoltDB(boltdb.BoltdbSnapshotDeltaDataFilePath)

	var (
		wg                          sync.WaitGroup
		conv                        pbMs.Conversation
		snapshotgenGoroutinesGroups []*model.SnapshotGenGoroutinesGroup
	)

	wg.Add(1)
	go TransformDeviceBucketStatus(&wg)

	totalBatches, chanTotalBatches := 0, make(chan int)
	chanUserSnapshotsMaps := make(chan map[string]*pbMsLsyncV1.UserSnapshotsMap, 2)

	wg.Add(1)
	go InsertUserSnapshotsMaps(&wg, chanTotalBatches, chanUserSnapshotsMaps)

	snapshotgenGoroutinesGroups = append(snapshotgenGoroutinesGroups, &model.SnapshotGenGoroutinesGroup{
		Conversations:   make([]pbMs.Conversation, 0),
		UserId2ConvIds:  make(map[string][]string),
		GroupIds:        make([]string, 0),
		ConversationIds: make([]string, 0),
	})

	// Iterate over all conversations.
	convIter, baseId := mongodb.IterateConversations(&bson.M{
		"conversationStatus": bson.M{
			"$ne": pbMs.ConversationStatuses_DESTROYED,
		},
		"createAt": bson.M{
			"$lt": util.SnapshotPoint,
		},
	}), 1

	for convIter.Next(&conv) {

		// Dangerous: low memory, intervally check:
		//   - still low memory: sleep.
		//   - fine: contine.
		for memStats, _ := mem.VirtualMemory(); memStats.Available < safeMemLeft; memStats, _ = mem.VirtualMemory() {
			time.Sleep(checkInterval)
		}

		snapshotgenGoroutinesGroups[len(snapshotgenGoroutinesGroups)-1].Conversations = append(
			snapshotgenGoroutinesGroups[len(snapshotgenGoroutinesGroups)-1].Conversations,
			conv,
		)

		snapshotgenGoroutinesGroups[len(snapshotgenGoroutinesGroups)-1].ConversationIds = append(
			snapshotgenGoroutinesGroups[len(snapshotgenGoroutinesGroups)-1].ConversationIds,
			conv.ConversationId,
		)

		if conv.ForGroupId != "" {
			snapshotgenGoroutinesGroups[len(snapshotgenGoroutinesGroups)-1].GroupIds = append(
				snapshotgenGoroutinesGroups[len(snapshotgenGoroutinesGroups)-1].GroupIds,
				conv.ConversationId,
			)
		} else {
			for userId, _ := range conv.Members {
				snapshotgenGoroutinesGroups[len(snapshotgenGoroutinesGroups)-1].UserId2ConvIds[userId] = append(
					snapshotgenGoroutinesGroups[len(snapshotgenGoroutinesGroups)-1].UserId2ConvIds[userId],
					conv.ConversationId,
				)
			}
		}

		if len(snapshotgenGoroutinesGroups[len(snapshotgenGoroutinesGroups)-1].ConversationIds) == batchSize {

			wg.Add(1)
			go StartSnapshotGenGoroutinesGroup(&wg, chanUserSnapshotsMaps,
				snapshotgenGoroutinesGroups, len(snapshotgenGoroutinesGroups)-1, uint64(baseId))
			totalBatches++

			baseId += batchSize

			// Append an empty struct at end.
			snapshotgenGoroutinesGroups = append(snapshotgenGoroutinesGroups, &model.SnapshotGenGoroutinesGroup{
				Conversations:   make([]pbMs.Conversation, 0),
				UserId2ConvIds:  make(map[string][]string),
				GroupIds:        make([]string, 0),
				ConversationIds: make([]string, 0),
			})

		}

	}

	if len(snapshotgenGoroutinesGroups[len(snapshotgenGoroutinesGroups)-1].ConversationIds) > 0 {
		wg.Add(1)
		go StartSnapshotGenGoroutinesGroup(&wg, chanUserSnapshotsMaps,
			snapshotgenGoroutinesGroups, len(snapshotgenGoroutinesGroups)-1, uint64(baseId))
		totalBatches++
	}

	chanTotalBatches <- totalBatches
	close(chanTotalBatches)

	if err := convIter.Close(); err != nil {
		util.Logger.Warn("Failed to close iter when iterated over all conversations.")
	}

	wg.Wait()

}

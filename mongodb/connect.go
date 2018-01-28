package mongodb

import (
	"math/rand"
	"time"

	"gopkg.in/mgo.v2"

	"lsync/model"
	"lsync/util"
)

var ConnSessions mgoSessions

func Connect(config *model.MongoDbConfig) {

	ConnSessions = ConnSessions[:0]

	for _, seqs := range sudokuConsts[len(config.Hosts)] {
		tmp := make([]string, len(seqs))
		for idx, val := range seqs {
			tmp[idx] = config.Hosts[val]
		}
		dialInfo := mgo.DialInfo{
			Addrs:    tmp,
			Direct:   false,
			FailFast: false,
			Database: config.DbName,
		}

		session, err := mgo.DialWithInfo(&dialInfo)

		if err != nil {
			util.Logger.Panic("Failed to connect to MongoDB.")
			panic("Failed to connect to MongoDB.")
		}

		ConnSessions = append(ConnSessions, session)
	}

}

func (sessions mgoSessions) PickRandomly() *mgo.Session {
	rand.Seed(time.Now().Unix())
	return sessions[rand.Intn(len(sessions))]
}

// Add non-exported stuffs below.

type mgoSessions []*mgo.Session

var sudokuConsts = map[int][][]int{
	1: [][]int{
		{0},
	},
	2: [][]int{
		{0, 1},
		{1, 0},
	},
	3: [][]int{
		{0, 2, 1},
		{1, 0, 2},
		{2, 1, 0},
	},
	4: [][]int{
		{0, 1, 2, 3},
		{1, 0, 3, 2},
		{2, 3, 0, 1},
		{3, 2, 1, 0},
	},
	5: [][]int{
		{0, 4, 2, 1, 3},
		{1, 0, 3, 2, 4},
		{2, 3, 1, 4, 0},
		{3, 1, 4, 0, 2},
		{4, 2, 0, 3, 1},
	},
	6: [][]int{
		{0, 2, 3, 1, 4, 5},
		{1, 4, 5, 2, 3, 0},
		{2, 0, 4, 3, 5, 1},
		{3, 5, 2, 0, 1, 4},
		{4, 3, 1, 5, 0, 2},
		{5, 1, 0, 4, 2, 3},
	},
}

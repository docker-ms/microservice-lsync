package constant

const HISTORY_DELTA_COVER_DAYS int = 14

// All stuffs below are not tunable, I mean either from performance perspective or security perspective.

const MGO_MAX_BULK_PAIRS_SIZE int = 2000

const ONE_DAY_IN_MILLISECONDS int64 = 86400000

// Related MongoDB 'Collection Names'.
const (
	CN_USERS               string = "Users"
	CN_GROUPS              string = "Groups"
	CN_DEVICES             string = "Devices"
	CN_MESSAGES            string = "Messages"
	CN_CONVERSATIONS       string = "Conversations"
	CN_MESSAGEREADSTATUSES string = "MessageReadStatuses"
)

// Related BoltDB 'Bucket Names'.
const (
	BN_SNAPSHOTS_BY_CONV   string = "SnapshotsByConv"
	BN_USER_SNAPSHOTS_MAPS string = "UserSnapshotsMaps"

	BN_DELTAS_BUCKETS_PREFIX string = "Deltas-"

	BN_USER_DEVICES_BUCKETS_MAP string = "UserDevicesBucketsMap"
	BN_DEVICES                  string = "Devices"
)

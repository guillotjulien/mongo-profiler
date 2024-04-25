package collector

import (
	"context"
	"time"

	"github.com/guillotjulien/mongo-profiler/internal/constant"
	"github.com/guillotjulien/mongo-profiler/internal/logger"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SlowOpsRecord struct {
	Host           string    // This is provided by the conf
	Timestamp      time.Time `bson:"timestamp,omitempty"`
	OP             string    `bson:"op,omitempty"`
	Collection     string    `bson:"collection,omitempty"`
	User           string    `bson:"user,omitempty"`
	ResponseLength int       `bson:"responseLength,omitempty"`
	DurationMS     int       `bson:"durationMS,omitempty"`
	CursorID       int64     `bson:"cursorID,omitempty"` // Used to group by cursor
	KeysExamined   int       `bson:"keysExamined,omitempty"`
	DocExamined    int       `bson:"docsExamined,omitempty"`
	HasSortStage   bool      `bson:"hasSortStage,omitempty"`
	NReturned      int       `bson:"nreturned,omitempty"`
	NDeleted       int       `bson:"ndeleted,omitempty"`
	NInserted      int       `bson:"ninserted,omitempty"`
	NModified      int       `bson:"nmodified,omitempty"`
	QueryHash      string    `bson:"queryHash,omitempty"`   // Identify queries with the same shape (so that we can group and find examples)
	PlanHash       string    `bson:"planHash,omitempty"`    // Identify queries with the same plan (so that we can find all queries using specific index or all non-indexed queries)
	PlanSummary    string    `bson:"planSummary,omitempty"` // Make plan hash more readable by storing the summary
}

// FIXME: In case the query is empty, there is no queryHash or planCacheKey. Example distinct key on all collection
// Should we generate a hash ourself in this case? e.g. ns + op + _EMPTY?
// Java does label + db + col + op + fields + sort + projection

func InitSlowOpsRecordCollection(ctx context.Context, db *mongo.Database) error {
	if err := db.CreateCollection(ctx, constant.PROFILER_SLOWOPS_COLLECTION); err != nil {
		if e, ok := err.(mongo.ServerError); ok {
			if !e.HasErrorCode(constant.MONGO_COLLECTION_EXISTS_ERROR) {
				return err
			}
		} else {
			return err
		}
	}

	collection := db.Collection(constant.PROFILER_SLOWOPS_COLLECTION)

	options := options.Index()
	options.SetExpireAfterSeconds(constant.PROFILER_SLOWOPS_EXPIRE_SECONDS) // 3 months - could make it a knob later

	// Add indexes to the collection
	_, err := collection.Indexes().CreateMany(
		ctx,
		[]mongo.IndexModel{
			{
				Keys:    bson.M{"timestamp": 1},
				Options: options,
			},
			{
				Keys: bson.M{"planHash": 1},
			},
			{
				Keys: bson.M{"queryHash": 1},
			},
		},
	)
	if err != nil {
		if e, ok := err.(mongo.ServerError); ok {
			if !e.HasErrorCode(constant.MONGO_INDEX_EXISTS_ERROR) {
				return err
			}
		} else {
			return err
		}
	}

	return nil
}

func (r *SlowOpsRecord) TryInsert(ctx context.Context, db *mongo.Database) {
	if _, err := db.Collection(constant.PROFILER_SLOWOPS_COLLECTION).InsertOne(ctx, r); err != nil {
		logger.Warn("failed to insert slow ops record %+v: %v", r, err) // Simply add as an error, but we don't really care. We could react if we see that the amount is too high
	}
}

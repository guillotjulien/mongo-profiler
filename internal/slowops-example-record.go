package internal

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SlowOpsExampleRecord struct {
	QueryHash   string   `bson:"queryHash"` // Should be unique
	PlanHash    string   `bson:"planHash"`
	PlanSummary string   `bson:"planSummary"` // Summary of used plan (can group queries by plan used)
	Document    bson.Raw `bson:"document"`
}

func InitSlowOpsExampleRecordCollection(ctx context.Context, db *mongo.Database) error {
	if err := db.CreateCollection(ctx, SLOWOPS_EXAMPLE_COLLECTION); err != nil {
		if e, ok := err.(mongo.ServerError); ok {
			if !e.HasErrorCode(COLLECTION_EXISTS_ERROR) {
				return err
			}
		}
	}

	collection := db.Collection(SLOWOPS_EXAMPLE_COLLECTION)

	planHashOptions := options.Index()
	planHashOptions.SetExpireAfterSeconds(SLOWOPS_EXPIRE_SECONDS)

	queryHashOptions := options.Index()
	queryHashOptions.SetUnique(true)

	// Add indexes to the collection
	_, err := collection.Indexes().CreateMany(
		ctx,
		[]mongo.IndexModel{
			{
				Keys:    bson.M{"planHash": 1},
				Options: planHashOptions,
			},
			{
				Keys:    bson.M{"queryHash": 1},
				Options: queryHashOptions,
			},
		},
	)
	if err != nil {
		if e, ok := err.(mongo.ServerError); ok {
			if !e.HasErrorCode(INDEX_EXISTS_ERROR) {
				return err
			}
		}
	}

	return nil
}

func (r *SlowOpsExampleRecord) TryInsert(ctx context.Context, db *mongo.Database) {
	if _, err := db.Collection(SLOWOPS_EXAMPLE_COLLECTION).InsertOne(ctx, r); err != nil {
		if e, ok := err.(mongo.ServerError); ok {
			if e.HasErrorCode(DUPLICATE_DOCUMENT_ERROR) {
				return
			}
		}

		Error("failed to insert slow ops example record %+v: %v", r, err) // Simply add as an error, but we don't really care. We could react if we see that the amount is too high
	}
}

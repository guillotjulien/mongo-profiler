package mongo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

type MongoWriter struct {
	Client     *Client
	Collection string
	// No choice if we want to implement io.Writer properly
	Ctx context.Context
}

func (w MongoWriter) Write(p []byte) (n int, err error) {
	var data interface{}
	if err := bson.Unmarshal(p, &data); err != nil {
		return 0, fmt.Errorf("cannot write data: %w", err)
	}

	db := w.Client.GetDefaultDatabase()
	collection := db.Collection(w.Collection)

	switch v := data.(type) {
	case bson.M:
		if _, err = collection.InsertOne(w.Ctx, v); err != nil {
			return 0, fmt.Errorf("cannot write data: %w", err)
		}
	case bson.A:
		if _, err = collection.InsertMany(w.Ctx, v); err != nil {
			return 0, fmt.Errorf("cannot write data: %w", err)
		}
	default:
		return 0, errors.New("cannot write data: invalid data type")
	}

	return len(p), nil
}

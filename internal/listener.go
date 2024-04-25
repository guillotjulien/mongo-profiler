package internal

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Listener struct {
	client *MongoClient

	lastTimestamp    time.Time
	stopChangeStream bool
}

func NewListener(client *MongoClient) *Listener {
	l := &Listener{}
	l.client = client

	return l
}

func (l *Listener) Start(ctx context.Context, handler func(ctx context.Context, data bson.Raw) error) error {
	db := l.client.C.Database(l.client.Connstr.Database)

	// Stop profiler
	res := db.RunCommand(ctx, bson.M{
		"profile": 0,
	})

	if res.Err() != nil {
		return fmt.Errorf("failed to stop profiler for Mongo host %s (database: %s): %w", l.client.Connstr.Hosts, l.client.Connstr.Database, res.Err())
	}

	// Clean current profiling collection to make sure it is capped
	err := db.Collection(SYSTEM_PROFILE).Drop(ctx)
	if err != nil {
		return fmt.Errorf("failed to drop %s for Mongo host %s (database: %s): %w", SYSTEM_PROFILE, l.client.Connstr.Hosts, l.client.Connstr.Database, err)
	}

	createCollectionOptions := options.CreateCollectionOptions{}
	createCollectionOptions.SetCapped(true)
	createCollectionOptions.SetSizeInBytes(SYSTEM_PROFILE_MAX_SIZE) // FIXME: what if we have so much ops that we are not fast enough to consume messages? Or if we have huge documents? How is the java one handling that?

	if err := db.CreateCollection(ctx, SYSTEM_PROFILE, &createCollectionOptions); err != nil {
		if e, ok := err.(mongo.ServerError); ok {
			if !e.HasErrorCode(COLLECTION_EXISTS_ERROR) {
				return fmt.Errorf("failed to create %s for Mongo host %s (database: %s): %w", SYSTEM_PROFILE, l.client.Connstr.Hosts, l.client.Connstr.Database, err)
			}
		}
	}

	// turn on profiler
	res = db.RunCommand(ctx, bson.D{
		{Key: "profile", Value: 1},
		{Key: "slowms", Value: QUERY_SLOW_MS},
	})

	if res.Err() != nil {
		return fmt.Errorf("failed to start profiler for Mongo host %v (database: %s): %w", l.client.Connstr.Hosts, l.client.Connstr.Database, res.Err())
	}

	// start change stream
	collection := db.Collection(SYSTEM_PROFILE)

	// No way to open a change stream against a system collection so use a tailable cursor instead (https://www.mongodb.com/community/forums/t/why-change-streams-cannot-be-used-with-local-database/3063)
	var cursor *mongo.Cursor
	cursorOptions := options.FindOptions{}
	cursorOptions.SetCursorType(options.Tailable)
	cursorOptions.SetSort(bson.M{"$natural": 1})

	Info("starting change stream against %s for Mongo host %v (database: %s)", SYSTEM_PROFILE, l.client.Connstr.Hosts, l.client.Connstr.Database)

	for !l.stopChangeStream {
		// According to MongoDB, the driver handle reconnection in case of network error

		if ctx.Err() != nil {
			Warn("change stream cursor error against %s for Mongo host %v (database %s): %v", SYSTEM_PROFILE, l.client.Connstr.Hosts, l.client.Connstr.Database, ctx.Err())
		}

		if cursor == nil || cursor.ID() == 0 || ctx.Err() != nil { // Cursor was closed - create a new cursor (actually fine since this is a very small capped collection)
			Info("change stream cursor closed against %s for Mongo host %v (database: %s) Will retry after %s", SYSTEM_PROFILE, l.client.Connstr.Hosts, l.client.Connstr.Database, RETRY_AFTER.String())
			time.Sleep(RETRY_AFTER)

			cursorQuery := bson.M{
				"ns": bson.M{
					"$regex": fmt.Sprintf("^%s.", l.client.Connstr.Database),                  // only the database in our conf
					"$ne":    fmt.Sprintf("%s.%s", l.client.Connstr.Database, SYSTEM_PROFILE), // all collections except system.profile
				},
				"ts": bson.M{
					"$gt": l.lastTimestamp,
				},
			}

			cursor, err = collection.Find(ctx, cursorQuery, &cursorOptions)
			if err != nil {
				Error("failed to obtain cursor for mongo host %v (database: %s): %v", l.client.Connstr.Hosts, l.client.Connstr.Database, err)
			}
		}

		if hasNext := cursor.TryNext(ctx); !hasNext {
			continue
		}

		// TODO: decode and store last timestamp. PB: Overhead of decoding: Java does that in the thread pool, but is that safe (e.g. race condition)??
		// Worse case we just have a few duplicates so not the end of the world.
		// Or unique constraint on lsid.id?

		go handler(ctx, cursor.Current) // The result insn't important. We can miss a few without any issue
	}
	return nil
}

func (l *Listener) Stop(ctx context.Context) error {
	// 1. stop change stream
	l.stopChangeStream = true

	Info("attempting to stop profiler for mongo host %v (database: %s)", l.client.Connstr.Hosts, l.client.Connstr.Database)

	// 2. stop profiler
	res := l.client.C.Database(l.client.Connstr.Database).RunCommand(ctx, bson.M{
		"profile": 0,
	})

	if res.Err() != nil {
		return fmt.Errorf("failed to stop profiler for Mongo host %v (database: %s): %w", l.client.Connstr.Hosts, l.client.Connstr.Database, res.Err())
	}

	Info("successfully stopped profiler for mongo host %v (database: %s)", l.client.Connstr.Hosts, l.client.Connstr.Database)

	// 3. close connection with source store
	if err := l.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to close connection with Mongo host %v (database: %s): %w", l.client.Connstr.Hosts, l.client.Connstr.Database, err)
	}

	return nil
}

package collector

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/guillotjulien/mongo-profiler/internal/constant"
	"github.com/guillotjulien/mongo-profiler/internal/logger"
	mgo "github.com/guillotjulien/mongo-profiler/internal/mongo"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Collector struct {
	client          *mgo.Client
	slowThresholdMS uint64
	profilerLevel   uint

	lastTimestamp            time.Time
	stopChangeStream         bool
	currentSystemProfileSize int64
}

func NewCollector(client *mgo.Client, slowThresholdMS uint64, profilerLevel uint) *Collector {
	c := &Collector{}
	c.client = client
	c.slowThresholdMS = slowThresholdMS
	c.profilerLevel = profilerLevel

	return c
}

func (c *Collector) Start(ctx context.Context, handler func(ctx context.Context, data bson.Raw) error) error {
	if err := c.client.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to Mongo host %s (database: %s): %w", c.client.Connstr.Hosts, c.client.Connstr.Database, err)
	}

	if err := c.increaseSystemProfileSize(ctx); err != nil {
		return fmt.Errorf("failed to initialize collector: %w", err)
	}

	db := c.client.GetDefaultDatabase()

	// start change stream
	collection := db.Collection(constant.PROFILER_SYSTEM_PROFILE)

	// No way to open a change stream against a system collection so use a tailable cursor instead (https://www.mongodb.com/community/forums/t/why-change-streams-cannot-be-used-with-local-database/3063)
	var cursor *mongo.Cursor
	cursorOptions := options.FindOptions{}
	cursorOptions.SetCursorType(options.Tailable)
	cursorOptions.SetSort(bson.M{"$natural": 1})

	logger.Info("starting change stream against %s", constant.PROFILER_SYSTEM_PROFILE)

	for !c.stopChangeStream {
		if ctx.Err() != nil {
			logger.Warn("change stream cursor error %w", ctx.Err())
		}

		if cursor != nil && cursor.Err() != nil {
			logger.Warn("change stream cursor error %w", cursor.Err())

			if e, ok := cursor.Err().(mongo.ServerError); ok {
				if e.HasErrorCode(constant.MONGO_CAPPED_POSITION_LOST_ERROR) {
					logger.Info("attempting to resize %s", constant.PROFILER_SYSTEM_PROFILE)
					if err := c.increaseSystemProfileSize(ctx); err != nil {
						logger.Fatal("failed to resize %s: %w", constant.PROFILER_SYSTEM_PROFILE, err)
					}
					logger.Info("resized %s to %v bytes", constant.PROFILER_SYSTEM_PROFILE, c.currentSystemProfileSize)
				}
			} else if e, ok := cursor.Err().(mongo.CommandError); ok {
				log.Fatal(e.Code) // FIXME: (mongo.CommandError)
			} else {
				log.Fatal(cursor.Err())
				// TODO: Crash for now
			}
		}

		if cursor == nil || cursor.ID() == 0 || ctx.Err() != nil || cursor.Err() != nil { // Cursor was closed - create a new cursor (actually fine since this is a very small capped collection)
			logger.Info("change stream cursor closed for %s. Will retry after %s", constant.PROFILER_SYSTEM_PROFILE, constant.RETRY_AFTER.String())
			select { // make sure we can cancel the wait and close fast
			case <-ctx.Done():
			case <-time.After(constant.RETRY_AFTER):
			}

			if c.stopChangeStream { // Make sure we quit when we were sleeping and we suddenly stop the change stream
				break
			}

			cursorQuery := bson.M{
				"ns": bson.M{
					"$regex": fmt.Sprintf("^%s\\.", c.client.Connstr.Database),                                  // only the database in our conf
					"$ne":    fmt.Sprintf("%s.%s", c.client.Connstr.Database, constant.PROFILER_SYSTEM_PROFILE), // all collections except system.profile
				},
				"ts": bson.M{
					"$gt": c.lastTimestamp,
				},
			}

			var err error
			cursor, err = collection.Find(ctx, cursorQuery, &cursorOptions)
			if err != nil {
				logger.Error("failed to obtain cursor for %s: %v", constant.PROFILER_SYSTEM_PROFILE, err)
			}
		}

		if cursor == nil {
			continue
		}

		if hasNext := cursor.TryNext(ctx); !hasNext {
			continue
		}

		// TODO: decode and store last timestamp. PB: Overhead of decoding: Java does that in the thread pool, but is that safe (e.g. race condition)??
		// Worse case we just have a few duplicates so not the end of the world.
		// Or unique constraint on lsid.id? -> Doesn't work, not all ops have this...

		go handler(ctx, cursor.Current) // The result isn't important. We can miss a few without any issue
	}
	return nil
}

func (c *Collector) Stop(ctx context.Context) error {
	// 1. stop change stream
	c.stopChangeStream = true

	logger.Info("attempting to stop collector for mongo host %v (database: %s)", c.client.Connstr.Hosts, c.client.Connstr.Database)

	// 2. stop profiler
	res := c.client.GetDefaultDatabase().RunCommand(ctx, bson.M{
		"profile": 0,
	})

	if res.Err() != nil {
		return fmt.Errorf("failed to stop profiler for Mongo host %v (database: %s): %w", c.client.Connstr.Hosts, c.client.Connstr.Database, res.Err())
	}

	logger.Info("successfully stopped collector for mongo host %v (database: %s)", c.client.Connstr.Hosts, c.client.Connstr.Database)

	// 3. close connection with source store
	if err := c.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to close connection with Mongo host %v (database: %s): %w", c.client.Connstr.Hosts, c.client.Connstr.Database, err)
	}

	return nil
}

func (c *Collector) increaseSystemProfileSize(ctx context.Context) error {
	db := c.client.GetDefaultDatabase()

	// Stop profiler - no problem if it fails
	db.RunCommand(ctx, bson.M{
		"profile": 0,
	})

	// Clean current profiling collection to make sure it is capped
	err := db.Collection(constant.PROFILER_SYSTEM_PROFILE).Drop(ctx)
	if err != nil {
		// TODO: Depends on the error. If the collection is already here, we don't care
		return fmt.Errorf("failed to drop collection %s: %w", constant.PROFILER_SYSTEM_PROFILE, err)
	}

	c.currentSystemProfileSize += constant.PROFILER_SYSTEM_PROFILE_CAPPED_INCREMENT

	createCollectionOptions := options.CreateCollectionOptions{}
	createCollectionOptions.SetCapped(true)
	createCollectionOptions.SetSizeInBytes(c.currentSystemProfileSize)

	if err := db.CreateCollection(ctx, constant.PROFILER_SYSTEM_PROFILE, &createCollectionOptions); err != nil {
		if e, ok := err.(mongo.ServerError); ok {
			if !e.HasErrorCode(constant.MONGO_COLLECTION_EXISTS_ERROR) {
				return fmt.Errorf("failed to create collection %s: %w", constant.PROFILER_SYSTEM_PROFILE, err)
			}
		} else {
			return fmt.Errorf("failed to create collection %s: %w", constant.PROFILER_SYSTEM_PROFILE, err)
		}
	}

	// turn on profiler
	logger.Info("Setting profiler to level %v", c.profilerLevel)
	logger.Info("Slow query threshold %vms", c.slowThresholdMS)

	res := db.RunCommand(ctx, bson.D{
		{Key: "profile", Value: c.profilerLevel},
		{Key: "slowms", Value: c.slowThresholdMS},
	})

	if res.Err() != nil {
		return fmt.Errorf("failed to start collector: %w", res.Err())
	}

	return nil
}

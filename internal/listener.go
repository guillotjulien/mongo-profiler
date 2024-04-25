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
	conf   Config
	client *mongo.Client

	lastTimestamp    time.Time
	stopChangeStream bool
}

func NewListener(conf Config) *Listener {
	l := &Listener{}
	l.conf = conf

	return l
}

func (l *Listener) Start(ctx context.Context, handler func(ctx context.Context, data bson.Raw) error) error {
	if err := l.connect(ctx); err != nil {
		return err
	}

	db := l.client.Database(l.conf.Database)

	// Stop profiler
	res := db.RunCommand(ctx, bson.M{
		"profile": 0,
	})

	if res.Err() != nil {
		return fmt.Errorf("failed to stop profiler for Mongo host %s (database: %s): %w", l.conf.Host, l.conf.Database, res.Err())
	}

	// Clean current profiling collection to make sure it is capped
	err := db.Collection(SYSTEM_PROFILE).Drop(ctx)
	if err != nil {
		return fmt.Errorf("failed to drop %s for Mongo host %s (database: %s): %w", SYSTEM_PROFILE, l.conf.Host, l.conf.Database, err)
	}

	createCollectionOptions := options.CreateCollectionOptions{}
	createCollectionOptions.SetCapped(true)
	createCollectionOptions.SetSizeInBytes(SYSTEM_PROFILE_MAX_SIZE) // FIXME: what if we have so much ops that we are not fast enough to consume messages? Or if we have huge documents? How is the java one handling that?

	if err := db.CreateCollection(ctx, SYSTEM_PROFILE, &createCollectionOptions); err != nil {
		if e, ok := err.(mongo.ServerError); ok {
			if !e.HasErrorCode(COLLECTION_EXISTS_ERROR) {
				return fmt.Errorf("failed to create %s for Mongo host %s (database: %s): %w", SYSTEM_PROFILE, l.conf.Host, l.conf.Database, err)
			}
		}
	}

	// turn on profiler
	res = db.RunCommand(ctx, bson.D{
		{Key: "profile", Value: 1},
		{Key: "slowms", Value: QUERY_SLOW_MS},
	})

	if res.Err() != nil {
		return fmt.Errorf("failed to start profiler for Mongo host %s (database: %s): %w", l.conf.Host, l.conf.Database, res.Err())
	}

	// start change stream
	collection := db.Collection(SYSTEM_PROFILE)

	// No way to open a change stream against a system collection so use a tailable cursor instead (https://www.mongodb.com/community/forums/t/why-change-streams-cannot-be-used-with-local-database/3063)
	var cursor *mongo.Cursor
	cursorOptions := options.FindOptions{}
	cursorOptions.SetCursorType(options.Tailable)
	cursorOptions.SetSort(bson.M{"$natural": 1})

	Info("starting change stream against %s for Mongo host %s (database: %s)", SYSTEM_PROFILE, l.conf.Host, l.conf.Database)

	for !l.stopChangeStream {
		// According to MongoDB, the driver handle reconnection in case of network error

		if ctx.Err() != nil {
			Warn("change stream cursor error against %s for Mongo host %s (database %s): %v", SYSTEM_PROFILE, l.conf.Host, l.conf.Database, ctx.Err())
		}

		if cursor == nil || cursor.ID() == 0 || ctx.Err() != nil { // Cursor was closed - create a new cursor (actually fine since this is a very small capped collection)
			Info("change stream cursor closed against %s for Mongo host %s (database: %s) Will retry after %s", SYSTEM_PROFILE, l.conf.Host, l.conf.Database, RETRY_AFTER.String())
			time.Sleep(RETRY_AFTER)

			cursorQuery := bson.M{
				"ns": bson.M{
					"$regex": fmt.Sprintf("^%s.", l.conf.Database),                  // only the database in our conf
					"$ne":    fmt.Sprintf("%s.%s", l.conf.Database, SYSTEM_PROFILE), // all collections except system.profile
				},
				"ts": bson.M{
					"$gt": l.lastTimestamp,
				},
			}

			cursor, err = collection.Find(ctx, cursorQuery, &cursorOptions)
			if err != nil {
				Error("failed to obtain cursor for mongo host %s (database: %s): %v", l.conf.Host, l.conf.Database, err)
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

	Info("attempting to stop profiler for mongo host %s (database: %s)", l.conf.Host, l.conf.Database)

	// 2. stop profiler
	res := l.client.Database(l.conf.Database).RunCommand(ctx, bson.M{
		"profile": 0,
	})

	if res.Err() != nil {
		return fmt.Errorf("failed to stop profiler for Mongo host %s (database: %s): %w", l.conf.Host, l.conf.Database, res.Err())
	}

	Info("successfully stopped profiler for mongo host %s (database: %s)", l.conf.Host, l.conf.Database)

	// 3. close connection with source store
	if err := l.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to close connection with Mongo host %s (database: %s): %w", l.conf.Host, l.conf.Database, err)
	}

	return nil
}

func (l *Listener) connect(ctx context.Context) error {
	opt := options.Client()
	opt.ApplyURI(fmt.Sprintf("mongodb://%v/%v?ssl=%v", l.conf.Host, l.conf.Database, l.conf.SSL)) // TODO: how to handle + mongodb+srv:// probably just pass the URL instead of everything?

	if l.conf.UseAuth() {
		opt.SetAuth(options.Credential{
			Username: l.conf.Username,
			Password: l.conf.Password,
		})
	}

	return WithRetry(MAX_RETRY, RETRY_AFTER, func() error {
		var err error

		Info("trying to establish connection with Mongo host %s (database: %s)", l.conf.Host, l.conf.Database)

		l.client, err = mongo.Connect(ctx, opt)
		if err != nil {
			return fmt.Errorf("failed to establish connection with Mongo host %s (database: %s): %v", l.conf.Host, l.conf.Database, err)
		}

		if err = l.client.Ping(ctx, nil); err != nil {
			return fmt.Errorf("failed to ping Mongo host %s (database: %s): %v", l.conf.Host, l.conf.Database, err)
		}

		Info("established connection with Mongo host %s (database: %s)", l.conf.Host, l.conf.Database)

		return nil
	})
}

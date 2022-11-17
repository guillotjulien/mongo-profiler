package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/guillotjulien/mongo-profiler/internal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const SYSTEM_PROFILE = "system.profile"
const INTERNAL_DB = "mongo-profiler"

func main() {
	ctx := context.Background()

	internalClient, err := mongo.Connect(ctx) // TODO: Lower the timeout and log attempts to connect
	if err != nil {
		internal.Fatal("failed to connect to internal store: %v", err)
	}

	if err := internal.InitSlowOpsRecordCollection(ctx, internalClient.Database(INTERNAL_DB)); err != nil {
		internal.Fatal("failed to initialize %s collection in internal store: %v", internal.SLOWOPS_COLLECTION, err)
	}

	if err := internal.InitSlowOpsExampleRecordCollection(ctx, internalClient.Database(INTERNAL_DB)); err != nil {
		internal.Fatal("failed to initialize %s collection in internal store: %v", internal.SLOWOPS_EXAMPLE_COLLECTION, err)
	}

	conf := internal.Config{
		Host:     "",
		Username: "", // FIXME: Use flags when running the profiler (e.g. mongo-profiler -sourceUri="..." -targetUri="...")
		Password: "",
		SSL:      true,
		Database: "",
	}

	if _, err := conf.Validate(); err != nil {
		internal.Fatal("invalid configuration provided: %v", err)
	}

	l := internal.NewListener(conf)

	teardownComplete := make(chan bool, 1)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-signals // Wait for signal

		internal.Info("received shutdown signal. Stopping profiler")

		if err := l.Stop(ctx); err != nil {
			internal.Fatal("failed to stop profiler: %v", err)
		}

		if err := internalClient.Disconnect(ctx); err != nil {
			internal.Fatal("failed to close connection with internal store: %v", err)
		}

		internal.Info("profiler was successfully stopped")

		teardownComplete <- true
	}()

	err = l.Start(ctx, func(ctx context.Context, data bson.Raw) error {
		entry, err := internal.NewProfilerEntry(conf.Host, data)
		if err != nil {
			internal.Error("failed to read profiling entry: %w", err)
		}

		internal.Info("received slow op entry for %s %v", entry.Collection, entry.Timestamp)

		entry.ToSlowOpsRecord().TryInsert(ctx, internalClient.Database(INTERNAL_DB))
		entry.ToSlowOpsExampleRecord().TryInsert(ctx, internalClient.Database(INTERNAL_DB))

		return nil
	})

	if err != nil {
		internal.Fatal("%v", err)
	}

	<-teardownComplete // wait for teardown

	os.Exit(0)
}

package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/guillotjulien/mongo-profiler/internal"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	listenedURI := flag.String("listened", "", "Connection string URI of listened MongoDB installation")
	internalURI := flag.String("internal", "mongodb://localhost:27017/profiler", "Connection string URI of internal MongoDB installation")

	flag.Parse()

	if *listenedURI == "" || *internalURI == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	ctx := context.Background()
	listenedClient, err := internal.NewMongoClient(ctx, *listenedURI)
	if err != nil {
		internal.Fatal("failed to instantiate listened client: %v", err)
	}

	internalClient, err := internal.NewMongoClient(ctx, *internalURI)
	if err != nil {
		internal.Fatal("failed to instantiate internal client: %v", err)
	}

	if listenedClient.Equal(internalClient) {
		internal.Fatal("cannot use the same database for listened and internal MongoDB installation")
	}

	if err := internalClient.Connect(ctx); err != nil {
		internal.Fatal("failed to connect to internal MongoDB installation: %v", err)
	}

	// Init internal store collections
	if err := internal.InitSlowOpsRecordCollection(ctx, internalClient.C.Database(internalClient.Connstr.Database)); err != nil {
		internal.Fatal("failed to initialize %s collection in target MongoDB installation: %v", internal.SLOWOPS_COLLECTION, err)
	}
	if err := internal.InitSlowOpsExampleRecordCollection(ctx, internalClient.C.Database(internalClient.Connstr.Database)); err != nil {
		internal.Fatal("failed to initialize %s collection in target MongoDB installation: %v", internal.SLOWOPS_EXAMPLE_COLLECTION, err)
	}

	l := internal.NewListener(listenedClient)

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
			internal.Fatal("failed to close connection with target MongoDB installation: %v", err)
		}

		internal.Info("profiler was successfully stopped")

		teardownComplete <- true
	}()

	err = l.Start(ctx, func(ctx context.Context, data bson.Raw) error {
		entry, err := internal.NewProfilerEntry(strings.Join(listenedClient.Connstr.Hosts, ","), data)
		if err != nil {
			internal.Error("failed to read profiling entry: %w", err)
		}

		internal.Info("received slow op entry for %s %v", entry.Collection, entry.Timestamp)

		entry.ToSlowOpsRecord().TryInsert(ctx, internalClient.C.Database(internalClient.Connstr.Database))
		entry.ToSlowOpsExampleRecord().TryInsert(ctx, internalClient.C.Database(internalClient.Connstr.Database))

		return nil
	})

	if err != nil {
		internal.Fatal("%v", err)
	}

	<-teardownComplete // wait for teardown

	os.Exit(0)
}

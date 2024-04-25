package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/guillotjulien/mongo-profiler/internal/collector"
	"github.com/guillotjulien/mongo-profiler/internal/constant"
	"github.com/guillotjulien/mongo-profiler/internal/logger"
	"github.com/guillotjulien/mongo-profiler/internal/mongo"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	listenedURI := flag.String("listened", "", "Connection string URI of listened MongoDB installation")
	internalURI := flag.String("internal", "mongodb://localhost:27017/profiler", "Connection string URI of internal MongoDB installation")
	verbose := flag.Bool("v", false, "Make the profiler more talkative")

	flag.Parse()

	if *listenedURI == "" || *internalURI == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *verbose {
		logger.VERBOSE_LOGS = true
	}

	ctx := context.Background()
	listenedClient, err := mongo.NewClient(ctx, *listenedURI)
	if err != nil {
		logger.Fatal("failed to instantiate listened client: %v", err)
	}

	internalClient, err := mongo.NewClient(ctx, *internalURI)
	if err != nil {
		logger.Fatal("failed to instantiate internal client: %v", err)
	}

	if listenedClient.Equal(internalClient) {
		logger.Fatal("cannot use the same database for listened and internal MongoDB installation")
	}

	if err := internalClient.Connect(ctx); err != nil {
		logger.Fatal("failed to connect to internal MongoDB installation: %v", err)
	}

	// Init internal store collections
	if err := collector.InitSlowOpsRecordCollection(ctx, internalClient.C.Database(internalClient.Connstr.Database)); err != nil {
		logger.Fatal("failed to initialize %s collection in listened MongoDB installation: %v", constant.PROFILER_SLOWOPS_COLLECTION, err)
	}
	if err := collector.InitSlowOpsExampleRecordCollection(ctx, internalClient.C.Database(internalClient.Connstr.Database)); err != nil {
		logger.Fatal("failed to initialize %s collection in listened MongoDB installation: %v", constant.PROFILER_SLOWOPS_EXAMPLE_COLLECTION, err)
	}

	c := collector.NewCollector(listenedClient)

	teardownComplete := make(chan bool, 1)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-signals // Wait for signal

		logger.Info("received shutdown signal. Stopping collector")

		if err := c.Stop(ctx); err != nil {
			logger.Fatal("failed to stop collector: %v", err)
		}

		if err := internalClient.Disconnect(ctx); err != nil {
			logger.Fatal("failed to close connection with target MongoDB installation: %v", err)
		}

		logger.Info("collector was successfully stopped")

		teardownComplete <- true
	}()

	err = c.Start(ctx, func(ctx context.Context, data bson.Raw) error {
		entry, err := collector.NewProfilerEntry(strings.Join(listenedClient.Connstr.Hosts, ","), data)
		if err != nil {
			logger.Error("failed to read profiling entry: %w", err)
		}

		logger.Info("received slow op entry for %s %v", entry.Collection, entry.Timestamp)

		entry.ToSlowOpsRecord().TryInsert(ctx, internalClient.C.Database(internalClient.Connstr.Database))
		entry.ToSlowOpsExampleRecord().TryInsert(ctx, internalClient.C.Database(internalClient.Connstr.Database))

		return nil
	})

	if err != nil {
		logger.Fatal("%v", err)
	}

	<-teardownComplete // wait for teardown

	os.Exit(0)
}

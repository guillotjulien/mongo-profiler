package internal

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

type MongoClient struct {
	C       *mongo.Client
	Connstr connstring.ConnString
}

func NewMongoClient(ctx context.Context, uri string) (client *MongoClient, err error) {
	connstr, err := connstring.ParseAndValidate(uri)
	if err != nil {
		return nil, err
	}

	if connstr.Database == "" {
		return nil, errors.New("no database provided")
	}

	opt := options.Client()
	opt.ApplyURI(uri)

	if !connstr.ConnectTimeoutSet { // Set a sensible timeout
		opt.SetConnectTimeout(CONNECT_TIMEOUT)
		opt.SetServerSelectionTimeout(CONNECT_TIMEOUT)
	}

	if !connstr.SocketTimeoutSet { // Set a sensible timeout for queries
		opt.SetSocketTimeout(SOCKET_TIMEOUT)
	}

	cmdMonitor := &event.CommandMonitor{
		Started: func(_ context.Context, evt *event.CommandStartedEvent) {
			Trace("Started command %v: %v", evt.RequestID, evt.Command)
		},
		Succeeded: func(_ context.Context, evt *event.CommandSucceededEvent) {
			Trace("Completed command %v after %vns", evt.RequestID, evt.DurationNanos)
		},
		Failed: func(_ context.Context, evt *event.CommandFailedEvent) {
			Trace("Failed command %v: %v", evt.RequestID, evt.Failure)
		},
	}

	opt.SetMonitor(cmdMonitor)

	c, err := mongo.NewClient(opt)
	if err != nil {
		return nil, err
	}

	client = &MongoClient{
		C:       c,
		Connstr: connstr,
	}

	return client, nil
}

func (client *MongoClient) Connect(ctx context.Context) error {
	return WithRetry(MAX_RETRY, RETRY_AFTER, func() error {
		Info("trying to establish connection with host %v", client.Connstr.Hosts)

		if err := client.C.Connect(ctx); err != nil {
			return fmt.Errorf("cannot establish connection with host: %s", err)
		}
		if err := client.C.Ping(ctx, nil); err != nil {
			return fmt.Errorf("cannot ping host: %s", err)
		}

		Info("established connection with host %v", client.Connstr.Hosts)

		return nil
	})
}

func (client *MongoClient) Disconnect(ctx context.Context) error {
	return client.C.Disconnect(ctx)
}

func (client *MongoClient) Equal(cmpClient *MongoClient) bool {
	// TODO: Compare hosts and database, if they are the same, return true
	return false
}

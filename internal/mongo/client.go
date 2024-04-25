package mongo

import (
	"context"
	"errors"
	"fmt"

	"github.com/guillotjulien/mongo-profiler/internal/constant"
	"github.com/guillotjulien/mongo-profiler/internal/logger"

	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

type Client struct {
	C       *mongo.Client
	Connstr connstring.ConnString
}

func NewClient(ctx context.Context, uri string) (client *Client, err error) {
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
		opt.SetConnectTimeout(constant.MONGO_CONNECT_TIMEOUT)
		opt.SetServerSelectionTimeout(constant.MONGO_CONNECT_TIMEOUT)
	}

	if !connstr.SocketTimeoutSet { // Set a sensible timeout for queries
		opt.SetSocketTimeout(constant.MONGO_SOCKET_TIMEOUT)
	}

	cmdMonitor := &event.CommandMonitor{
		Started: func(_ context.Context, evt *event.CommandStartedEvent) {
			logger.Trace("Started command %v: %v", evt.RequestID, evt.Command)
		},
		Succeeded: func(_ context.Context, evt *event.CommandSucceededEvent) {
			logger.Trace("Completed command %v after %vns", evt.RequestID, evt.DurationNanos)
		},
		Failed: func(_ context.Context, evt *event.CommandFailedEvent) {
			logger.Trace("Failed command %v: %v", evt.RequestID, evt.Failure)
		},
	}

	opt.SetMonitor(cmdMonitor)

	c, err := mongo.NewClient(opt)
	if err != nil {
		return nil, err
	}

	client = &Client{
		C:       c,
		Connstr: connstr,
	}

	return client, nil
}

func (client *Client) Connect(ctx context.Context) error {
	return withRetry(constant.MAX_RETRY, constant.RETRY_AFTER, func() error {
		logger.Info("trying to establish connection with host %v", client.Connstr.Hosts)

		if err := client.C.Connect(ctx); err != nil {
			return fmt.Errorf("cannot establish connection with host: %s", err)
		}
		if err := client.C.Ping(ctx, nil); err != nil {
			return fmt.Errorf("cannot ping host: %s", err)
		}

		logger.Info("established connection with host %v", client.Connstr.Hosts)

		return nil
	})
}

func (client *Client) Disconnect(ctx context.Context) error {
	return client.C.Disconnect(ctx)
}

func (client *Client) Equal(cmpClient *Client) bool {
	// TODO: Compare hosts and database, if they are the same, return true
	return false
}

func (client *Client) GetDefaultDatabase() *mongo.Database {
	return client.C.Database(client.Connstr.Database)
}

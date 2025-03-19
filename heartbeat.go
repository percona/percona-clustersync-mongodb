package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona-lab/percona-mongolink/config"
	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
)

var (
	errConcurrentProcess = errors.New("detected concurrent process")
	errStaleHeartbeat    = errors.New("stale heartbeat")
)

const heartbeatID = "mongolink"

type StopHeartbeat func(context.Context) error

func RunHeartbeat(ctx context.Context, m *mongo.Client) (StopHeartbeat, error) {
	lg := log.New("heartbeat")

	lastBeat, err := doFirstHeartbeat(ctx, m)
	switch {
	case err == nil:
		// nothing

	case errors.Is(err, errConcurrentProcess):
		lastBeat, err = retryHeartbeat(ctx, m)
		if err != nil {
			return nil, errors.Wrap(err, "retry")
		}

	case errors.Is(err, errStaleHeartbeat):
		lg.Warn("Detected stale heartbeat")

	default:
		return nil, errors.Wrap(err, "first")
	}

	lg.With(log.Int64("hb", lastBeat)).Trace("")

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		lastBeat := lastBeat

		for {
			time.Sleep(config.HeartbeatInternal)

			savedBeat, err := doHeartbeat(ctx, m, lastBeat)
			switch {
			case err == nil:
				// nothing

			case errors.Is(err, context.Canceled):
				lg.Info("Heartbeat canceled")

				return

			case errors.Is(err, errConcurrentProcess):
				lg.Error(err, "Detected concurrent heartbeat")

			case errors.Is(err, errStaleHeartbeat):
				lg.Warn("Detected stale heartbeat")

			case errors.Is(err, mongo.ErrNoDocuments):
				lg.Warn("The previous heartbeat is missing")

			default:
				lg.Error(err, "beat")

				continue
			}

			lastBeat = savedBeat
			lg.With(log.Int64("hb", lastBeat)).Trace("")
		}
	}()

	stop := func(ctx context.Context) error {
		cancel()

		return DeleteHeartbeat(ctx, m)
	}

	return stop, nil
}

func retryHeartbeat(ctx context.Context, m *mongo.Client) (int64, error) {
	lg := log.New("heartbeat")

	lastBeat, err := doFirstHeartbeat(ctx, m)
	if err == nil {
		return lastBeat, nil
	}
	if errors.Is(err, errStaleHeartbeat) {
		lg.Warn("Detected stale heartbeat")
	} else {
		lg.Error(err, "Heartbeat error")
	}

	lg.Infof("Try heartbeat again in %s", config.HeartbeatTimeout)

	t := time.NewTicker(config.HeartbeatTimeout)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err() //nolint:wrapcheck

		case <-t.C:
		}

		stopHeartbeat, err := doFirstHeartbeat(ctx, m)
		if err != nil {
			if errors.Is(err, errStaleHeartbeat) {
				lg.Warn("Detected stale heartbeat")

				return stopHeartbeat, nil
			}

			lg.Error(err, "Heartbeat error")
			lg.Infof("Try heartbeat again in %s", config.HeartbeatTimeout)

			continue
		}

		return stopHeartbeat, nil
	}
}

func doFirstHeartbeat(ctx context.Context, m *mongo.Client) (int64, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, config.HeartbeatTimeout)
	defer cancel()

	currBeat := time.Now().Unix()

	_, err := m.Database(config.MongoLinkDatabase).
		Collection(config.HeartbeatCollection).
		InsertOne(timeoutCtx, bson.D{{"_id", heartbeatID}, {"time", currBeat}})
	if err == nil {
		return currBeat, nil
	}

	if !mongo.IsDuplicateKeyError(err) {
		return 0, err //nolint:wrapcheck
	}

	raw, err := m.Database(config.MongoLinkDatabase).
		Collection(config.HeartbeatCollection).
		FindOne(ctx, bson.D{{"_id", heartbeatID}}).
		Raw()
	if err != nil {
		return 0, errors.Wrap(err, "find")
	}

	lastBeat, _ := raw.Lookup("time").AsInt64OK()

	if time.Since(time.Unix(lastBeat, 0)) < config.StaleHeartbeatDuration {
		return 0, errConcurrentProcess
	}

	currBeat, err = doHeartbeat(ctx, m, lastBeat)
	if err != nil {
		return currBeat, errors.Wrap(err, "beat")
	}

	return currBeat, nil
}

func doHeartbeat(ctx context.Context, m *mongo.Client, lastBeat int64) (int64, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, config.HeartbeatTimeout)
	defer cancel()

	currBeat := time.Now().Unix()

	raw, err := m.Database(config.MongoLinkDatabase).
		Collection(config.HeartbeatCollection).
		FindOneAndUpdate(timeoutCtx,
			bson.D{{"_id", heartbeatID}},
			bson.D{{"$set", bson.D{{"time", currBeat}}}},
			options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.Before)).
		Raw()
	if err != nil {
		return currBeat, err //nolint:wrapcheck
	}

	savedBeat, _ := raw.Lookup("time").AsInt64OK()

	if time.Since(time.Unix(savedBeat, 0)) >= config.StaleHeartbeatDuration {
		return currBeat, errStaleHeartbeat
	}

	if savedBeat != lastBeat {
		return currBeat, errConcurrentProcess
	}

	return currBeat, nil
}

func DeleteHeartbeat(ctx context.Context, m *mongo.Client) error {
	_, err := m.Database(config.MongoLinkDatabase).
		Collection(config.HeartbeatCollection).
		DeleteOne(ctx, bson.D{{"_id", heartbeatID}})

	return err //nolint:wrapcheck
}

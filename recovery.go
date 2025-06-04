package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-link-mongodb/config"
	"github.com/percona/percona-link-mongodb/errors"
	"github.com/percona/percona-link-mongodb/log"
)

var errNoRecoveryData = errors.New("no recovery data")

const recoveryID = "plm"

type Recoverable interface {
	Checkpoint(ctx context.Context) ([]byte, error)
	Recover(ctx context.Context, data []byte) error
}

type checkpoint struct {
	ID   string    `bson:"_id"`
	TS   time.Time `bson:"_ts"`
	Data bson.Raw  `bson:"data"`
}

func Restore(ctx context.Context, m *mongo.Client, rec Recoverable) error {
	lg := log.New("recovery")

	lg.Infof("Checking Recovery Data for %q", recoveryID)

	var cp checkpoint

	err := m.Database(config.PLMDatabase).
		Collection(config.RecoveryCollection).
		FindOne(ctx, bson.D{{"_id", recoveryID}}).
		Decode(&cp)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			lg.Info("Recovery Data not found")

			return nil
		}

		return errors.Wrap(err, "find")
	}

	lg.Info("Found Recovery Data. Recovering...")

	err = rec.Recover(ctx, cp.Data)
	if err != nil {
		return errors.Wrap(err, "recover")
	}

	lg.Info("Successfully recovered")

	return nil
}

func RunCheckpointing(ctx context.Context, m *mongo.Client, rec Recoverable) {
	lg := log.New("checkpointing")

	for {
		err := DoCheckpoint(ctx, m, rec)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if !errors.Is(err, errNoRecoveryData) {
				lg.Error(err, "Failed to save a checkpoint")
			}
		} else {
			lg.Debug("Checkpoint saved")
		}

		time.Sleep(config.RecoveryCheckpointingInternal)
	}
}

func DoCheckpoint(ctx context.Context, m *mongo.Client, rec Recoverable) error {
	data, err := rec.Checkpoint(ctx)
	if err != nil {
		return errors.Wrap(err, "checkpoint")
	}
	if len(data) == 0 {
		return errNoRecoveryData
	}

	_, err = m.Database(config.PLMDatabase).
		Collection(config.RecoveryCollection).
		ReplaceOne(ctx,
			bson.D{{"_id", recoveryID}},
			checkpoint{
				ID:   recoveryID,
				TS:   time.Now(),
				Data: data,
			},
			options.Replace().SetUpsert(true))
	if err != nil {
		return errors.Wrap(err, "save")
	}

	return nil
}

func DeleteRecoveryData(ctx context.Context, m *mongo.Client) error {
	_, err := m.Database(config.PLMDatabase).
		Collection(config.RecoveryCollection).
		DeleteOne(ctx, bson.D{{"_id", recoveryID}})

	return err //nolint:wrapcheck
}

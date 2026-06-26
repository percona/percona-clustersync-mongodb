package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
)

var (
	errNoRecoveryData = errors.New("no recovery data")
	// errCheckpointFenced is returned when a checkpoint write is rejected because
	// a newer term owns the lease. A demoted active that keeps trying to write
	// hits this; it is the hard guarantee that a stale active cannot corrupt the
	// target checkpoint.
	errCheckpointFenced = errors.New("checkpoint fenced by newer term")
)

const recoveryID = "pcsm"

type Recoverable interface {
	Checkpoint(ctx context.Context) ([]byte, error)
	Recover(ctx context.Context, data []byte) error
}

type checkpoint struct {
	ID   string    `bson:"_id"`
	TS   time.Time `bson:"_ts"`
	Data bson.Raw  `bson:"data"`
	// Term is the lease term of the active instance that wrote this checkpoint.
	// It is the fencing token: a write is only accepted when its term is greater
	// than or equal to the stored term. A missing field decodes as 0, which is
	// the pre-HA baseline.
	Term int64 `bson:"term,omitempty"`
}

func Restore(ctx context.Context, m *mongo.Client, rec Recoverable) error {
	lg := log.New("recovery")

	lg.Infof("Checking Recovery Data for %q", recoveryID)

	var cp checkpoint

	err := m.Database(config.PCSMDatabase).
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

// RunCheckpointing periodically persists the checkpoint until ctx is canceled.
// It is scoped to a single ACTIVE epoch identified by term; every write is
// fenced against that term. If a write is fenced by a newer term (this instance
// has been deposed), onFenced is invoked once and the loop returns, so the
// demoted active stops writing. The loop is ctx-aware: cancellation stops it
// within one interval rather than after a full sleep.
func RunCheckpointing(ctx context.Context, m *mongo.Client, rec Recoverable, term int64, onFenced func()) {
	lg := log.New("checkpointing").With(log.Int64("term", term))

	ticker := time.NewTicker(config.RecoveryCheckpointingInternal)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			err := DoCheckpoint(ctx, m, rec, term)
			switch {
			case err == nil:
				lg.Debug("Checkpoint saved")

			case errors.Is(err, context.Canceled):
				return

			case errors.Is(err, errCheckpointFenced):
				lg.Warn("Checkpoint fenced by a newer term; stopping checkpointing")
				if onFenced != nil {
					onFenced()
				}

				return

			case errors.Is(err, errNoRecoveryData):
				// Nothing to persist yet.

			default:
				lg.Error(err, "Failed to save a checkpoint")
			}
		}
	}
}

// DoCheckpoint persists the current checkpoint, stamped with term. The write is
// fenced: it is accepted only when no checkpoint exists yet or the stored term
// is not newer than term. A newer stored term means this instance has been
// deposed and the write is rejected with errCheckpointFenced.
func DoCheckpoint(ctx context.Context, m *mongo.Client, rec Recoverable, term int64) error {
	data, err := rec.Checkpoint(ctx)
	if err != nil {
		return errors.Wrap(err, "checkpoint")
	}
	if len(data) == 0 {
		return errNoRecoveryData
	}

	coll := m.Database(config.PCSMDatabase).Collection(config.RecoveryCollection)

	// Term-gated update without upsert: take the document only when our term is
	// at least the stored term (a missing term decodes as 0). A newer stored
	// term excludes the document, so FindOneAndUpdate returns ErrNoDocuments,
	// which we surface as a fence. MongoDB forbids $expr in an upsert predicate,
	// so bootstrap (first write) is a separate insert below.
	filter := bson.D{{"_id", recoveryID}, {"$expr", bson.D{
		{"$lte", bson.A{bson.D{{"$ifNull", bson.A{"$term", int64(0)}}}, term}},
	}}}
	update := bson.D{{"$set", bson.D{
		{"_ts", time.Now()},
		{"term", term},
		{"data", data},
	}}}

	err = coll.FindOneAndUpdate(ctx, filter, update).Err()
	switch {
	case err == nil:
		return nil

	case errors.Is(err, mongo.ErrNoDocuments):
		// Either the document does not exist yet (bootstrap) or a newer term
		// owns it (fence). Disambiguate with a bootstrap insert.
		return doCheckpointBootstrap(ctx, coll, data, term)

	default:
		return errors.Wrap(err, "save checkpoint")
	}
}

// doCheckpointBootstrap inserts the first checkpoint document. A duplicate-key
// collision means the document already exists with a newer term (the fence
// filter excluded it), so the write is fenced.
func doCheckpointBootstrap(ctx context.Context, coll *mongo.Collection, data bson.Raw, term int64) error {
	_, err := coll.InsertOne(ctx, checkpoint{
		ID:   recoveryID,
		TS:   time.Now(),
		Term: term,
		Data: data,
	})
	switch {
	case err == nil:
		return nil

	case mongo.IsDuplicateKeyError(err):
		return errCheckpointFenced

	default:
		return errors.Wrap(err, "bootstrap checkpoint")
	}
}

func DeleteRecoveryData(ctx context.Context, m *mongo.Client) error {
	_, err := m.Database(config.PCSMDatabase).
		Collection(config.RecoveryCollection).
		DeleteOne(ctx, bson.D{{"_id", recoveryID}})

	return err //nolint:wrapcheck
}

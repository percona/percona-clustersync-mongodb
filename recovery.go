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
	// errCheckpointFenced is returned when a checkpoint write is rejected
	// because a newer term owns the checkpoint. It is the hard guarantee that a
	// deposed active cannot corrupt the target checkpoint.
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
	// Term is the fencing token: a write is accepted only when its term is >=
	// the stored term. A missing field decodes as 0 (the pre-HA baseline).
	Term int64 `bson:"term,omitempty"`
	// InstanceID identifies the writer. Informational only; fencing is decided
	// solely on Term.
	InstanceID string `bson:"instanceId,omitempty"`
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
// It is scoped to one ACTIVE epoch: every write is fenced against term. A write
// fenced by a newer term means this instance was deposed; onFenced is invoked
// once and the loop returns.
func RunCheckpointing(
	ctx context.Context, m *mongo.Client, rec Recoverable, term int64, instanceID string, onFenced func(),
) {
	lg := log.New("checkpointing").With(log.Int64("term", term))

	ticker := time.NewTicker(config.RecoveryCheckpointingInternal)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			err := DoCheckpoint(ctx, m, rec, term, instanceID)
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
// fenced: a stored term newer than term means this instance was deposed and the
// write is rejected with errCheckpointFenced.
func DoCheckpoint(ctx context.Context, m *mongo.Client, rec Recoverable, term int64, instanceID string) error {
	data, err := rec.Checkpoint(ctx)
	if err != nil {
		return errors.Wrap(err, "checkpoint")
	}
	if len(data) == 0 {
		return errNoRecoveryData
	}

	coll := m.Database(config.PCSMDatabase).Collection(config.RecoveryCollection)

	// Term-gated update: matches only when our term >= the stored term (missing
	// decodes as 0). MongoDB forbids $expr in an upsert predicate, so the first
	// write is a separate bootstrap insert below.
	filter := bson.D{{"_id", recoveryID}, {"$expr", bson.D{
		{"$lte", bson.A{bson.D{{"$ifNull", bson.A{"$term", int64(0)}}}, term}},
	}}}
	// Store data as bson.Raw so it lands as an embedded document (inspectable
	// in the shell) rather than BSON Binary.
	update := bson.D{{"$set", bson.D{
		{"_ts", time.Now()},
		{"term", term},
		{"instanceId", instanceID},
		{"data", bson.Raw(data)},
	}}}

	err = coll.FindOneAndUpdate(ctx, filter, update).Err()
	switch {
	case err == nil:
		return nil

	case errors.Is(err, mongo.ErrNoDocuments):
		// The document is absent (bootstrap) or a newer term owns it (fence).
		// The bootstrap insert disambiguates.
		return doCheckpointBootstrap(ctx, coll, data, term, instanceID)

	default:
		return errors.Wrap(err, "save checkpoint")
	}
}

// doCheckpointBootstrap inserts the first checkpoint document. A duplicate-key
// collision means a newer term already owns it: the write is fenced.
func doCheckpointBootstrap(
	ctx context.Context, coll *mongo.Collection, data bson.Raw, term int64, instanceID string,
) error {
	_, err := coll.InsertOne(ctx, checkpoint{
		ID:         recoveryID,
		TS:         time.Now(),
		Term:       term,
		InstanceID: instanceID,
		Data:       data,
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

package main

import (
	"context"
	"strconv"
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
	// errCheckpointExists is returned by the bootstrap insert when another
	// writer created the checkpoint document first; its term decides the rest.
	errCheckpointExists = errors.New("checkpoint already exists")
	// errCheckpointSuperseded is returned when the stored checkpoint of the
	// same term holds a newer snapshot than the one being written. The write
	// is dropped; the instance was not deposed.
	errCheckpointSuperseded = errors.New("checkpoint superseded by a newer snapshot")
	// errNoCheckpoint is returned by Restore when there is no recovery data.
	// A promotion decides what that means: nothing to do for an idle
	// pipeline, a stale pipeline that cannot be replaced otherwise.
	errNoCheckpoint = errors.New("no checkpoint to restore")
)

const recoveryID = "pcsm"

// fencedError is the errCheckpointFenced of one rejected write: it carries the
// term that owns the checkpoint, which the demotion uses to repair a lease
// that fell behind it. It unwraps to the sentinel, so errors.Is still matches.
type fencedError struct {
	StoredTerm int64
}

func (e fencedError) Error() string {
	return errCheckpointFenced.Error() + ": stored term " + strconv.FormatInt(e.StoredTerm, 10)
}

func (e fencedError) Unwrap() error {
	return errCheckpointFenced
}

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
	// Seq orders the snapshots of one term: a write of the same term is
	// accepted only when its seq is >= the stored seq. It is the snapshot's
	// capture order (data.seq), lifted here for the write predicate. A missing
	// field decodes as 0.
	Seq int64 `bson:"seq,omitempty"`
	// InstanceID identifies the writer. Informational only; fencing is decided
	// solely on Term and Seq.
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

			return errNoCheckpoint
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

// RunCheckpointing persists the checkpoint every interval, and as soon as
// saveNow signals, until ctx is canceled. It is the only checkpoint writer of
// one ACTIVE epoch: the epoch's writes share one term, and a single writer
// captures and saves them in order. The snapshot sequence covers what that
// order cannot: a write of the same term that lands late, such as the one the
// previous epoch's writer had on the wire when it was canceled, which MongoDB
// may still apply after the next epoch saved a newer snapshot. Every write is
// fenced against term. A write fenced by a newer term means this instance was
// deposed; onFenced is invoked once with the term that owns the checkpoint and
// the loop returns.
func RunCheckpointing(
	ctx context.Context,
	m *mongo.Client,
	rec Recoverable,
	term int64,
	instanceID string,
	interval time.Duration,
	saveNow <-chan struct{},
	onFenced func(storedTerm int64),
) {
	lg := log.New("checkpointing").With(log.Int64("term", term))

	if interval <= 0 {
		interval = config.RecoveryCheckpointingInternal
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
		case <-saveNow:
		}

		err := DoCheckpoint(ctx, m, rec, term, instanceID)
		switch {
		case errors.Is(err, context.Canceled):
			return

		case errors.Is(err, errCheckpointFenced):
			lg.Warn("Stopping checkpointing")
			if onFenced != nil {
				var fenced fencedError
				_ = errors.As(err, &fenced)
				onFenced(fenced.StoredTerm)
			}

			return
		}
	}
}

// DoCheckpoint persists the current checkpoint, stamped with term. The write is
// fenced: a stored term newer than term means this instance was deposed and the
// write is rejected with errCheckpointFenced. A stored snapshot of the same
// term newer than this one supersedes it: the write is dropped and nil is
// returned, since the instance still owns the checkpoint.
//
// Outcomes are logged here; callers only handle control flow.
func DoCheckpoint(ctx context.Context, m *mongo.Client, rec Recoverable, term int64, instanceID string) error {
	err := doCheckpoint(ctx, m, rec, term, instanceID)

	lg := log.New("checkpointing").With(log.Int64("term", term))
	switch {
	case err == nil:
		lg.Debug("Checkpoint saved")

	case errors.Is(err, errCheckpointFenced):
		// Expected on a STANDBY or a just-deposed active, not a failure.
		lg.Debug("Checkpoint fenced by a newer term")

	case errors.Is(err, errCheckpointSuperseded):
		// A late write of this term: its snapshot is already outdated.
		lg.Debug("Checkpoint superseded by a newer snapshot")

		return nil

	case errors.Is(err, errNoRecoveryData), errors.Is(err, context.Canceled):
		// Nothing to persist yet, or shutting down.

	default:
		lg.Error(err, "Failed to save a checkpoint")
	}

	return err
}

func doCheckpoint(ctx context.Context, m *mongo.Client, rec Recoverable, term int64, instanceID string) error {
	data, err := rec.Checkpoint(ctx)
	if err != nil {
		return errors.Wrap(err, "checkpoint")
	}
	if len(data) == 0 {
		return errNoRecoveryData
	}

	seq := snapshotSeq(data)
	coll := m.Database(config.PCSMDatabase).Collection(config.RecoveryCollection)

	// Ordered update: matches only when our term is newer than the stored one,
	// or the same and our snapshot is at least as new (missing fields decode
	// as 0). MongoDB forbids $expr in an upsert predicate, so the first write
	// is a separate bootstrap insert below.
	storedTerm := bson.D{{"$ifNull", bson.A{"$term", int64(0)}}}
	storedSeq := bson.D{{"$ifNull", bson.A{"$seq", int64(0)}}}
	filter := bson.D{{"_id", recoveryID}, {"$expr", bson.D{{"$or", bson.A{
		bson.D{{"$lt", bson.A{storedTerm, term}}},
		bson.D{{"$and", bson.A{
			bson.D{{"$eq", bson.A{storedTerm, term}}},
			bson.D{{"$lte", bson.A{storedSeq, seq}}},
		}}},
	}}}}}
	// Store data as bson.Raw so it lands as an embedded document (inspectable
	// in the shell) rather than BSON Binary.
	update := bson.D{{"$set", bson.D{
		{"_ts", time.Now()},
		{"term", term},
		{"seq", seq},
		{"instanceId", instanceID},
		{"data", bson.Raw(data)},
	}}}

	err = coll.FindOneAndUpdate(ctx, filter, update).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		// The document is absent (bootstrap), a newer term owns it (fence) or a
		// newer snapshot of our term is stored (superseded). The bootstrap
		// insert tells the first apart.
		err = doCheckpointBootstrap(ctx, coll, data, term, seq, instanceID)
		if !errors.Is(err, errCheckpointExists) {
			return err
		}

		// Another writer created the document between the update and the
		// insert, which by itself says nothing about its term: an older term's
		// late bootstrap must not read as a newer owner. The ordered update
		// decides; the stored document tells fenced from superseded.
		err = coll.FindOneAndUpdate(ctx, filter, update).Err()
		if errors.Is(err, mongo.ErrNoDocuments) {
			return classifyRejectedCheckpoint(ctx, coll, term, seq)
		}
	}

	if err != nil {
		return errors.Wrap(err, "save checkpoint")
	}

	return nil
}

// snapshotSeq reads the snapshot's capture order from the checkpoint data. A
// snapshot without one (written before sequencing existed) orders as 0.
func snapshotSeq(data bson.Raw) int64 {
	seq, _ := data.Lookup("seq").AsInt64OK()

	return seq
}

// classifyRejectedCheckpoint explains an ordered update that matched nothing
// although the document exists: a newer term owns it (errCheckpointFenced) or
// a newer snapshot of the same term is stored (errCheckpointSuperseded).
func classifyRejectedCheckpoint(ctx context.Context, coll *mongo.Collection, term, seq int64) error {
	var stored checkpoint

	err := coll.FindOne(ctx, bson.D{{"_id", recoveryID}}).Decode(&stored)
	if err != nil {
		return errors.Wrap(err, "read rejected checkpoint")
	}

	switch {
	case stored.Term > term:
		return fencedError{StoredTerm: stored.Term}

	case stored.Term == term && stored.Seq > seq:
		return errCheckpointSuperseded

	default:
		// The document changed again between the update and this read; the
		// next write resolves it.
		return errors.Errorf("checkpoint write did not apply: stored term %d seq %d, ours term %d seq %d",
			stored.Term, stored.Seq, term, seq)
	}
}

// doCheckpointBootstrap inserts the first checkpoint document. A duplicate-key
// collision means another writer created it first: errCheckpointExists.
func doCheckpointBootstrap(
	ctx context.Context, coll *mongo.Collection, data bson.Raw, term, seq int64, instanceID string,
) error {
	_, err := coll.InsertOne(ctx, checkpoint{
		ID:         recoveryID,
		TS:         time.Now(),
		Term:       term,
		Seq:        seq,
		InstanceID: instanceID,
		Data:       data,
	})
	switch {
	case err == nil:
		return nil

	case mongo.IsDuplicateKeyError(err):
		return errCheckpointExists

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

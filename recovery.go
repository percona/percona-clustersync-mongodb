package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
)

var (
	errNoRecoveryData = errors.New("no recovery data")
	// errCheckpointFenced is returned when a checkpoint claim or write is
	// rejected because the document is owned by a newer term. It is the hard
	// guarantee that a deposed active cannot corrupt the target checkpoint.
	errCheckpointFenced = errors.New("checkpoint fenced by newer term")
	// errNoCheckpoint is returned by Restore when the claimed checkpoint holds
	// no snapshot: the tenure's fresh pipeline stays idle, for a /start.
	errNoCheckpoint = errors.New("no checkpoint to restore")
)

const (
	recoveryID = "pcsm"

	fieldTerm       = "term"
	fieldInstanceID = "instanceId"
)

type Recoverable interface {
	Checkpoint(ctx context.Context) ([]byte, error)
	Recover(ctx context.Context, data []byte) error
}

// checkpoint is the recovery document. Its term orders everything written to
// it: a tenure claims the document at its term before it restores the
// snapshot, and a write is accepted only at a term no older than the stored
// one. Terms are never reused (each tenure opens above every term before
// it), and within a tenure one writer saves the snapshots one at a time and
// stops at the first write whose outcome it does not learn. So the stored
// snapshot is always the newest one written at or below the stored term. A
// write of an earlier term still on the wire when the next tenure claims the
// document is either the snapshot that tenure restores or rejected.
//
// This ordering covers restarts, failover, lease expiry and uncertain write
// outcomes. It assumes lease terms are not reused while a checkpoint survives:
// reset lease and recovery state together, with every instance stopped and
// its outstanding operations finished. Live resets and manual deletion of HA
// state are operator errors, not recovery paths this protocol repairs.
type checkpoint struct {
	ID string `bson:"_id"`
	// TS is a client-clock diagnostic timestamp: document creation or the last
	// snapshot write. Claims on an existing document leave it unchanged.
	// It is not used to order snapshots, fence writes or decide recovery.
	TS time.Time `bson:"_ts"`
	// Data is the snapshot; absent on a document claimed before its first
	// write.
	Data bson.Raw `bson:"data,omitempty"`
	// Term is the fencing token: the term of the tenure that owns the
	// document. A missing field decodes as 0 (the pre-HA baseline).
	Term int64 `bson:"term,omitempty"`
	// InstanceID identifies the last claimant or writer for diagnostics only.
	// A claim updates it without replacing Data, so it need not identify the
	// process that captured the snapshot. Neither fencing nor recovery checks
	// this field; Term alone decides whether a claim or write is accepted.
	InstanceID string `bson:"instanceId,omitempty"`
}

func recoveryCollection(m *mongo.Client) *mongo.Collection {
	return m.Database(config.PCSMDatabase).Collection(config.RecoveryCollection)
}

// ownedAt matches the checkpoint document when it is owned by term or an
// older one. A missing term decodes as 0.
func ownedAt(term int64) bson.D {
	return bson.D{{"_id", recoveryID}, {"$expr", bson.D{
		{"$lte", bson.A{bson.D{{"$ifNull", bson.A{"$" + fieldTerm, int64(0)}}}, term}},
	}}}
}

// Restore claims the checkpoint for a tenure at term and recovers rec from
// the snapshot it holds. The claim stamps the document with term, creating it
// when absent, before anything is restored: from then on every write of an
// older term is fenced, so a write a previous tenure left on the wire either
// landed before the claim, and is the snapshot restored, or is rejected. A
// document owned by a newer term fences the claim (errCheckpointFenced); a
// claimed document without a snapshot is errNoCheckpoint, and the tenure's
// fresh pipeline stays idle, for a /start.
func Restore(ctx context.Context, m *mongo.Client, rec Recoverable, term int64, instanceID string) error {
	lg := log.New("recovery").With(log.Int64("term", term))

	lg.Infof("Claiming Recovery Data for %q", recoveryID)

	cp, err := claimCheckpoint(ctx, recoveryCollection(m), term, instanceID)
	if err != nil {
		if errors.Is(err, errCheckpointFenced) {
			lg.Warn("Recovery Data is owned by a newer term")
		}

		return err
	}

	if len(cp.Data) == 0 {
		lg.Info("Recovery Data not found")

		return errNoCheckpoint
	}

	lg.Info("Found Recovery Data. Recovering...")

	err = rec.Recover(ctx, cp.Data)
	if err != nil {
		return errors.Wrap(err, "recover")
	}

	lg.Info("Successfully recovered")

	return nil
}

// claimCheckpoint stamps the checkpoint document with term and returns it as
// stamped. Two steps, because MongoDB forbids $expr in an upsert predicate: a
// term-gated update, then a bootstrap insert when no document exists. The
// insert is not an upsert: a duplicate key is how a document created
// meanwhile is detected, which by itself says nothing about its term (an
// older term's late bootstrap must not read as a newer owner), so the gated
// update decides once more. Matching nothing then means the document is owned
// by a newer term: errCheckpointFenced.
func claimCheckpoint(ctx context.Context, coll *mongo.Collection, term int64, instanceID string) (checkpoint, error) {
	stamp := bson.D{{"$set", bson.D{{fieldTerm, term}, {fieldInstanceID, instanceID}}}}
	stamped := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var cp checkpoint

	err := coll.FindOneAndUpdate(ctx, ownedAt(term), stamp, stamped).Decode(&cp)
	if err == nil {
		return cp, nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return checkpoint{}, errors.Wrap(err, "claim checkpoint")
	}

	_, err = coll.InsertOne(ctx, bson.D{
		{"_id", recoveryID},
		{"_ts", time.Now()},
		{fieldTerm, term},
		{fieldInstanceID, instanceID},
	})
	switch {
	case err == nil:
		return checkpoint{ID: recoveryID, Term: term, InstanceID: instanceID}, nil

	case !mongo.IsDuplicateKeyError(err):
		return checkpoint{}, errors.Wrap(err, "bootstrap checkpoint")
	}

	err = coll.FindOneAndUpdate(ctx, ownedAt(term), stamp, stamped).Decode(&cp)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return checkpoint{}, errCheckpointFenced
	}

	return cp, errors.Wrap(err, "claim checkpoint")
}

// RunCheckpointing persists the checkpoint every interval, and as soon as
// saveNow signals, until ctx is canceled. It is the only checkpoint writer of
// one ACTIVE tenure, and it saves one snapshot at a time, so the snapshots of
// the tenure reach the document in capture order. That holds only while every
// write's outcome is known: the first write that is fenced, or whose error is
// treated as unsettled (every other error is, whether or not the write could
// have applied), ends the writer and is returned, so no later snapshot of
// this term is ever written and a write still on the wire can only be the
// newest of the term. The tenure ends with its writer. A canceled ctx returns
// nil.
//
// Driver retries belong to the same logical write and may succeed without
// ending the tenure. Once an error reaches this loop, do not capture another
// snapshot at this term: recovering through a new claim orders any write
// whose result was lost. Treating even definite failures this way keeps one
// recovery policy rather than relying on error-specific retry assumptions.
func RunCheckpointing(
	ctx context.Context,
	m *mongo.Client,
	rec Recoverable,
	term int64,
	instanceID string,
	interval time.Duration,
	saveNow <-chan struct{},
) error {
	lg := log.New("checkpointing").With(log.Int64("term", term))

	if interval <= 0 {
		interval = config.RecoveryCheckpointingInternal
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
		case <-saveNow:
		}

		err := DoCheckpoint(ctx, m, rec, term, instanceID)
		switch {
		case err == nil, errors.Is(err, errNoRecoveryData):

		case ctx.Err() != nil:
			return nil

		default:
			lg.Warn("Stopping checkpointing")

			return err
		}
	}
}

// DoCheckpoint persists the current checkpoint, stamped with term. The write
// is fenced: a document owned by a newer term rejects it with
// errCheckpointFenced. Any other error is treated as an unsettled outcome:
// the write may still apply.
//
// Outcomes are logged here; callers only handle control flow.
func DoCheckpoint(ctx context.Context, m *mongo.Client, rec Recoverable, term int64, instanceID string) error {
	err := doCheckpoint(ctx, m, rec, term, instanceID)

	lg := log.New("checkpointing").With(log.Int64("term", term))
	switch {
	case err == nil:
		lg.Debug("Checkpoint saved")

	case errors.Is(err, errCheckpointFenced):
		// Expected on a just-deposed active, not a failure.
		lg.Debug("Checkpoint fenced by a newer term")

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

	// The document exists: the tenure claimed it in Restore. One that matches
	// nothing is owned by a newer term now (or is gone, which only a reset
	// under a running instance produces); either way this tenure no longer
	// owns the checkpoint. Store data as bson.Raw so it lands as an embedded
	// document (inspectable in the shell) rather than BSON Binary.
	update := bson.D{{"$set", bson.D{
		{"_ts", time.Now()},
		{fieldTerm, term},
		{fieldInstanceID, instanceID},
		{"data", bson.Raw(data)},
	}}}

	err = recoveryCollection(m).FindOneAndUpdate(ctx, ownedAt(term), update,
		options.FindOneAndUpdate().SetProjection(bson.D{{"_id", 1}})).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return errCheckpointFenced
	}

	return errors.Wrap(err, "save checkpoint")
}

func DeleteRecoveryData(ctx context.Context, m *mongo.Client) error {
	_, err := recoveryCollection(m).DeleteOne(ctx, bson.D{{"_id", recoveryID}})

	return err //nolint:wrapcheck
}

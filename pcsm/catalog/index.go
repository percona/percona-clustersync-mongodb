package catalog

import (
	"context"
	"slices"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/mdb"
)

// IndexUnsuccessfulType describes why an index ended up unsuccessful at finalize time.
type IndexUnsuccessfulType string

const (
	// IndexFailed means the index failed to create on the target cluster.
	IndexFailed IndexUnsuccessfulType = "failed"
	// IndexIncomplete means the index was being built on the source when replication observed it.
	IndexIncomplete IndexUnsuccessfulType = "incomplete"
	// IndexInconsistent means the index was inconsistent across shards on the source cluster.
	IndexInconsistent IndexUnsuccessfulType = "inconsistent"
)

const (
	finalizeReasonNoLongerPresent         = "no longer present on source"
	finalizeReasonSourceIndexBuilding     = "index is still building on one or more source shards"
	finalizeReasonSourceIndexInconsistent = "index is missing on one or more source shards"
)

// UnsuccessfulIndex describes an index that did not complete cleanly during replication
// and was not recovered during finalize.
type UnsuccessfulIndex struct {
	Namespace string
	Name      string
	Keys      bson.Raw
	Type      IndexUnsuccessfulType
	Reason    string
}

// finalizeUnsuccessfulIndexes finalizes indexes that were unsuccessful during
// replication. Failed entries are recreated from their stored specs. Other
// entries are recreated from their current source specs only after current
// source state confirms that they are complete and consistent.
//
// The source checks and recreate are not atomic: replication is paused, but
// source DDL is not fenced, so the current-source-state decision is best-effort.
func (c *Catalog) finalizeUnsuccessfulIndexes(ctx context.Context) []UnsuccessfulIndex {
	lg := log.Ctx(ctx)
	lg.Info("Finalizing unsuccessful indexes")

	var report []UnsuccessfulIndex

	for db, colls := range c.Databases {
		for coll, collEntry := range colls.Collections {
			for _, index := range collEntry.Indexes {
				if !index.Unsuccessful() {
					continue
				}

				originalType := index.unsuccessfulType()
				selectedSpec := index.IndexSpecification
				namespace := Namespace{Database: db, Collection: coll}

				// Keep ordered source probes explicit and local to finalize policy.
				if originalType != IndexFailed { //nolint:nestif
					inProgress, err := c.sourceIndexInProgress(ctx, namespace, index.Name)
					if err != nil {
						lg.Warnf("Index %s on %s.%s was not recreated during finalize: %s",
							index.Name, db, coll, err)
						report = append(report, UnsuccessfulIndex{
							Namespace: db + "." + coll,
							Name:      index.Name,
							Keys:      index.KeysDocument,
							Type:      originalType,
							Reason:    err.Error(),
						})

						continue
					}
					if inProgress {
						lg.Warnf("Index %s on %s.%s was not recreated during finalize: %s",
							index.Name, db, coll, finalizeReasonSourceIndexBuilding)
						report = append(report, UnsuccessfulIndex{
							Namespace: db + "." + coll,
							Name:      index.Name,
							Keys:      index.KeysDocument,
							Type:      IndexIncomplete,
							Reason:    finalizeReasonSourceIndexBuilding,
						})

						continue
					}

					inconsistent, err := c.sourceIndexInconsistent(ctx, namespace, index.Name)
					if err != nil {
						lg.Warnf("Index %s on %s.%s was not recreated during finalize: %s",
							index.Name, db, coll, err)
						report = append(report, UnsuccessfulIndex{
							Namespace: db + "." + coll,
							Name:      index.Name,
							Keys:      index.KeysDocument,
							Type:      originalType,
							Reason:    err.Error(),
						})

						continue
					}
					if inconsistent {
						lg.Warnf("Index %s on %s.%s was not recreated during finalize: %s",
							index.Name, db, coll, finalizeReasonSourceIndexInconsistent)
						report = append(report, UnsuccessfulIndex{
							Namespace: db + "." + coll,
							Name:      index.Name,
							Keys:      index.KeysDocument,
							Type:      IndexInconsistent,
							Reason:    finalizeReasonSourceIndexInconsistent,
						})

						continue
					}

					sourceIndexes, err := mdb.ListIndexes(ctx, c.source, db, coll)
					if err != nil {
						err = errors.Wrap(err, "list source indexes")
						lg.Warnf("Index %s on %s.%s was not recreated during finalize: %s",
							index.Name, db, coll, err)
						report = append(report, UnsuccessfulIndex{
							Namespace: db + "." + coll,
							Name:      index.Name,
							Keys:      index.KeysDocument,
							Type:      originalType,
							Reason:    err.Error(),
						})

						continue
					}

					selectedSpec = findIndexByName(sourceIndexes, index.Name)
					if selectedSpec == nil {
						lg.Warnf("Index %s on %s.%s was not recreated during finalize: %s",
							index.Name, db, coll, finalizeReasonNoLongerPresent)
						report = append(report, UnsuccessfulIndex{
							Namespace: db + "." + coll,
							Name:      index.Name,
							Keys:      index.KeysDocument,
							Type:      originalType,
							Reason:    finalizeReasonNoLongerPresent,
						})

						continue
					}

					lg.Infof("Index %s on %s.%s is valid on source, trying to recreate it",
						index.Name, db, coll)
				} else {
					lg.Infof("Index %s on %s.%s failed to create during replication, trying to recreate it",
						index.Name, db, coll)
				}

				err := runWithRetry(ctx, func(ctx context.Context) error {
					err := c.target.Database(db).RunCommand(ctx, bson.D{
						{"createIndexes", coll},
						{"indexes", bson.A{selectedSpec}},
					}).Err()

					return errors.Wrapf(err, "recreate index %s.%s.%s", db, coll, index.Name)
				})
				if err != nil {
					lg.Warnf("Failed to recreate unsuccessful index %s on %s.%s: %v",
						index.Name, db, coll, err)
					report = append(report, UnsuccessfulIndex{
						Namespace: db + "." + coll,
						Name:      index.Name,
						Keys:      selectedSpec.KeysDocument,
						Type:      originalType,
						Reason:    err.Error(),
					})

					continue
				}

				lg.Infof("Recreated index %s on %s.%s", index.Name, db, coll)

				c.lock.Lock()
				c.addIndexesToCatalog(ctx, db, coll, []indexCatalogEntry{{IndexSpecification: selectedSpec}})
				c.lock.Unlock()
			}
		}
	}

	return report
}

func (i indexCatalogEntry) unsuccessfulType() IndexUnsuccessfulType {
	switch {
	case i.Failed:
		return IndexFailed
	case i.Incomplete:
		return IndexIncomplete
	case i.Inconsistent:
		return IndexInconsistent
	default:
		return ""
	}
}

func (c *Catalog) sourceIndexInProgress(ctx context.Context, namespace Namespace, name string) (bool, error) {
	inProgress, err := mdb.ListInProgressIndexBuilds(ctx, c.source, namespace.Database, namespace.Collection)
	if err != nil {
		return false, errors.Wrap(err, "list source in-progress index builds")
	}

	return slices.Contains(inProgress, name), nil
}

func (c *Catalog) sourceIndexInconsistent(ctx context.Context, namespace Namespace, name string) (bool, error) {
	inconsistent, err := mdb.ListInconsistentIndexes(ctx, c.source, namespace.Database, namespace.Collection)
	if mdb.IsNamespaceNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(err, "list source inconsistent indexes")
	}

	return findIndexByName(inconsistent, name) != nil, nil
}

func findIndexByName(indexes []*mdb.IndexSpecification, name string) *mdb.IndexSpecification {
	idx := slices.IndexFunc(indexes, func(spec *mdb.IndexSpecification) bool {
		return spec != nil && spec.Name == name
	})
	if idx == -1 {
		return nil
	}

	return indexes[idx]
}

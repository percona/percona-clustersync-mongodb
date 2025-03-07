package mongolink

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona-lab/percona-mongolink/topo"
)

type OperationType string

// https://www.mongodb.com/docs/v8.0/changeStreams/
const (
	// Invalidate occurs when an operation renders the change stream invalid.
	Invalidate OperationType = "invalidate"

	// DropDatabase occurs when a database is dropped.
	DropDatabase OperationType = "dropDatabase"

	// Create occurs on the creation of a collection.
	//
	// Requires that you set the showExpandedEvents option to true.
	//
	// New in version 6.0.
	Create OperationType = "create"

	// Rename occurs when a collection is renamed.
	Rename OperationType = "rename"

	// Modify occurs when a collection is modified.
	//
	// Requires that you set the showExpandedEvents option to true.
	//
	// New in version 6.0.
	Modify OperationType = "modify"

	// Drop occurs when a collection is dropped from a database.
	Drop OperationType = "drop"

	// CreateIndexes occurs on the creation of indexes on the collection.
	//
	// Requires that you set the showExpandedEvents option to true.
	//
	// New in version 6.0.
	CreateIndexes OperationType = "createIndexes"

	// DropIndexes occurs when an index is dropped from the collection.
	//
	// Requires that you set the showExpandedEvents option to true.
	//
	// New in version 6.0.
	DropIndexes OperationType = "dropIndexes"

	// Insert occurs when an operation adds documents to a collection.
	Insert OperationType = "insert"

	// Update occurs when an operation updates a document in a collection.
	Update OperationType = "update"

	// Replace occurs when an update operation removes a document from a collection and replaces it
	// with a new document.
	Replace OperationType = "replace"

	// Delete occurs when a document is removed from the collection.
	Delete OperationType = "delete"

	// ShardCollection occurs when a collection is sharded.
	//
	// Requires that you set the showExpandedEvents option to true.
	//
	// New in version 6.0.
	ShardCollection OperationType = "shardCollection"

	// ReshardCollection occurs when the shard key for a collection and the distribution of data
	// changes.
	//
	// Requires that you set the showExpandedEvents option to true.
	//
	// New in version 6.1. Also available in 6.0.14.
	ReshardCollection OperationType = "reshardCollection"

	// RefineCollectionShardKey occurs when a shard key is modified.
	//
	// New in version 6.1.
	RefineCollectionShardKey OperationType = "refineCollectionShardKey"
)

// Namespace is the namespace (database and/or collection) affected by the event.
type Namespace struct {
	// Database is the name of the database where the event occurred.
	Database string `bson:"db"`

	// Collection is the name of the collection where the event occurred.
	Collection string `bson:"coll"`
}

func (ns Namespace) String() string {
	var rv string

	if ns.Collection != "" {
		rv = ns.Database + "." + ns.Collection
	} else {
		rv = ns.Database
	}

	return rv
}

// InvalidateEvent occurs when an operation renders the change stream invalid. For example, a change
// stream opened on a collection that was later dropped or renamed would cause an invalidate event.
type InvalidateEvent struct {
	// OperationType is the type of operation that the change notification reports.
	//
	// Returns a value of create for these change events.
	OperationType OperationType `bson:"operationType"`

	// ID is a BSON object which serves as an identifier for the change stream event.
	//
	// This value is used as the resumeToken for the resumeAfter parameter when resuming a change
	// stream. The _id object has the following form: `{ "_data" : <BinData|hex string> }`
	ID bson.Raw `bson:"_id"`

	// ClusterTime is the timestamp from the oplog entry associated with the event.
	//
	// Change stream event notifications associated with a multi-document transaction all have the
	// same clusterTime value: the time when the transaction was committed.
	//
	// Events with the same clusterTime may not all relate to the same transaction. Some events
	// don't relate to a transaction at all. Starting in MongoDB 8.0, this may be true for events on
	// any deployment. In previous versions, this behavior was possible only for events on a sharded
	// cluster.
	//
	// To identify events for a single transaction, you can use the combination of lsid and
	// txnNumber in the change stream event document.
	//
	// Changed in version 8.0.
	ClusterTime bson.Timestamp `bson:"clusterTime"`

	// WallTime is the server date and time of the database operation. WallTime differs from
	// clusterTime in that clusterTime is a timestamp taken from the oplog entry associated with the
	// database operation event.
	//
	// New in version 6.0.
	// WallTime bsonDateTime `bson:"wallTime"`
}

// BaseEvent is the base structure for all events.
type BaseEvent struct {
	// OperationType is the type of operation that the change notification reports.
	//
	// Returns a value of create for these change events.
	OperationType OperationType `bson:"operationType"`

	// Namespace is the namespace (database and/or collection) affected by the event.
	Namespace Namespace `bson:"ns"`

	// CollectionUUID is the collection's UUID.
	//
	// If the change occurred on a collection, CollectionUUID indicates the collection's UUID. If
	// the change occurred on a view, CollectionUUID doesn't exist.
	//
	// New in version 6.0.
	CollectionUUID *bson.Binary `bson:"collectionUUID,omitempty"`

	// TxnNumber together with the lsid, a number that helps uniquely identify a transaction.
	//
	// Only present if the operation is part of a multi-document transaction.
	TxnNumber *int64 `bson:"txnNumber,omitempty"`

	// LSID is the identifier for the session associated with the transaction.
	//
	// Only present if the operation is part of a multi-document transaction.
	LSID bson.Raw `bson:"lsid,omitempty"`

	// ID is a BSON object which serves as an identifier for the change stream event.
	//
	// This value is used as the resumeToken for the resumeAfter parameter when resuming a change
	// stream. The _id object has the following form: `{ "_data" : <BinData|hex string> }`
	ID bson.Raw `bson:"_id"`

	// ClusterTime is the timestamp from the oplog entry associated with the event.
	//
	// Change stream event notifications associated with a multi-document transaction all have the
	// same clusterTime value: the time when the transaction was committed.
	//
	// Events with the same clusterTime may not all relate to the same transaction. Some events
	// don't relate to a transaction at all. Starting in MongoDB 8.0, this may be true for events on
	// any deployment. In previous versions, this behavior was possible only for events on a sharded
	// cluster.
	//
	// To identify events for a single transaction, you can use the combination of lsid and
	// txnNumber in the change stream event document.
	//
	// Changed in version 8.0.
	ClusterTime bson.Timestamp `bson:"clusterTime"`

	// WallTime is the server date and time of the database operation. WallTime differs from
	// clusterTime in that clusterTime is a timestamp taken from the oplog entry associated with the
	// database operation event.
	//
	// New in version 6.0.
	// WallTime bsonDateTime `bson:"wallTime"`
}

// CreateEvent occurs when a collection is created on a watched database and the change stream has
// the showExpandedEvents option set to true.
//
// New in version 6.0.
type CreateEvent struct {
	// OperationDescription is additional information on the change operation.
	//
	// This document and its subfields only appear when the change stream uses expanded events.
	//
	// New in version 6.0.
	OperationDescription CreateCollectionOptions `bson:"operationDescription"`

	BaseEvent `bson:",inline"`
}

// IsView returns true if the event is for a view.
func (e CreateEvent) IsView() bool {
	return e.CollectionUUID == nil
}

// IsTimeseries returns true if the event is for a timeseries collection.
func (e CreateEvent) IsTimeseries() bool {
	return strings.HasPrefix(e.OperationDescription.ViewOn, "system.buckets.")
}

// DropEvent occurs when a collection is dropped from a database.
type DropEvent struct {
	BaseEvent `bson:",inline"`
}

// DropDatabaseEvent occurs when a database is dropped.
type DropDatabaseEvent struct {
	BaseEvent `bson:",inline"`
}

// CreateIndexesEvent occurs when an index is created on the collection and the change stream has
// the showExpandedEvents option set to true.
//
// New in version 6.0.
type CreateIndexesEvent struct {
	BaseEvent `bson:",inline"`

	// OperationDescription is additional information on the change operation.
	//
	// This document and its subfields only appear when the change stream uses expanded events.
	//
	// New in version 6.0.
	OperationDescription createIndexesOpDesc `bson:"operationDescription"`
}

// createIndexesOpDesc represents the description of the create indexes operation.
type createIndexesOpDesc struct {
	Indexes []*topo.IndexSpecification `bson:"indexes"`
}

// DropIndexesEvent occurs when an index is dropped from the collection and the change stream has
// the showExpandedEvents option set to true.
//
// New in version 6.0.
type DropIndexesEvent struct {
	BaseEvent `bson:",inline"`

	// OperationDescription is additional information on the change operation.
	//
	// This document and its subfields only appear when the change stream uses expanded events.
	//
	// New in version 6.0.
	OperationDescription dropIndexesOpDesc `bson:"operationDescription"`
}

// dropIndexesOpDesc represents the description of the drop indexes operation.
type dropIndexesOpDesc struct {
	Indexes []struct {
		Name string `bson:"name"`
	} `bson:"indexes"`
}

// ModifyEvent occurs when a collection is modified, such as when the collMod command adds or
// removes options from a collection or view. This event is received only if the change stream has
// the showExpandedEvents option set to true.
//
// New in version 6.0.
type ModifyEvent struct {
	BaseEvent `bson:",inline"`

	// OperationDescription is additional information on the change operation.
	//
	// This document and its subfields only appear when the change stream uses expanded events.
	//
	// New in version 6.0.
	OperationDescription modifyOpDesc `bson:"operationDescription"`
}

// modifyOpDesc represents the description of the modify operation.
type modifyOpDesc struct {
	// Index is the index that was modified.
	//
	// New in version 6.0.
	Index *ModifyIndexOption `bson:"index,omitempty"`

	CappedSize *int64 `bson:"cappedSize,omitempty"`
	CappedMax  *int64 `bson:"cappedMax,omitempty"`

	ViewOn   string `bson:"viewOn,omitempty"`
	Pipeline any    `bson:"pipeline,omitempty"`

	ExpireAfterSeconds *int64 `bson:"expireAfterSeconds,omitempty"`

	ChangeStreamPreAndPostImages *struct {
		Enabled bool `bson:"enabled"`
	} `bson:"changeStreamPreAndPostImages,omitempty"`

	Validator       *bson.Raw `bson:"validator,omitempty"`
	ValidatorLevel  *string   `bson:"validatorLevel,omitempty"`
	ValidatorAction *string   `bson:"validatorAction,omitempty"`

	Unknown map[string]any `bson:",inline"`
}

// InsertEvent occurs when an operation adds documents to a collection.
type InsertEvent struct {
	// DocumentKey is the document that contains the _id value of the document created or modified
	// by the CRUD operation.
	//
	// For sharded collections, this field also displays the full shard key for the document. The
	// _id field is not repeated if it is already a part of the shard key.
	DocumentKey bson.D `bson:"documentKey"`

	// FullDocument is the document created by the operation.
	//
	// Changed in version 6.0.
	//
	// Starting in MongoDB 6.0, if you set the changeStreamPreAndPostImages option using
	// db.createCollection(), create, or collMod, then the fullDocument field shows the document
	// after it was inserted, replaced, or updated (the document post-image). fullDocument is always
	// included for insert events.
	FullDocument bson.Raw `bson:"fullDocument"`

	BaseEvent `bson:",inline"`
}

// DeleteEvent occurs when operations remove documents from a collection, such as when a user or
// application executes the delete command.
type DeleteEvent struct {
	// DocumentKey is the document that contains the _id value of the document created or modified
	// by the CRUD operation.
	//
	// For sharded collections, this field also displays the full shard key for the document. The
	// _id field is not repeated if it is already a part of the shard key.
	DocumentKey bson.D `bson:"documentKey"`

	BaseEvent `bson:",inline"`
}

// UpdateEvent occurs when an operation updates a document in a collection.
type UpdateEvent struct {
	// DocumentKey is the document that contains the _id value of the document created or modified
	// by the CRUD operation.
	//
	// For sharded collections, this field also displays the full shard key for the document. The
	// _id field is not repeated if it is already a part of the shard key.
	DocumentKey bson.D `bson:"documentKey"`

	// FullDocument is the document created or modified by a CRUD operation.
	//
	// This field only appears if you configured the change stream with fullDocument set to
	// updateLookup. When you configure the change stream with updateLookup, the field represents
	// the current majority-committed version of the document modified by the update operation. The
	// document may differ from the changes described in updateDescription if any other
	// majority-committed operations have modified the document between the original update
	// operation and the full document lookup.
	//
	// For more information, see [Lookup Full Document for Update
	// Operations](
	// https://www.mongodb.com/docs/v8.0/changeStreams/#std-label-change-streams-updateLookup).
	//
	// Starting in MongoDB 6.0, if you set the changeStreamPreAndPostImages option using
	// db.createCollection(), create, or collMod, then the fullDocument field shows the document
	// after it was inserted, replaced, or updated (the document post-image). fullDocument is always
	// included for insert events.
	//
	// Changed in version 6.0.
	FullDocument bson.D `bson:"fullDocument"`

	// FullDocumentBeforeChange is the document before changes were applied by the operation. That
	// is, the document pre-image.
	//
	// This field is available when you enable the changeStreamPreAndPostImages field for a
	// collection using db.createCollection() method or the create or collMod commands.
	//
	// New in version 6.0.
	// FullDocumentBeforeChange bson.D `bson:"fullDocumentBeforeChange,omitempty"`

	UpdateDescription UpdateDescription `bson:"updateDescription"`

	BaseEvent `bson:",inline"`
}

// UpdateDescription is a document describing the fields that were updated or removed by the update
// operation.
type UpdateDescription struct {
	// DisambiguatedPaths provides clarification of ambiguous field descriptors in
	// updateDescription.
	//
	// When the update change event describes changes on a field where the path contains a period
	// (.) or where the path includes a non-array numeric subfield, the disambiguatedPath field
	// provides a document with an array that lists each entry in the path to the modified field.
	//
	// Requires that you set the showExpandedEvents option to true.
	//
	// New in version 6.1.
	DisambiguatedPaths bson.D `bson:"disambiguatedPaths,omitempty"`

	// An array of fields that were removed by the update operation.
	RemovedFields []string `bson:"removedFields,omitempty"`

	// An array of documents which record array truncations performed with pipeline-based updates
	// using one or more of the following stages:
	//  - $addFields
	//  - $set
	//  - $replaceRoot
	//  - $replaceWith
	//
	// If the entire array is replaced, the truncations will be reported under
	// updateDescription.updatedFields.
	TruncatedArrays []struct {
		// Field is the name of the truncated field.
		Field string `bson:"field"`

		// NewSize is the number of elements in the truncated array.
		NewSize int32 `bson:"newSize"`
	} `bson:"truncatedArrays,omitempty"`

	// A document whose keys correspond to the fields that were modified by the update operation.
	// The value of each field corresponds to the new value of those fields, rather than the
	// operation that resulted in the new value.
	UpdatedFields bson.D `bson:"updatedFields,omitempty"`
}

// ReplaceEvent occurs when an update operation removes a document from a collection and replaces it
// with a new document, such as when the replaceOne method is called.
type ReplaceEvent struct {
	// DocumentKey is the document that contains the _id value of the document created or modified
	// by the CRUD operation.
	//
	// For sharded collections, this field also displays the full shard key for the document. The
	// _id field is not repeated if it is already a part of the shard key.
	DocumentKey bson.Raw `bson:"documentKey"`

	// FullDocument is the new document created by the operation.
	//
	// Starting in MongoDB 6.0, if you set the changeStreamPreAndPostImages option using
	// db.createCollection(), create, or collMod, then the fullDocument field shows the document
	// after it was inserted, replaced, or updated (the document post-image). fullDocument is always
	// included for insert events.
	//
	// Changed in version 6.0.
	FullDocument bson.Raw `bson:"fullDocument"`

	// FullDocumentBeforeChange is the document before changes were applied by the operation. That
	// is, the document pre-image.
	//
	// This field is available when you enable the changeStreamPreAndPostImages field for a
	// collection using db.createCollection() method or the create or collMod commands.
	//
	// New in version 6.0.
	// FullDocumentBeforeChange bson.Raw `bson:"fullDocumentBeforeChange,omitempty"`

	BaseEvent `bson:",inline"`
}

// RenameEvent occurs when a collection is renamed.
type RenameEvent struct {
	// OperationDescription is additional information on the change operation.
	//
	// This document and its subfields only appear when the change stream uses
	// expanded events.
	//
	// New in version 6.0.
	OperationDescription renameOpDesc `bson:"operationDescription"`

	BaseEvent `bson:",inline"`
}

// renameOpDesc represents the description of the rename operation.
type renameOpDesc struct {
	// DropTarget is UUID of the collection that was dropped in the rename operation.
	// DropTarget bson.Binary `bson:"dropTarget,omitempty"`

	// To is the new namespace of the collection after the rename.
	To Namespace `bson:"to"`
}

// ParsingError represents an error that occurred during parsing.
type ParsingError struct {
	cause error
}

// Error returns the error message.
func (e ParsingError) Error() string {
	return "parsing event: " + e.cause.Error()
}

// Unwrap returns the underlying error.
func (e ParsingError) Unwrap() error {
	return e.cause
}

// parseEvent parses the given BSON data into the specified event type.
func parseEvent[T any](data bson.Raw) (*T, error) {
	var event T

	err := bson.Unmarshal(data, &event)
	if err != nil {
		return nil, ParsingError{cause: err}
	}

	return &event, nil
}

package config

import "os"

// UseCollectionBulkWrite determines whether to use the Collection Bulk Write API
// instead of the Client Bulk Write API (introduced in MongoDB v8.0).
// Enabled when the PML_USE_COLLECTION_BULK_WRITE environment variable is set to "1".
func UseCollectionBulkWrite() bool {
	return os.Getenv("PML_USE_COLLECTION_BULK_WRITE") == "1"
}

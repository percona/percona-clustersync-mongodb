package config

import "go.mongodb.org/mongo-driver/v2/mongo/options"

// MongoDB logging configuration constants.
const (
	// MongoLogEnabled indicates if MongoDB logging is enabled.
	MongoLogEnabled = false
	// MongoLogComponent specifies the MongoDB log component.
	MongoLogComponent = options.LogComponentAll
	// MongoLogLevel specifies the MongoDB log level.
	MongoLogLevel = options.LogLevelInfo
)

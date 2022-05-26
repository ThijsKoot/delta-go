package delta

import (
	"encoding/json"
	"fmt"
	"net/url"
)

type Cdc *struct {
	Path            *string             `parquet:"name=Path, type=UTF8, repetitiontype=OPTIONAL"`
	PartitionValues *map[string]*string `parquet:"name=PartitionValues, type=MAP, repetitiontype=OPTIONAL, keytype=UTF8, valuetype=UTF8"`
	Size            *int64              `parquet:"name=Size, type=INT64, repetitiontype=OPTIONAL"`
	Tags            *map[string]*string `parquet:"name=Tags, type=MAP, repetitiontype=OPTIONAL, keytype=UTF8, valuetype=UTF8"`
}
type Format struct {
	// Name of the encoding for files in this table.
	Provider string `json:"provider" parquet:"name=provider, type=BYTE_ARRAY, valuetype=UTF8"`
	// A map containing configuration options for the format.
	Options map[string]string `json:"options" parquet:"name=options, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

type ColumnValueStat struct {
	// Composite HashMap representation of statistics.
	Column map[string]ColumnValueStat
	// Json representation of statistics.
	Value Value
}

type ColumnCountStat struct {
	// Composite HashMap representation of statistics.
	Column map[string]ColumnCountStat
	// Json representation of statistics.
	Value DeltaDataTypeLong
}

// Statistics associated with Add actions contained in the Delta log.
type Stats struct {
	// Number of records in the file associated with the log action.
	NumRecords DeltaDataTypeLong
	// start of per column stats
	// Contains a value smaller than all values present in the file for all columns.
	MinValues map[string]ColumnValueStat
	// Contains a value larger than all values present in the file for all columns.
	MaxValues map[string]ColumnValueStat
	// The number of null values for all columns.
	NullCount map[string]ColumnCountStat
}

// File stats parsed from raw parquet format.
type StatsParsed struct {
	// Number of records in the file associated with the log action.
	NumRecords DeltaDataTypeLong
	// start of per column stats
	// Contains a value smaller than all values present in the file for all columns.
	MinValues map[string]interface{} //parquet::record::Field
	// Contains a value larger than all values present in the file for all columns.
	MaxValues map[string]interface{}
	// The number of null values for all columns.
	NullCount map[string]DeltaDataTypeLong
}

// Delta log action that describes a parquet data file that is part of the table.
type Add struct {
	// A relative path, from the root of the table, to a file that should be added to the table
	Path *string `json:"path" parquet:"name=path, type=BYTE_ARRAY, convertedType=UTF8"`
	// The size of this file in bytes
	Size DeltaDataTypeLong `json:"size" parquet:"name=size, type=INT64"`
	// A map from partition column to value for this file
	PartitionValues *map[string]string `json:"partitionValues" parquet:"name=partitionValues, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	// Partition values stored in raw parquet struct format. In this struct, the column names
	// correspond to the partition columns and the values are stored in their corresponding data
	// type. This is a required field when the table is partitioned and the table property
	// delta.checkpoint.writeStatsAsStruct is set to true. If the table is not partitioned, this
	// column can be omitted.
	//
	// This field is only available in add action records read from checkpoints
	// PartitionValuesParsed map[string]string `json:"partitionValuesParsed" parquet:"name=partitionValuesParsed, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"` // Option<parquet::record::Row>
	// The time this file was created, as milliseconds since the epoch
	ModificationTime DeltaDataTypeTimestamp `json:"modificationTime" parquet:"name=modificationTime, type=INT64"`
	// When false the file must already be present in the table or the records in the added file
	// must be contained in one or more remove actions in the same version
	//
	// streaming queries that are tailing the transaction log can use this flag to skip actions
	// that would not affect the final results.
	DataChange bool `json:"dataChange" parquet:"name=dataChange, type=BOOLEAN"`
	// Contains statistics (e.g., count, min/max values for columns) about the data in this file
	Stats *string `json:"stats" parquet:"name=stats, type=BYTE_ARRAY, valuetype=UTF8"`
	// Contains statistics (e.g., count, min/max values for columns) about the data in this file in
	// raw parquet format. This field needs to be written when statistics are available and the
	// table property: delta.checkpoint.writeStatsAsStruct is set to true.
	//
	// This field is only available in add action records read from checkpoints
	// StatsParsed []byte `json:"statsParsed" parquet:"name=statsParsed, type=BYTE_ARRAY"` // Option<parquet::record::Row>
	// Map containing metadata about this file
	Tags *map[string]string `json:"tags" parquet:"name=tags, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

// Action that describes the metadata of the table.
// This is a top-level action in Delta log entries.
type Metadata struct {
	// Unique identifier for this table
	Id string `json:"path" parquet:"name=path, type=BYTE_ARRAY, convertedType=UTF8"`
	// User-provided identifier for this table
	Name *string `json:"name,omitempty" parquet:"name=name, type=BYTE_ARRAY, convertedType=UTF8"`
	// User-provided description for this table
	Description *string `json:"description,omitempty" parquet:"name=description, type=BYTE_ARRAY, convertedType=UTF8"`
	// Specification of the encoding for the files stored in the table
	Format Format `json:"format" parquet:"name=format"`
	// Schema of the table
	SchemaString *string `json:"schemaString" parquet:"name=schemaString, type=BYTE_ARRAY, convertedType=UTF8"`
	// An array containing the names of columns by which the data should be partitioned
	PartitionColumns *[]string `json:"partitionColumns" parquet:"name=partitionColumns, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	// A map containing configuration options for the table
	Configuration *map[string]string `json:"configuration" parquet:"name=configuration, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	// The time when this metadata action is created, in milliseconds since the Unix epoch
	CreatedTime *DeltaDataTypeTimestamp `json:"createdTime,omitempty" parquet:"name=createdTime, type=INT64"`
}

// Represents a tombstone (deleted file) in the Delta log.
// This is a top-level action in Delta log entries.
type Remove struct {
	// The path of the file that is removed from the table.
	Path *string `json:"path" parquet:"name=path, type=BYTE_ARRAY, convertedType=UTF8"`
	// The timestamp when the remove was added to table state.
	DeletionTimestamp *DeltaDataTypeTimestamp `json:"deletionTimestamp,omitempty" parquet:"name=deletionTimestamp, type=INT64"`
	// Whether data is changed by the remove. A table optimize will report this as false for
	// example, since it adds and removes files by combining many files into one.
	DataChange *bool `json:"dataChange" parquet:"name=dataChange, type=BOOLEAN"`
	// When true the fields partitionValues, size, and tags are present
	//
	// NOTE: Although it's defined as required in scala delta implementation, but some writes
	// it's still nullable so we keep it as Option<> for compatibly.
	// ExtendedFileMetadata *bool `json:"extendedFileMetadata,omitempty" parquet:"name=extendedFileMetadata, type=BOOLEAN"`
	// A map from partition column to value for this file.
	PartitionValues *map[string]*string `json:"partitionValues" parquet:"name=partitionValues, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	// Size of this file in bytes
	Size *DeltaDataTypeLong `json:"size,omitempty" parquet:"name=size, type=INT64"`
	// Map containing metadata about this file
	Tags *map[string]string `json:"tags,omitempty" parquet:"name=tags, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

// Action used by streaming systems to track progress using application-specific versions to
// enable idempotency.
type Txn struct {
	// A unique identifier for the application performing the transaction.
	AppId *string `json:"appId" parquet:"name=appId, type=BYTE_ARRAY, convertedType=UTF8"`
	// An application-specific numeric identifier for this transaction.
	Version DeltaDataTypeVersion `json:"version" parquet:"name=version, type=INT64"`
	// The time when this transaction action was created in milliseconds since the Unix epoch.
	LastUpdated *DeltaDataTypeTimestamp `json:"lastUpdated,omitempty" parquet:"name=lastUpdated, type=INT64"`
}

// Action used to increase the version of the Delta protocol required to read or write to the
// table.
type Protocol struct {
	// Minimum version of the Delta read protocol a client must implement to correctly read the
	// table.
	MinReaderVersion DeltaTableTypeInt `json:"minReaderVersion" parquet:"name=minReaderVersion, type=INT32"`
	// Minimum version of the Delta write protocol a client must implement to correctly read the
	// table.
	MinWriterVersion DeltaTableTypeInt `json:"minWriterVersion" parquet:"name=minWriterVersion, type=INT32"`
}

type ActionType string

const (
	ActionTypeMetadata   ActionType = "add"
	ActionTypeAdd        ActionType = "metaData"
	ActionTypeRemove     ActionType = "remove"
	ActionTypeTxn        ActionType = "txn"
	ActionTypeProtocol   ActionType = "protocol"
	ActionTypeCommitInfo ActionType = "commitInfo"
	ActionTypeInvalid    ActionType = ""
)

func (a *Action) GetType() ActionType {
	if a.Metadata != nil {
		return ActionTypeMetadata
	}
	if a.Add != nil {
		return ActionTypeAdd
	}
	if a.Remove != nil {
		return ActionTypeRemove
	}
	if a.Txn != nil {
		return ActionTypeTxn
	}
	if a.Protocol != nil {
		return ActionTypeProtocol
	}
	if a.CommitInfo != nil {
		return ActionTypeCommitInfo
	}
	return ActionTypeInvalid
}

// Represents an action in the Delta log. The Delta log is an aggregate of all actions performed
// on the table, so the full list of actions is required to properly read a table.
type Action struct {
	// Changes the current metadata of the table. Must be present in the first version of a table.
	// Subsequent `metaData` actions completely overwrite previous metadata.
	Metadata *Metadata `json:"metaData,omitempty" parquet:"name=metaData"`
	// Adds a file to the table state.
	Add *Add `json:"add,omitempty" parquet:"name=add"`
	// Removes a file from the table state.
	Remove *Remove `json:"remove,omitempty" parquet:"name=remove"`
	// Used by streaming systems to track progress externally with application specific version
	// identifiers.
	Txn *Txn `json:"txn,omitempty" parquet:"name=txn"`
	// Describes the minimum reader and writer versions required to read or write to the table.
	Protocol *Protocol `json:"protocol,omitempty" parquet:"name=protocol"`
	// Describes commit provenance information for the table.
	CommitInfo *CommitInfo `json:"commitInfo,omitempty" parquet:"name=commitInfo"`
	Cdc        *Cdc        `parquet:"name=Cdc, repetitiontype=OPTIONAL"`
}

type CommitInfo struct {
	Version             *DeltaDataTypeVersion   `json:"version" parquet:"name=version, type=INT64"`
	Timestamp           *DeltaDataTypeTimestamp `json:"timestamp" parquet:"name=timestamp, type=INT64"`
	UserId              *string                 `json:"userId" parquet:"name=userId, type=BYTE_ARRAY, convertedType=UTF8"`
	UserName            *string                 `json:"userName" parquet:"name=userName, type=BYTE_ARRAY, convertedType=UTF8"`
	Operation           *string                 `json:"operation" parquet:"name=operation, type=BYTE_ARRAY, convertedType=UTF8"`
	OperationParameters *map[string]string      `json:"operationParameters" parquet:"name=operationParameters, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Job                 *Job                    `json:"job" parquet:"name=job"`
	Notebook            *Notebook               `json:"notebook" parquet:"name=notebook"`
	ClusterId           *string                 `json:"clusterId" parquet:"name=clusterId, type=BYTE_ARRAY, convertedType=UTF8"`
	ReadVersion         *DeltaDataTypeLong      `json:"readVersion" parquet:"name=readVersion, type=INT64"`
	IsolationLevel      *string                 `json:"isolationLevel" parquet:"name=isolationLevel, type=BYTE_ARRAY, convertedType=UTF8"`
	IsBlindAppend       *bool                   `json:"isBlindAppend" parquet:"name=isBlindAppend, type=BOOLEAN"`
}

type Job struct {
	JobId       *string `json:"jobId" parquet:"name=jobId, type=BYTE_ARRAY, convertedType=UTF8"`
	JobName     *string `json:"jobName" parquet:"name=jobName, type=BYTE_ARRAY, convertedType=UTF8"`
	RunId       *string `json:"runId" parquet:"name=runId, type=BYTE_ARRAY, convertedType=UTF8"`
	JobOwnerId  *string `json:"jobOwnerId" parquet:"name=jobOwnerId, type=BYTE_ARRAY, convertedType=UTF8"`
	TriggerType *string `json:"triggerType" parquet:"name=triggerType, type=BYTE_ARRAY, convertedType=UTF8"`
}

type Notebook struct {
	NotebookId *string `json:"notebookId" parquet:"name=notebookId, type=BYTE_ARRAY, convertedType=UTF8"`
}

/// Operation performed when creating a new log entry with one or more actions.
/// This is a key element of the `CommitInfo` action.
type DeltaOperation struct {
	/// Represents a Delta `Create` operation.
	/// Would usually only create the table, if also data is written,
	/// a `Write` operations is more appropriate
	Create struct {
		/// The save mode used during the create.
		Mode SaveMode
		/// The storage location of the new table
		Location string
		/// The min reader and writer protocol versions of the table
		Protocol Protocol
		/// Metadata associated with the new table
		Metadata DeltaTableMetaData
	}

	/// Represents a Delta `Write` operation.
	/// Write operations will typically only include `Add` actions.
	Write struct {
		/// The save mode used during the write.
		Mode SaveMode
		/// The columns the write is partitioned by.
		PartitionBy *[]string
		/// The predicate used during the write.
		Predicate *string
	}

	/// Represents a Delta `StreamingUpdate` operation.
	StreamingUpdate struct {
		/// The output mode the streaming writer is using.
		OutputMode OutputMode
		/// The query id of the streaming writer.
		QueryId string
		/// The epoch id of the written micro-batch.
		EpochId int64
	}
	// TODO: Add more operations
}

/// The SaveMode used when performing a DeltaOperation
type SaveMode int

const (
	/// Files will be appended to the target location.
	SaveModeAppend SaveMode = iota
	/// The target location will be overwritten.
	SaveModeOverwrite
	/// If files exist for the target, the operation must fail.
	SaveModeErrorIfExists
	/// If files exist for the target, the operation must not proceed or change any data.
	SaveModeIgnore
)

/// The OutputMode used in streaming operations.
type OutputMode int

const (
	/// Only new rows will be written when new data is available.
	OutputModeAppend OutputMode = iota
	/// The full output (all rows) will be written whenever new data is available.
	OutputModeComplete
	/// Only rows with updates will be written when new or changed data is available.
	OutputModeUpdate
)

func decodePath(path string) (string, error) {
	decoded, err := url.QueryUnescape(path)
	if err != nil {
		return "", fmt.Errorf("unable to decode path: %w", err)
	}
	return decoded, nil
}

func (x *Add) PathDecoded() error {
	p, err := decodePath(*x.Path)
	if err != nil {
		return err
	}
	x.Path = &p
	return nil
}

func (x *Remove) PathDecoded() error {
	p, err := decodePath(*x.Path)
	if err != nil {
		return err
	}
	x.Path = &p
	return nil
}

func (m *Metadata) TryConvertToDeltaTableMetaData() (*DeltaTableMetaData, error) {
	schema, err := m.GetSchema()
	if err != nil {
		return nil, fmt.Errorf("unable to get schema: %w", err)
	}
	return &DeltaTableMetaData{
		Id:               m.Id,
		Name:             m.Name,
		Description:      *m.Description,
		Format:           m.Format,
		Schema:           *schema,
		PartitionColumns: *m.PartitionColumns,
		CreatedTime:      m.CreatedTime,
		Configuration:    *m.Configuration,
	}, nil
}

func (m *Metadata) GetSchema() (*Schema, error) {
	var s Schema
	if err := json.Unmarshal([]byte(*m.SchemaString), &s); err != nil {
		return nil, fmt.Errorf("unable to unmarshal schema: %w", err)
	}
	return &s, nil
}

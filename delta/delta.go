package delta

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/thijskoot/delta-go/delta/schema"
	"github.com/thijskoot/delta-go/storage"
	"github.com/thijskoot/delta-go/util"
)

const DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS uint32 = 10_000_000

// In memory representation of a Delta Table
type DeltaTable struct {
	// The version of the table as of the most recent loaded Delta log entry.
	Version DeltaDataTypeVersion
	// The URI the DeltaTable was loaded from.
	TableUri string
	// the load options used during load
	Config DeltaTableConfig

	State DeltaTableState
	// metadata
	// application_transactions
	Storage storage.StorageBackend // StorageBackend

	LastCheckPoint   *CheckPoint
	LogUri           string
	VersionTimestamp map[DeltaDataTypeVersion]int64
}

// The next commit that's available from underlying storage
// TODO: Maybe remove this and replace it with Some/None and create a `Commit` struct to contain the next commit
type PeekCommit struct {
	// The next commit version and associated actions
	New struct {
		Version DeltaDataTypeVersion
		Actions []Action
	}
	// Provided DeltaVersion is up to date
	UpToDate bool
}

// Options for customizing behavior of a `DeltaTransaction`
type DeltaTransactionOptions struct {
	// number of retry attempts allowed when committing a transaction
	MaxRetryCommitAttempts uint32
}

// Object representing a delta transaction.
// Clients that do not need to mutate action content in case a transaction conflict is encountered
// may use the `commit` method and rely on optimistic concurrency to determine the
// appropriate Delta version number for a commit. A good example of this type of client is an
// append only client that does not need to maintain transaction state with external systems.
// Clients that may need to do conflict resolution if the Delta version changes should use
// the `prepare_commit` and `try_commit_transaction` methods and manage the Delta version
// themselves so that they can resolve data conflicts that may occur between Delta versions.
//
// Please not that in case of non-retryable error the temporary commit file such as
// `_delta_log/_commit_<uuid>.json` will orphaned in storage.
type DeltaTransaction struct {
	DeltaTable *DeltaTable //&'a mut DeltaTable,
	Actions    []Action
	Options    DeltaTransactionOptions
}

type PreparedCommit struct {
	Uri string
}

type CheckPoint struct {
	// Delta table version
	Version DeltaDataTypeVersion // 20 digits decimals
	Size    DeltaDataTypeLong
	Parts   *uint32 // 10 digits decimals
}

type DeltaTableMetaData struct {
	// Unique identifier for this table
	Id *string
	// User-provided identifier for this table
	Name *string
	// User-provided description for this table
	Description *string
	// Specification of the encoding for the files stored in the table
	Format *Format
	// Schema of the table
	Schema *schema.Schema
	// An array containing the names of columns by which the data should be partitioned
	PartitionColumns []string
	// The time when this metadata action is created, in milliseconds since the Unix epoch
	CreatedTime *DeltaDataTypeTimestamp
	// table properties
	Configuration map[string]string
}

type DeltaTableConfig struct {
	// Indicates whether our use case requires tracking tombstones.
	// This defaults to `true`
	//
	// Read-only applications never require tombstones. Tombstones
	// are only required when writing checkpoints, so even many writers
	// may want to skip them.
	RequireTombstones bool

	// Indicates whether DeltaTable should track files.
	// This defaults to `true`
	//
	// Some append-only applications might have no need of tracking any files.
	// Hence, DeltaTable will be loaded with significant memory reduction.
	RequireFiles bool
}

type DeltaTableLoadOptions struct {
	// table root uri
	TableUri string
	// backend to access storage system
	StorageBackend storage.StorageBackend // Box<dyn StorageBackend>
	// specify the version we are going to load: a time stamp, a version, or just the newest
	// available version
	Version DeltaVersion
	// Indicates whether our use case requires tracking tombstones.
	// This defaults to `true`
	//
	// Read-only applications never require tombstones. Tombstones
	// are only required when writing checkpoints, so even many writers
	// may want to skip them.
	RequireTombstones bool
	// Indicates whether DeltaTable should track files.
	// This defaults to `true`
	//
	// Some append-only applications might have no need of tracking any files.
	// Hence, DeltaTable will be loaded with significant memory reduction.
	RequireFiles bool
}

type DeltaTableBuilder struct {
	Options DeltaTableLoadOptions
}

type DeltaVersion struct {
	// load the newest version
	Newest bool
	// specify the version to load
	Version *DeltaDataTypeVersion
	// specify the timestamp in UTC
	Timestamp *time.Time
}

func NewDefaultDeltaVersion() DeltaVersion {
	return DeltaVersion{
		Newest: true,
	}
}

type Guid = string
type DeltaDataTypeDuration = time.Duration
type DeltaDataTypeLong = int64
type DeltaDataTypeVersion = DeltaDataTypeLong
type DeltaDataTypeTimestamp = DeltaDataTypeLong
type DeltaTableTypeInt = int32

func OpenTable(tableUri string) (*DeltaTable, error) {
	// TODO: do we need to create a builder or would DeltaTable suffice, calling Load() on that?
	builder, err := NewDeltaTableBuilderFromUri(tableUri)
	if err != nil {
		return nil, fmt.Errorf("unable to open delta table: %w", err)
	}
	table, err := builder.Load()
	if err != nil {
		return nil, fmt.Errorf("unable to load delta table: %w", err)
	}
	return table, nil
}

func NewDeltaTableBuilderFromUri(tableUri string, opts ...DeltaTableBuilderOption) (*DeltaTableBuilder, error) {
	backend, err := storage.NewBackendForUri(tableUri)
	if err != nil {
		return nil, fmt.Errorf("unable to create backend for uri: %w", err)
	}
	builder := &DeltaTableBuilder{
		DeltaTableLoadOptions{
			TableUri:          tableUri,
			StorageBackend:    backend,
			RequireTombstones: true,
			RequireFiles:      true,
			Version:           NewDefaultDeltaVersion(),
		},
	}
	for _, o := range opts {
		o(builder)
	}
	return builder, nil
}

type DeltaTableBuilderOption = func(*DeltaTableBuilder)

// Sets `require_tombstones=false` to the builder
func WithoutTombstones() DeltaTableBuilderOption {
	return func(d *DeltaTableBuilder) {
		d.Options.RequireTombstones = false
	}
}

// Sets `require_files=false` to the builder
func WithoutFiles() DeltaTableBuilderOption {
	return func(d *DeltaTableBuilder) {
		d.Options.RequireFiles = false
	}
}

// Sets `version` to the builder
func WithVersion(version DeltaDataTypeVersion) DeltaTableBuilderOption {
	return func(d *DeltaTableBuilder) {
		d.Options.Version.Version = &version
	}
}

// specify the timestamp given as ISO-8601/RFC-3339 timestamp
func WithDatestring(dateString string) (DeltaTableBuilderOption, error) {
	t, err := time.Parse(time.RFC3339, dateString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse time from input '%s': %w", dateString, err)
	}
	return WithTimestamp(t), nil
}

// specify a timestamp
func WithTimestamp(timestamp time.Time) DeltaTableBuilderOption {
	return func(d *DeltaTableBuilder) {
		d.Options.Version.Timestamp = &timestamp
	}
}

// explicitely set a backend (override backend derived from `table_uri`)
func WithStorageBackend(storage storage.StorageBackend) DeltaTableBuilderOption {
	return func(d *DeltaTableBuilder) {
		d.Options.StorageBackend = storage
	}
}

func (b *DeltaTableBuilder) Load() (*DeltaTable, error) {
	config := DeltaTableConfig{
		RequireTombstones: b.Options.RequireTombstones,
		RequireFiles:      b.Options.RequireFiles,
	}

	table, err := NewDeltaTable(b.Options.TableUri, b.Options.StorageBackend, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create DeltaTable: %w", err)
	}

	return table.Load()
}

func NewDeltaTable(tableUri string, storageBackend storage.StorageBackend, config DeltaTableConfig) (*DeltaTable, error) {
	tableUri = storageBackend.TrimPath(tableUri)
	logUriNormalized := storageBackend.JoinPath(tableUri, "_delta_log")

	table := &DeltaTable{
		Version:          -1,
		State:            DeltaTableState{},
		Storage:          storageBackend,
		TableUri:         tableUri,
		Config:           config,
		LastCheckPoint:   nil,
		LogUri:           logUriNormalized,
		VersionTimestamp: make(map[int64]int64),
	}

	return table, nil
}

func (d *DeltaTable) Load() (*DeltaTable, error) {
	d.LastCheckPoint = nil
	d.Version = -1
	d.State = *NewDeltaTableState()
	if err := d.Update(); err != nil {
		return nil, fmt.Errorf("unable to update state: %w", err)
	}

	return d, nil
}

// Updates the DeltaTable to the most recent state committed to the transaction log by
// loading the last checkpoint and incrementally applying each version since.
func (d *DeltaTable) Update() error {
	cp, err := d.GetLastCheckpoint()
	if err != nil {
		return fmt.Errorf("unable to update: %w", err)
	}

	if cp == nil || cp.Equal(d.LastCheckPoint) {
		return d.UpdateIncremental()
	} else {
		d.LastCheckPoint = cp
		if err := d.RestoreCheckPoint(cp); err != nil {
			return fmt.Errorf("unable to restore checkpoint: %w", err)
		}
		d.Version = cp.Version
		return d.UpdateIncremental()
	}
}

func (d *DeltaTable) RestoreCheckPoint(checkpoint *CheckPoint) error {
	state, err := NewDeltaTableStateFromCheckPoint(d, checkpoint)
	if err != nil {
		return fmt.Errorf("unable to restore checkpoint: %w", err)
	}
	d.State = *state
	return nil
}

// Updates the DeltaTable to the latest version by incrementally applying newer versions.
// It assumes that the table is already updated to the current version `self.version`.
func (d *DeltaTable) UpdateIncremental() error {
	for {
		peekCommit, err := d.PeekNextCommit(d.Version)
		if err != nil {
			return fmt.Errorf("unable to peek next commit: %w", err)
		}
		if peekCommit.UpToDate {
			break
		}

		if !peekCommit.UpToDate {
			d.ApplyActions(peekCommit.New.Version, peekCommit.New.Actions)
		}

		if d.Version == -1 {
			return fmt.Errorf("no snapshot or version 0 found, perhaps %s is an empty dir", d.TableUri)
		}
	}

	return nil
}

// Get the list of actions for the next commit
func (d *DeltaTable) PeekNextCommit(currentVersion DeltaDataTypeVersion) (*PeekCommit, error) {
	nextVersion := currentVersion + 1
	commitUri := d.CommitUriFromVersion(nextVersion)
	commitLogBytes, err := d.Storage.GetObj(commitUri)
	if err != nil {
		// TODO: Implement error types in storage so we can apply this logic only if the error is not found
		// Err(StorageError::NotFound) => return Ok(PeekCommit::UpToDate),
		return &PeekCommit{
			UpToDate: true,
		}, nil
	}

	dec := json.NewDecoder(strings.NewReader(string(commitLogBytes)))
	actions := make([]Action, 0)
	for {
		var a Action

		err := dec.Decode(&a)
		if err == io.EOF {
			// all done
			break
		}
		if err != nil {
			return nil, fmt.Errorf("unable to decode commit json: %w", err)
		}
		actions = append(actions, a)
	}
	return &PeekCommit{
		New: struct {
			Version int64
			Actions []Action
		}{
			Version: nextVersion,
			Actions: actions,
		},
	}, nil

}

func (d *DeltaTable) ApplyActions(newVersion DeltaDataTypeVersion, actions []Action) error {
	if d.Version+1 != newVersion {
		return fmt.Errorf("version mismatch, old version is %v, new version is %v", d.Version, newVersion)
	}

	state, err := NewDeltaTableStateFromActions(actions)
	if err != nil {
		return fmt.Errorf("unable to create state from actions: %w", err)
	}

	d.State.Merge(state, d.Config.RequireTombstones, d.Config.RequireFiles)
	d.Version = newVersion

	return nil
}

func (d *DeltaTable) CommitUriFromVersion(version DeltaDataTypeVersion) string {
	v := fmt.Sprintf("%020d.json", version)
	return d.Storage.JoinPath(d.LogUri, v)
}

func (d *DeltaTable) GetLastCheckpoint() (*CheckPoint, error) {
	lastCheckpointPath := d.Storage.JoinPath(d.LogUri, "_last_checkpoint")
	// FIXME: return custom not found error
	data, err := d.Storage.GetObj(lastCheckpointPath)
	if err != nil {
		return nil, nil
	}
	// if err != nil {
	// 	return nil, fmt.Errorf("unable to get last checkpoint: %w", err)
	// }

	var cp CheckPoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("unable to unmarshal checkpoint: %w", err)
	}

	return &cp, nil
}

func (d *DeltaTable) GetCheckPointDataPaths(checkPoint *CheckPoint) []string {
	prefixPattern := fmt.Sprintf("%020d", checkPoint.Version)
	prefix := d.Storage.JoinPath(d.LogUri, prefixPattern)

	if checkPoint.Parts == nil {
		return []string{fmt.Sprintf("%s.checkpoint.parquet", prefix)}
	}

	parts := int(*checkPoint.Parts)
	dataPaths := make([]string, parts)
	for i := 0; i < parts; i++ {
		path := fmt.Sprintf("%s.checkpoint.%010d.%010d.parquet", prefix, i+1, parts)
		dataPaths[i] = path
	}

	return dataPaths
}

// Tries to commit a prepared commit file. Returns `DeltaTableError::VersionAlreadyExists`
// if the given `version` already exists. The caller should handle the retry logic itself.
// This is low-level transaction API. If user does not want to maintain the commit loop then
// the `DeltaTransaction.commit` is desired to be used as it handles `try_commit_transaction`
// with retry logic.
func (d *DeltaTable) TryCommitTransaction(commit PreparedCommit, version DeltaDataTypeVersion) (DeltaDataTypeVersion, error) {
	err := d.Storage.RenameObjNoReplace(commit.Uri, d.CommitUriFromVersion(version))
	if errors.Is(err, &storage.ErrAlreadyExists{}) {
		return 0, &ErrVersionAlreadyExists{
			Inner:   err,
			Version: version,
		}
	} else if err != nil {
		return 0, err
	}

	if err := d.Update(); err != nil {
		return 0, fmt.Errorf("error updating table after transaction")
	}

	return version, nil
}

// Creates a new DeltaTransaction for the DeltaTable.
func (d *DeltaTable) CreateTransaction(options *DeltaTransactionOptions) DeltaTransaction {
	return NewDeltaTransaction(d, options)
}

func NewDeltaTransaction(table *DeltaTable, options *DeltaTransactionOptions) DeltaTransaction {
	var o DeltaTransactionOptions
	if options == nil {
		o = DeltaTransactionOptions{
			MaxRetryCommitAttempts: DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS,
		}
	} else {
		o = *options
	}

	return DeltaTransaction{
		DeltaTable: table,
		Actions:    nil,
		Options:    o,
	}
}

func (tx *DeltaTransaction) AddActions(actions []Action) {
	tx.Actions = append(tx.Actions, actions...)
}

func (tx *DeltaTransaction) Commit(op *DeltaOperation, appMetadata map[string]json.RawMessage) (DeltaDataTypeVersion, error) {
	preparedCommit, err := tx.PrepareCommit(op, appMetadata)
	if err != nil {
		return 0, fmt.Errorf("unable to prepare commit: %w", err)
	}

	version, err := tx.TryCommit(preparedCommit)
	if err != nil {
		return 0, fmt.Errorf("unable to commit: %w", err)
	}

	return version, nil
}

func (tx *DeltaTransaction) PrepareCommit(op *DeltaOperation, appMeta util.RawJsonMap) (PreparedCommit, error) {
	var hasCommit bool
	for _, a := range tx.Actions {
		if a.GetType() == ActionTypeCommitInfo {
			hasCommit = true
		}
	}
	if !hasCommit {
		commitInfo := make(util.RawJsonMap)

		commitInfo.MustUpsert("timestamp", time.Now().UnixMilli())

		if op != nil {
			opInfo := op.GetCommitInfo()
			for k, v := range opInfo {
				commitInfo[k] = v
			}
		}

		for k, v := range appMeta {
			commitInfo[k] = v
		}

		commitAction := Action{
			CommitInfo: &commitInfo,
		}
		tx.Actions = append(tx.Actions, commitAction)
	}

	logEntry, err := LogEntryFromActions(tx.Actions)
	if err != nil {
		return PreparedCommit{}, fmt.Errorf("unable to prepare commit: %w", err)
	}

	token := uuid.New()
	fileName := fmt.Sprintf("_commit_%s.json.tmp", token.String())
	uri := tx.DeltaTable.Storage.JoinPath(tx.DeltaTable.LogUri, fileName)

	if err := tx.DeltaTable.Storage.PutObj(uri, []byte(logEntry)); err != nil {
		return PreparedCommit{}, fmt.Errorf("unable to upload temporary commit: %w", err)
	}

	return PreparedCommit{
		Uri: uri,
	}, nil
}

func LogEntryFromActions(actions []Action) (string, error) {
	lines := make([]string, len(actions))
	for i, a := range actions {
		b, err := json.Marshal(&a)
		if err != nil {
			return "", fmt.Errorf("unable to create log entry from actions: %w", err)
		}
		lines[i] = string(b)
	}
	return strings.Join(lines, "\n"), nil
}

func (tx *DeltaTransaction) TryCommit(commit PreparedCommit) (DeltaDataTypeVersion, error) {
	maxTries := int(tx.Options.MaxRetryCommitAttempts)
	attempt := 0
	for {
		if err := tx.DeltaTable.Update(); err != nil {
			return 0, err
		}

		version := tx.DeltaTable.Version + 1
		newVersion, err := tx.DeltaTable.TryCommitTransaction(commit, version)
		switch {
		case err == nil:
			return newVersion, nil
		case !errors.Is(err, &ErrVersionAlreadyExists{}):
			return 0, fmt.Errorf("error during transaction attempt: %w", err)
		case attempt > maxTries+1:
			return 0, fmt.Errorf("transaction attempt failed, attempts exhausted beyond MaxRetryCommitAttempts")
		}
		attempt += 1
	}

}

// func (d *DeltaTable) LoadVersion(version DeltaDataTypeVersion) (*DeltaTable, error) {
//
// }
//
// func (d *DeltaTable) LoadWithDatetime(ts time.Time) (*DeltaTable, error) {
//
// }

package delta

import (
	"bytes"
	"encoding/json"
	"fmt"

	_ "embed"

	"github.com/thijskoot/delta-go/storage"
	"github.com/thijskoot/delta-go/types"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
)

var (
	//go:embed checkpoint.schema
	checkpointSchema string
)

type TableState struct {
	// A remove action should remain in the state of the table as a tombstone until it has expired.
	// A tombstone expires when the creation timestamp of the delta file exceeds the expiration
	Tombstones               map[string]ActionRemove // HashSet<action::Remove>
	Files                    []ActionAdd
	CommitInfos              []map[string]json.RawMessage
	AppTransactionVersion    map[string]types.Version
	MinReaderVersion         int32
	MinWriterVersion         int32
	CurrentMetadata          *TableMetadata
	TombstoneRetentionMillis types.Long
	LogRetentionMillis       types.Long
	EnableExpiredLogCleanup  bool
	Version                  types.Version
}

func NewTableState() *TableState {
	return &TableState{
		Tombstones:            make(map[string]ActionRemove),
		Files:                 make([]ActionAdd, 0),
		CommitInfos:           make([]map[string]json.RawMessage, 0),
		AppTransactionVersion: make(map[string]int64),
	}
}

func NewTableStateFromActions(actions []Action) (*TableState, error) {
	state := NewTableState()
	for _, a := range actions {
		if err := state.ProcessAction(a, true, true); err != nil {
			return nil, fmt.Errorf("error processing action: %w", err)
		}
	}
	return state, nil
}

func NewTableStateFromCommit(table *Table, committedVersion int64) (*TableState, error) {
	commitUri := storage.CommitUriFromVersion(committedVersion)
	commitLog, err := table.Storage.GetObj(commitUri)
	if err != nil {
		return nil, err
	}

	state := NewTableState()
	state.Version = types.Version(committedVersion)

	for _, l := range bytes.Split([]byte("\n"), commitLog) {
		var a Action

		if err := json.Unmarshal(l, &a); err != nil {
			return nil, fmt.Errorf("error unmarshaling action: %w", err)
		}

		err := state.ProcessAction(a, table.Config.RequireTombstones, table.Config.RequireFiles)
		if err != nil {
			return nil, fmt.Errorf("error processing action: %w", err)
		}
	}

	return state, nil
}

type record struct {
	SparkSchema Action `parquet:"spark_schema"`
}

func newTableStateFromCheckPoint(table *Table, checkPoint *CheckPoint) (*TableState, error) {
	checkPointDataPaths := table.GetCheckPointDataPaths(checkPoint)

	state := NewTableState()
	for _, p := range checkPointDataPaths {
		data, err := table.Storage.GetObj(p)
		if err != nil {
			return nil, fmt.Errorf("unable to get checkpoint data: %w", err)
		}

		pf := buffer.NewBufferFileFromBytes(data)
		pr, err := reader.NewParquetReader(pf, checkpointSchema, 1)
		if err != nil {
			return nil, fmt.Errorf("unable to create parquet reader: %w", err)
		}

		num := int(pr.GetNumRows())
		actions := make([]Action, num)
		if err = pr.Read(&actions); err != nil {
			return nil, fmt.Errorf("unable to unmarshal Action from parquet row: %w", err)
		}

		for _, a := range actions {
			if err := state.ProcessAction(a, table.Config.RequireTombstones, table.Config.RequireFiles); err != nil {
				return nil, fmt.Errorf("unable to process action: %w", err)
			}
		}
	}
	return state, nil
}

func (state *TableState) Merge(newState *TableState, requireTombstones, requireFiles bool) {
	// remove deleted files from new state
	if len(newState.Tombstones) > 0 {
		newFiles := make([]ActionAdd, 0, len(state.Files))
		for _, add := range state.Files {

			if _, isDeleted := newState.Tombstones[add.Path]; !isDeleted {
				newFiles = append(newFiles, add)
			}
		}
		state.Files = newFiles
	}

	if requireTombstones && requireFiles {
		for path, del := range newState.Tombstones {
			state.Tombstones[path] = del
		}

		if len(newState.Files) > 0 {
			for _, add := range newState.Files {
				delete(state.Tombstones, add.Path)
			}
		}
	}

	if requireFiles {
		state.Files = append(state.Files, newState.Files...)
	}

	if newState.MinReaderVersion > 0 {
		state.MinReaderVersion = newState.MinReaderVersion
		state.MinWriterVersion = newState.MinWriterVersion
	}

	if newState.CurrentMetadata != nil {
		state.TombstoneRetentionMillis = newState.TombstoneRetentionMillis
		state.LogRetentionMillis = newState.LogRetentionMillis
		state.EnableExpiredLogCleanup = newState.EnableExpiredLogCleanup
		state.CurrentMetadata = newState.CurrentMetadata
	}

	for appId, version := range newState.AppTransactionVersion {
		state.AppTransactionVersion[appId] = version
	}

	if len(newState.CommitInfos) > 0 {
		state.CommitInfos = append(state.CommitInfos, newState.CommitInfos...)
	}
}

func (state *TableState) ProcessAction(action Action, requireTombstones, requireFiles bool) error {
	switch action.GetType() {
	case ActionTypeAdd:
		if requireFiles {
			if err := action.Add.PathDecoded(); err != nil {
				return err
			}
			state.Files = append(state.Files, *action.Add)
		}
		return nil
	case ActionTypeRemove:
		if requireTombstones && requireFiles {
			if err := action.Remove.PathDecoded(); err != nil {
				return err
			}
			state.Tombstones[action.Remove.Path] = *action.Remove
		}

	case ActionTypeProtocol:
		state.MinReaderVersion = action.Protocol.MinReaderVersion
		state.MinWriterVersion = action.Protocol.MinWriterVersion
	case ActionTypeMetadata:
		md, err := action.MetaData.ToTableMetadata()
		if err != nil {
			return fmt.Errorf("unable to convert action metadata: %w", err)
		}
		tombstoneRentention, err := CONFIG_TOMBSTONE_RETENTION.GetDurationFromMetadata(md)
		if err != nil {
			return fmt.Errorf("unable to parse tombstone retention: %w", err)
		}
		logRentention, err := CONFIG_LOG_RETENTION.GetDurationFromMetadata(md)
		if err != nil {
			return fmt.Errorf("unable to parse log retention: %w", err)
		}
		enableExpiredLogCleanup, err := CONFIG_ENABLE_EXPIRED_LOG_CLEANUP.GetBoolFromMetadata(md)
		if err != nil {
			return fmt.Errorf("unable to parse enable expired log cleanup: %w", err)
		}

		state.TombstoneRetentionMillis = tombstoneRentention.Milliseconds()
		state.LogRetentionMillis = logRentention.Milliseconds()
		state.EnableExpiredLogCleanup = enableExpiredLogCleanup

		state.CurrentMetadata = md
	case ActionTypeTxn:
		state.AppTransactionVersion[*action.Txn.AppId] = *action.Txn.Version
	case ActionTypeCommitInfo:
		state.CommitInfos = append(state.CommitInfos, state.CommitInfos...)
	}
	return nil
}

package delta

import (
	"bytes"
	"fmt"

	_ "embed"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor/interfaces"
)

var (
	//go:embed checkpoint.schema
	checkpointSchema string
)

type DeltaTableState struct {
	// A remove action should remain in the state of the table as a tombstone until it has expired.
	// A tombstone expires when the creation timestamp of the delta file exceeds the expiration
	Tombstones               map[string]Remove // HashSet<action::Remove>
	Files                    []Add
	CommitInfos              []map[string]Value
	AppTransactionVersion    map[string]DeltaDataTypeVersion
	MinReaderVersion         int32
	MinWriterVersion         int32
	CurrentMetadata          *DeltaTableMetaData
	TombstoneRetentionMillis DeltaDataTypeLong
	LogRetentionMillis       DeltaDataTypeLong
	EnableExpiredLogCleanup  bool
}

func NewDeltaTableState() *DeltaTableState {
	return &DeltaTableState{
		Tombstones:            make(map[string]Remove),
		Files:                 make([]Add, 0),
		CommitInfos:           make([]map[string]Value, 0),
		AppTransactionVersion: make(map[string]int64),
	}
}

func NewDeltaTableStateFromActions(actions []Action) (*DeltaTableState, error) {
	state := NewDeltaTableState()
	for _, a := range actions {
		if err := state.ProcessAction(a, true, true); err != nil {
			return nil, fmt.Errorf("error processing action: %w", err)
		}
	}
	return state, nil
}

type record struct {
	SparkSchema Action `parquet:"name=Spark_schema, repetitiontype=REQUIRED"`
}

func NewDeltaTableStateFromCheckPoint(table *DeltaTable, checkPoint *CheckPoint) (*DeltaTableState, error) {
	checkPointDataPaths := table.GetCheckPointDataPaths(checkPoint)

	state := NewDeltaTableState()
	for _, p := range checkPointDataPaths {
		data, err := table.Storage.GetObj(p)
		if err != nil {
			return nil, fmt.Errorf("unable to get checkpoint data: %w", err)
		}

		reader, err := goparquet.NewFileReader(bytes.NewReader(data))
		if err != nil {
			panic(err)
		}

		row, err := reader.NextRow()
		if err != nil {
			panic(err)
		}
		fmt.Printf("%+v\n", row)

		// hlReader := floor.NewReader(reader)
		// records := make([]Spark_schema, reader.NumRows())
		// for i := 0; i < int(reader.NumRows()); i++ {
		// 	hlReader.Next()
		// 	var record Spark_schema
		// 	if err := hlReader.Scan(&record); err != nil {
		// 		panic(err)
		// 	}
		// 	records[i] = record
		// 	fmt.Printf("%+v\n", record)
		//
		// }
		// for hlReader.Next() {
		// records = append(records, record)
		// }

	}
	return state, nil
}

func (a *Action) MarshalParquet(obj interfaces.MarshalObject) error {
	obj.AddField("path")

}

// func NewDeltaTableStateFromCheckPoint(table *DeltaTable, checkPoint *CheckPoint) (*DeltaTableState, error) {
// 	checkPointDataPaths := table.GetCheckPointDataPaths(checkPoint)
//
// 	state := NewDeltaTableState()
// 	for _, p := range checkPointDataPaths {
// 		data, err := table.Storage.GetObj(p)
// 		if err != nil {
// 			return nil, fmt.Errorf("unable to get checkpoint data: %w", err)
// 		}
//
// 		// pf, err := buffer.NewBufferFileFromBytes()
// 		pf := buffer.NewBufferFileFromBytes(data)
// 		// var a source.ParquetFile
// 		// _ = a
//
// 		// r, _ := reader.NewParquetColumnReader(pf, 4)
//
// 		// r.SetSchemaHandlerFromJSON(checkpointSchema)
//
// 		pr, err := reader.NewParquetReader(pf, new(Spark_schema), 4)
// 		if err != nil {
// 			return nil, fmt.Errorf("unable to create parquet reader: %w", err)
// 		}
// 		fmt.Println(pr.Footer.String())
//
// 		num := int(pr.GetNumRows())
// 		// println(num)
// 		actions := make([]Action, num)
// 		records := make([]Spark_schema, num)
// 		// if err = pr.Read(&records); err != nil {
// 		rows, err := pr.ReadByNumber(1)
// 		if err != nil {
// 			panic(err)
// 		}
// 		for _, r := range rows {
// 			fmt.Printf("%+v\n", r)
// 		}
// 		if err = pr.Read(&records); err != nil {
// 			return nil, fmt.Errorf("unable to unmarshal Action from parquet row: %w", err)
// 		}
// 		_ = actions
// 		for _, a := range records {
// 			panic(a)
// 			// state.ProcessAction(a.Spark_schema, table.Config.RequireTombstones, table.Config.RequireFiles)
// 		}
// 	}
// 	return state, nil
// }

func (state *DeltaTableState) Merge(newState *DeltaTableState, requireTombstones, requireFiles bool) {
	// remove deleted files from new state
	if len(newState.Tombstones) > 0 {
		newFiles := make([]Add, 0, len(state.Files))
		for _, add := range state.Files {
			if _, isDeleted := newState.Tombstones[*add.Path]; !isDeleted {
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
				delete(state.Tombstones, *add.Path)
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

func (state *DeltaTableState) ProcessAction(action Action, requireTombstones, requireFiles bool) error {
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
			state.Tombstones[*action.Remove.Path] = *action.Remove
		}

	case ActionTypeProtocol:
		state.MinReaderVersion = action.Protocol.MinReaderVersion
		state.MinWriterVersion = action.Protocol.MinWriterVersion
	case ActionTypeMetadata:
		md, err := action.Metadata.TryConvertToDeltaTableMetaData()
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
		state.AppTransactionVersion[*action.Txn.AppId] = action.Txn.Version
	case ActionTypeCommitInfo:
		state.CommitInfos = append(state.CommitInfos, state.CommitInfos...)
	}
	return nil
}

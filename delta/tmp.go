package delta

type Record struct {
	Spark_schema action `parquet:"Spark_schema"`
}

type action struct {
	Txn        *txn        `parquet:"Txn"`
	Add        *add        `parquet:"Add"`
	Remove     *remove     `parquet:"Remove"`
	MetaData   *metadata   `parquet:"MetaData"`
	Protocol   *protocol   `parquet:"Protocol"`
	CommitInfo *commitInfo `parquet:"CommitInfo"`
}

type txn struct {
	AppId       *string `parquet:"AppId"`
	Version     int64   `parquet:"Version"`
	LastUpdated *int64  `parquet:"LastUpdated"`
}

type add struct {
	Path             *string             `parquet:"Path"`
	PartitionValues  *map[string]*string `parquet:"PartitionValues"`
	Size             int64               `parquet:"Size"`
	ModificationTime int64               `parquet:"ModificationTime"`
	DataChange       bool                `parquet:"DataChange"`
	Stats            *string             `parquet:"Stats"`
	Tags             *map[string]*string `parquet:"Tags"`
}

type remove struct {
	Path              *string `parquet:"Path"`
	DeletionTimestamp *int64  `parquet:"DeletionTimestamp"`
	DataChange        bool    `parquet:"DataChange"`
}

type metadata struct {
	Id               *string             `parquet:"Id"`
	Name             *string             `parquet:"Name"`
	Description      *string             `parquet:"Description"`
	Format           *format             `parquet:"Format"`
	SchemaString     *string             `parquet:"SchemaString"`
	PartitionColumns *[]*string          `parquet:"PartitionColumns"`
	Configuration    *map[string]*string `parquet:"Configuration"`
	CreatedTime      *int64              `parquet:"CreatedTime"`
}

type format struct {
	Provider *string             `parquet:"Provider"`
	Options  *map[string]*string `parquet:"Options"`
}

type protocol struct {
	MinReaderVersion int32 `parquet:"MinReaderVersion"`
	MinWriterVersion int32 `parquet:"MinWriterVersion"`
}

type commitInfo struct {
	Version             *int64              `parquet:"Version"`
	Timestamp           *string             `parquet:"Timestamp"`
	UserId              *string             `parquet:"UserId"`
	UserName            *string             `parquet:"UserName"`
	Operation           *string             `parquet:"Operation"`
	OperationParameters *map[string]*string `parquet:"OperationParameters"`
	Job                 *job                `parquet:"Job"`
	Notebook            *notebook           `parquet:"Notebook"`
	ClusterId           *string             `parquet:"ClusterId"`
	ReadVersion         *int64              `parquet:"ReadVersion"`
	IsolationLevel      *string             `parquet:"IsolationLevel"`
	IsBlindAppend       *bool               `parquet:"IsBlindAppend"`
}

type notebook struct {
	NotebookId *string `parquet:"NotebookId"`
}

type job struct {
	JobId       *string `parquet:"JobId"`
	JobName     *string `parquet:"JobName"`
	RunId       *string `parquet:"RunId"`
	JobOwnerId  *string `parquet:"JobOwnerId"`
	TriggerType *string `parquet:"TriggerType"`
}

// type foo struct {
type Spark_schema struct {
	Txn *struct {
		AppId       *string `parquet:"AppId"`
		Version     *int64  `parquet:"Version"`
		LastUpdated *int64  `parquet:"LastUpdated"`
	} `parquet:"Txn"`
	Add *struct {
		Path             *string             `parquet:"Path"`
		PartitionValues  *map[string]*string `parquet:"PartitionValues"`
		Size             *int64              `parquet:"Size"`
		ModificationTime *int64              `parquet:"ModificationTime"`
		DataChange       *bool               `parquet:"DataChange"`
		Tags             *map[string]*string `parquet:"Tags"`
		Stats            *string             `parquet:"Stats"`
	} `parquet:"Add"`
	Remove *struct {
		Path                 *string             `parquet:"Path"`
		DeletionTimestamp    *int64              `parquet:"DeletionTimestamp"`
		DataChange           *bool               `parquet:"DataChange"`
		ExtendedFileMetadata *bool               `parquet:"ExtendedFileMetadata"`
		PartitionValues      *map[string]*string `parquet:"PartitionValues"`
		Size                 *int64              `parquet:"Size"`
		Tags                 *map[string]*string `parquet:"Tags"`
	} `parquet:"Remove"`
	MetaData *struct {
		Id          *string `parquet:"Id"`
		Name        *string `parquet:"Name"`
		Description *string `parquet:"Description"`
		Format      *struct {
			Provider *string             `parquet:"Provider"`
			Options  *map[string]*string `parquet:"Options"`
		} `parquet:"Format"`
		SchemaString     *string             `parquet:"SchemaString"`
		PartitionColumns *[]*string          `parquet:"PartitionColumns"`
		Configuration    *map[string]*string `parquet:"Configuration"`
		CreatedTime      *int64              `parquet:"CreatedTime"`
	} `parquet:"MetaData"`
	Protocol *struct {
		MinReaderVersion *int32 `parquet:"MinReaderVersion"`
		MinWriterVersion *int32 `parquet:"MinWriterVersion"`
	} `parquet:"Protocol"`
	Cdc *struct {
		Path            *string             `parquet:"Path"`
		PartitionValues *map[string]*string `parquet:"PartitionValues"`
		Size            *int64              `parquet:"Size"`
		Tags            *map[string]*string `parquet:"Tags"`
	} `parquet:"Cdc"`
}

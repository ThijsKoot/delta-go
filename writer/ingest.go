package writer

// import (
// 	"time"
//
// 	kafka "github.com/segmentio/kafka-go"
// 	"github.com/thijskoot/delta-go/delta"
// )
//
// // // The enum to represent 'auto.offset.reset' options.
// // pub enum AutoOffsetReset {
// //     // The "earliest" option. Messages will be ingested from the beginning of a partition on reset.
// //     Earliest,
// //     // The "latest" option. Messages will be ingested from the end of a partition on reset.
// //     Latest,
// // }
// //
// // impl AutoOffsetReset {
// //     // The librdkafka config key used to specify an `auto.offset.reset` policy.
// //     pub const CONFIG_KEY: &'static str = "auto.offset.reset";
// // }
// //
// // Options for configuring the behavior of the run loop executed by the [`start_ingest`] function.
// type IngestOptions struct {
// 	// The Kafka broker string to connect to.
// 	KafkaBrokers string
// 	// The Kafka consumer group id to set to allow for multiple consumers per topic.
// 	ConsumerGroupId string
// 	// Unique per topic per environment. **Must** be the same for all processes that are part of a single job.
// 	// It's used as a prefix for the `txn` actions to track messages offsets between partition/writers.
// 	AppId string
// 	// Offsets to seek to before the ingestion. Creates new delta log version with `txn` actions
// 	// to store the offsets for each partition in delta table.
// 	// Note that `seek_offsets` is not the starting offsets, as such, then first ingested message
// 	// will be `seek_offset + 1` or the next successive message in a partition.
// 	// This configuration is only applied when offsets are not already stored in delta table.
// 	// Note that if offsets are already exists in delta table but they're lower than provided
// 	// then the error will be returned as this could break the data integrity. If one would want to skip
// 	// the data and write from the later offsets then supplying new `app_id` is a safer approach.
// 	SeekOffsets *[]struct {
// 		Partition int64
// 		Offset    int64
// 	}
// 	// pub seek_offsets: Option<Vec<(DataTypePartition, DataTypeOffset)>>,
// 	// The policy to start reading from if both `txn` and `seek_offsets` has no specified offset
// 	// for the partition. Either "earliest" or "latest". The configuration is also applied to the
// 	// librdkafka `auto.offset.reset` config.
// 	AutoOffsetReset interface{}
// 	// Max desired latency from when a message is received to when it is written and
// 	// committed to the target delta table (in seconds)
// 	AllowedLatency uint64
// 	// Number of messages to buffer before writing a record batch.
// 	MaxMessagesPerBatch int
// 	// Desired minimum number of compressed parquet bytes to buffer in memory
// 	// before writing to storage and committing a transaction.
// 	MinBytesPerFile int
// 	// A list of transforms to apply to the message before writing to delta lake.
// 	Transforms map[string]string
// 	// An optional dead letter table to write messages that fail deserialization, transformation or schema validation.
// 	DlqTableUri *string
// 	// Transforms to apply to dead letters when writing to a delta table.
// 	DlqTransforms map[string]string
// 	// If `true` then application will write checkpoints on each 10th commit.
// 	WriteCheckpoints bool
// 	// Additional properties to initialize the Kafka consumer with.
// 	AdditionalKafkaSettings map[string]string
// 	// A statsd endpoint to send statistics to.
// 	StatsdEndpoint string
// }
//
// // Holds state and encapsulates functionality required to process messages and write to delta.
// type IngestProcessor struct {
// 	Topic                 string
// 	Consumer              interface{} // from rdkafka Arc<StreamConsumer<KafkaContext>>,
// 	Transformer           interface{}
// 	CoercionTree          interface{}
// 	Table                 delta.DeltaTable
// 	DeltaWriter           DataWriter
// 	ValueBuffers          interface{}
// 	DeltaPartitionOffsets map[int64]int64
// 	LatencyTimer          time.Time
// 	Dlq                   interface{} // DeadLetterQueue
// 	Opts                  IngestOptions
// 	IngestMetrics         interface{}
// }
//
// func NewIngestProcessor(
// 	topic string,
// 	tableUri string,
// 	consumer interface{},
// 	opts IngestOptions,
// 	ingestMetrics interface{},
// ) (*IngestProcessor, error) {
// 	panic("not implemented")
// }
//
// func (ip *IngestProcessor) WriteOffsetsToDeltaIfAny() error {
// 	panic("not implemented")
// }
//
// func (ip *IngestProcessor) ProcessMessage() error {
// 	var foo kafka.Message
// }
//
// // func (ip *IngestProcessor)

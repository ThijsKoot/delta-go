package operations

import "github.com/thijskoot/delta-go/delta"

// Command for writing data into Delta table
type WriteCommand struct {
	TableUri string
	// The save mode used in operation
	Mode delta.SaveMode
	// Column names for table partitioning
	PartitionColumns *[]string
	// When using `Overwrite` mode, replace data that matches a predicate
	Predicate *string
	// Schema of data to be written to disk
	Schema interface{} // arrow::datatypes::SchemaRef as ArrowSchemaRef,
	// The input plan
	Input interface{} //Arc<dyn ExecutionPlan>
}

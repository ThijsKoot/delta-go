package operations

import "github.com/thijskoot/delta-go/delta"

type DeltaTransactionPlan struct {
	TableUri     string
	TableVersion delta.DeltaDataTypeVersion
	Input        interface{} //*ExecutionPlan
	Operation    delta.DeltaOperation
	AppMetadata  *map[string]interface{}
}


package operations

import (
	"github.com/thijskoot/delta-go/delta"
	"github.com/thijskoot/delta-go/types"
)

type DeltaTransactionPlan struct {
	TableUri     string
	TableVersion types.Version
	Input        interface{} //*ExecutionPlan
	Operation    delta.DeltaOperation
	AppMetadata  *map[string]interface{}
}


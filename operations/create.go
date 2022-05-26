package operations

import (
	"github.com/thijskoot/delta-go/delta"
)

type CreateCommand struct {
	TableUri string
	Mode     delta.SaveMode
	Metadata delta.DeltaTableMetaData
	Protocol delta.Protocol
}

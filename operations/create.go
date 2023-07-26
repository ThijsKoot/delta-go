package operations

import (
	"github.com/thijskoot/delta-go"
)

type CreateCommand struct {
	TableUri string
	Mode     delta.SaveMode
	Metadata delta.TableMetadata
	Protocol delta.ActionProtocol
}

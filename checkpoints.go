package delta

type SchemaPath = []string

func (c *CheckPoint) Equal(other *CheckPoint) bool {
	return c != nil && other != nil &&
		c.Version == other.Version &&
		c.Parts == other.Parts &&
		c.Size == other.Size

}

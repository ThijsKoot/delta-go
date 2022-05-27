package delta

type SchemaPath = []string

func (self *CheckPoint) Equal(other *CheckPoint) bool {
	return self != nil && other != nil &&
		self.Version == other.Version &&
		self.Parts == other.Parts &&
		self.Size == other.Size

}

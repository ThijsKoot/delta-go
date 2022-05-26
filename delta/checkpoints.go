package delta

type SchemaPath = []string

func (self *CheckPoint) Equal(other *CheckPoint) bool {
	return self == other &&
		self.Version == other.Version &&
		self.Parts == other.Parts &&
		self.Size == other.Size

}

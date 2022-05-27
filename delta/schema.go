package delta

type Schema struct {
	Type   string        `json:"type"`
	Fields []SchemaField `json:"fields"`
}

type SchemaDataType struct {
	Primitive *PrimitiveDataType
	Struct    *Schema `json:"struct"`
	Array     *SchemaTypeArray
}

type SchemaTypeArray struct {
	// type field is always the string "array", so we are ignoring it here
	Type string
	// The type of element stored in this array represented as a string containing the name of a
	// primitive type, a struct definition, an array definition or a map definition
	ElementType SchemaDataType
	// Boolean denoting whether this array can contain one or more null values
	ContainsNull bool
}

const (
	PrimitiveString    = "utf8"
	PrimitiveLong      = "int64"
	PrimitiveInteger   = "int32"
	PrimitiveShort     = "int16"
	PrimitiveByte      = "int8"
	PrimitiveFloat     = "float32"
	PrimitiveDouble    = "float64"
	PrimitiveBoolean   = "bool"
	PrimitiveBinary    = "bytes"
	PrimitiveDate      = "date"
	PrimitiveTimestamp = "timestamp"
)

type PrimitiveDataType string

type SchemaTypeMap struct {
	Type              string          `json:"type"`
	KeyType           *SchemaDataType `json:"keyType,omitempty"`
	ValueType         *SchemaDataType `json:"valueType,omitempty"`
	ValueContainsNull bool            `json:"valueContainsNull,omitempty"`
}

type SchemaField struct {
	// Name of this (possibly nested) column
	Name        string            `json:"name"`
	Type        PrimitiveDataType `json:"type"`
	ElementType *SchemaField       `json:"elementType,omitempty"`
	// Boolean denoting whether this field can be null
	Nullable bool          `json:"nullable"`
	Fields   *[]SchemaField `json:"fields,omitempty"`
	// A JSON map containing information about this column. Keys prefixed with Delta are reserved
	// for the implementation.
	Metadata map[string]string `json:"metadata"`
}

type Value struct {
	// Represents a JSON null value.
	Null bool

	// Represents a JSON boolean.
	Bool bool

	// Represents a JSON number, whether integer or floating point.
	Number int

	// Represents a JSON string.
	String string

	// Represents a JSON array.
	Array []Value

	// Represents a JSON object.
	Object map[string]Value
}

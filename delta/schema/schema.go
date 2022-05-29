package schema

import "github.com/apache/arrow/go/arrow"

type DataType string

const (
	DataTypeString    DataType = "string"
	DataTypeLong      DataType = "long"
	DataTypeInteger   DataType = "integer"
	DataTypeShort     DataType = "short"
	DataTypeByte      DataType = "byte"
	DataTypeFloat     DataType = "float"
	DataTypeDouble    DataType = "double"
	DataTypeBoolean   DataType = "bool"
	DataTypeBinary    DataType = "bytes"
	DataTypeDate      DataType = "date"
	DataTypeTimestamp DataType = "timestamp"
	DataTypeArray     DataType = "array"
	DataTypeStruct    DataType = "struct"
	DataTypeMap       DataType = "map"
)

type Schema = SchemaTypeStruct

type SchemaDataType struct {
	Primitive *DataType
	Struct    *SchemaTypeStruct
	Array     *SchemaTypeArray
	Map       *SchemaTypeMap
}

type SchemaTypeStruct struct {
	Type   string        `json:"type"`
	Fields []SchemaField `json:"fields"`
}

type SchemaTypeMap struct {
	Type              string          `json:"type"`
	KeyType           *SchemaDataType `json:"keyType,omitempty"`
	ValueType         *SchemaDataType `json:"valueType,omitempty"`
	ValueContainsNull bool            `json:"valueContainsNull,omitempty"`
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

type SchemaField struct {
	// Name of this (possibly nested) column
	Name string
	Type SchemaDataType
	// Boolean denoting whether this field can be null
	Nullable bool `json:"nullable"`
	// A JSON map containing information about this column. Keys prefixed with Delta are reserved
	// for the implementation.
	Metadata map[string]string `json:"metadata"`
}

func NewPrimitiveType(dataType DataType) SchemaDataType {
	return SchemaDataType{
		Primitive: &dataType,
	}
}

func NewMapType(keyType, valueType SchemaDataType, valueContainsNull bool) SchemaDataType {
	return SchemaDataType{
		Map: &SchemaTypeMap{
			Type:              "map",
			KeyType:           &keyType,
			ValueType:         &valueType,
			ValueContainsNull: valueContainsNull,
		},
	}

}

func NewStructType(fields []SchemaField) SchemaDataType {
	return SchemaDataType{
		Struct: &SchemaTypeStruct{
			Type:   "struct",
			Fields: fields,
		},
	}
}

func NewArrayType(elementType SchemaDataType, containsNull bool) SchemaDataType {
	return SchemaDataType{
		Array: &SchemaTypeArray{
			Type:         "array",
			ElementType:  elementType,
			ContainsNull: containsNull,
		},
	}
}

func (s *Schema) ToArrowSchema() arrow.Schema {
	for _, f := range s.Fields {
		arrow.StructOf()
		af := arrow.Field{
			Name: f.Name,
			Type: &arrow.StructType{},
		}
		_ = af

		af = arrow.Field{
			Name: f.Name,
			Type: &arrow.StringType{},
		}

	}
	return arrow.Schema{}
}

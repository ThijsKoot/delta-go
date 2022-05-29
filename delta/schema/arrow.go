package schema

import (
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
)

func (sdt *SchemaDataType) AsArrowType() arrow.DataType {
	if sdt.Primitive != nil {
		return sdt.Primitive.AsArrowType()
	}

	if sdt.Array != nil {
		elementType := sdt.Array.ElementType.AsArrowType()
		return arrow.ListOf(elementType)
	}

	if sdt.Struct != nil {
		arrowFields := make([]arrow.Field, len(sdt.Struct.Fields))
		for i := range sdt.Struct.Fields {
			f := sdt.Struct.Fields[i]
			af := f.AsArrowField()
			arrowFields = append(arrowFields, af)
		}
		return arrow.StructOf(arrowFields...)
	}

	if sdt.Map != nil {
		keyType := sdt.Map.KeyType.AsArrowType()
		valueType := sdt.Map.ValueType.AsArrowType()
		return arrow.MapOf(keyType, valueType)
	}
	panic("can't convert SchemaDataType to Arrow type")
}

func (sf *SchemaField) AsArrowField() arrow.Field {
	return arrow.Field{
		Name:     sf.Name,
		Nullable: sf.Nullable,
		Metadata: arrow.MetadataFrom(sf.Metadata),
		Type:     sf.Type.AsArrowType(),
	}
}

func (d *DataType) AsArrowType() arrow.DataType {
	switch *d {
	case DataTypeString:
		return &arrow.StringType{}
	case DataTypeLong:
		return &arrow.Int64Type{}
	case DataTypeInteger:
		return &arrow.Int32Type{}
	case DataTypeShort:
		return &arrow.Int16Type{}
	case DataTypeByte:
		return &arrow.Int8Type{}
	case DataTypeFloat:
		return &arrow.Float32Type{}
	case DataTypeDouble:
		return &arrow.Float64Type{}
	case DataTypeBoolean:
		return &arrow.BooleanType{}
	case DataTypeBinary:
		return &arrow.BinaryType{}
	case DataTypeDate:
		return &arrow.Date32Type{}
	case DataTypeTimestamp:
		return &arrow.TimestampType{}
	default:
		panic(fmt.Sprintf("no arrow datatype known for %s", *d))
	}
}

func (s *Schema) ToArrowSchema() *arrow.Schema {
	fields := make([]arrow.Field, len(s.Fields))

	for i := range s.Fields {
		fields[i] = s.Fields[i].AsArrowField()
	}

	return arrow.NewSchema(fields, &arrow.Metadata{})
}

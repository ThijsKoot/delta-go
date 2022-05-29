package schema

import (
	"encoding/json"
	"fmt"
)

type rawSchema struct {
	Type   string           `json:"type"`
	Fields []rawSchemaField `json:"fields"`
}

type rawSchemaField struct {
	// Name of this (possibly nested) column
	Name        *string          `json:"name,omitempty"`
	Type        json.RawMessage  `json:"type"`
	ElementType *json.RawMessage `json:"elementType,omitempty"`
	// Boolean denoting whether this field can be null
	Nullable *bool             `json:"nullable,omitempty"`
	Fields   *[]rawSchemaField `json:"fields,omitempty"`
	// A JSON map containing information about this column. Keys prefixed with Delta are reserved
	// for the implementation.
	Metadata *map[string]string `json:"metadata,omitempty"`

	KeyType   *json.RawMessage `json:"keyType,omitempty"`
	ValueType *json.RawMessage `json:"valueType,omitempty"`

	ContainsNull *bool `json:"containsNull,omitempty"`

	ValueContainsNull *bool `json:"valueContainsNull,omitempty"`
}

func (r *rawSchemaField) asSchemaField() (SchemaField, error) {
	var dt SchemaDataType
	if err := json.Unmarshal(r.Type, &dt); err != nil {
		return SchemaField{}, fmt.Errorf("error unmarshaling field type: %w", err)
	}

	result := SchemaField{
		Name:     *r.Name,
		Nullable: *r.Nullable,
		Metadata: *r.Metadata,
		Type:     dt,
	}

	return result, nil
}

func (s *SchemaField) asRawSchemaField() (rawSchemaField, error) {
	result := rawSchemaField{
		Name:     &s.Name,
		Metadata: &s.Metadata,
		Nullable: &s.Nullable,
		// Type: ,
	}

	t, err := s.Type.MarshalJSON()
	if err != nil {
		return result, fmt.Errorf("unable to convert SchemaField to rawSchemaField: %w", err)
	}

	result.Type = t

	return result, nil
}

func (s *Schema) MarshalJSON() ([]byte, error) {
	raw := rawSchema{
		Type:   s.Type,
		Fields: make([]rawSchemaField, len(s.Fields)),
	}

	for i := range s.Fields {
		f := s.Fields[i]
		rf, err := f.asRawSchemaField()
		if err != nil {
			return nil, fmt.Errorf("unable to convert field to rawSchemaField: %w", err)
		}

		if rf.Metadata == nil {
			md := make(map[string]string)
			rf.Metadata = &md
		}

		raw.Fields[i] = rf
	}

	res, err := json.Marshal(&raw)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal rawSchema: %w", err)
	}

	return res, nil
}

func (s *Schema) UnmarshalJSON(data []byte) error {
	var raw rawSchema
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("error unmarshaling rawSchema: %w", err)
	}

	s.Type = raw.Type

	s.Fields = make([]SchemaField, len(raw.Fields))
	for i, f := range raw.Fields {
		field, err := f.asSchemaField()
		if err != nil {
			return fmt.Errorf("error converting rawSchemaField to SchemaField: %w", err)
		}

		s.Fields[i] = field
	}
	return nil
}

func (sdt *SchemaDataType) UnmarshalJSON(data []byte) error {
	var primitive DataType
	if err := json.Unmarshal(data, &primitive); err == nil {
		sdt.Primitive = &primitive
		return nil
	}

	var rawField rawSchemaField
	if err := json.Unmarshal(data, &rawField); err != nil {
		return fmt.Errorf("error unmarshaling raw schema field: %w", err)
	}

	if rawField.ElementType != nil {
		var et SchemaDataType
		if err := json.Unmarshal(*rawField.ElementType, &et); err != nil {
			return fmt.Errorf("error unmarshaling elementType: %w", err)

		}
		sdt.Array = &SchemaTypeArray{
			Type:         "array",
			ContainsNull: *rawField.ContainsNull,
			ElementType:  et,
		}
	}
	if rawField.Fields != nil {
		fields := make([]SchemaField, len(*rawField.Fields))
		for i, rf := range *rawField.Fields {
			var fieldType SchemaDataType
			if err := json.Unmarshal(rf.Type, &fieldType); err != nil {
				return fmt.Errorf("error unmarshaling struct field: %w", err)
			}
			f := SchemaField{
				Name:     *rf.Name,
				Type:     fieldType,
				Nullable: *rf.Nullable,
				Metadata: *rf.Metadata,
			}
			fields[i] = f
		}
		sdt.Struct = &SchemaTypeStruct{
			Type:   "struct",
			Fields: fields,
		}
	}
	if rawField.KeyType != nil {
		var keyType SchemaDataType
		var valueType SchemaDataType
		if err := json.Unmarshal(*rawField.KeyType, &keyType); err != nil {
			return fmt.Errorf("error unmarshaling keyType: %w", err)
		}
		if err := json.Unmarshal(*rawField.ValueType, &valueType); err != nil {
			return fmt.Errorf("error unmarshaling valueType: %w", err)
		}
		sdt.Map = &SchemaTypeMap{
			Type:              "map",
			KeyType:           &keyType,
			ValueType:         &valueType,
			ValueContainsNull: *rawField.ValueContainsNull,
		}
	}
	return nil
}

func (sdt *SchemaDataType) MarshalJSON() ([]byte, error) {
	if sdt.Primitive != nil {
		b, err := json.Marshal(sdt.Primitive)
		if err != nil {
			return b, fmt.Errorf("unable to marshal primitive type: %w", err)
		}
		return b, nil
	}

	var rawField rawSchemaField

	if sdt.Array != nil {
		et, err := json.Marshal(&sdt.Array.ElementType)
		if err != nil {
			return nil, fmt.Errorf("unable to marshal element type: %w", err)
		}
		etr := json.RawMessage(et)

		rawField = rawSchemaField{
			Type:         []byte(`"array"`),
			ElementType:  &etr,
			ContainsNull: &sdt.Array.ContainsNull,
		}
	}

	if sdt.Map != nil {
		kt, err := json.Marshal(&sdt.Map.KeyType)
		if err != nil {
			return nil, fmt.Errorf("unable to marshal key type: %w", err)
		}
		vt, err := json.Marshal(&sdt.Map.ValueType)
		if err != nil {
			return nil, fmt.Errorf("unable to marshal value type: %w", err)
		}

		ktr := json.RawMessage(kt)
		vtr := json.RawMessage(vt)

		rawField = rawSchemaField{
			Type:              []byte(`"map"`),
			KeyType:           &ktr,
			ValueType:         &vtr,
			ValueContainsNull: &sdt.Map.ValueContainsNull,
		}
	}

	if sdt.Struct != nil {
		rawFields := make([]rawSchemaField, len(sdt.Struct.Fields))
		for i := range sdt.Struct.Fields {
			r, err := sdt.Struct.Fields[i].asRawSchemaField()
			if err != nil {
				return nil, fmt.Errorf("unable to convert struct field to rawSchemaField: %w", err)
			}
			rawFields[i] = r
		}

		rawField = rawSchemaField{
			Type:   []byte(`"struct"`),
			Fields: &rawFields,
		}
	}
	res, err := json.Marshal(&rawField)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal rawSchemaField: %w", err)
	}
	return res, nil
}

package schema

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

const schemaJson = `
{
  "type": "struct",
  "fields": [
    {
      "name": "num",
      "type": "integer",
      "nullable": false,
      "metadata": {}
    },
    {
      "name": "str",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "map",
      "type": {
        "type": "map",
        "keyType": "string",
        "valueType": {
          "type": "map",
          "keyType": "integer",
          "valueType": "double",
          "valueContainsNull": false
        },
        "valueContainsNull": true
      },
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "foo",
      "type": {
        "type": "struct",
        "fields": [
          {
            "name": "a",
            "type": "string",
            "nullable": true,
            "metadata": {}
          }
        ]
      },
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "arr",
      "type": {
        "type": "array",
        "elementType": "integer",
        "containsNull": true
      },
      "nullable": true,
      "metadata": {}
    }
  ]
}
`

var schema = Schema{
	Type: "struct",
	Fields: []SchemaField{
		{
			Name:     "num",
			Type:     NewPrimitiveType(DataTypeInteger),
			Nullable: false,
			Metadata: make(map[string]string),
		},
		{
			Name:     "str",
			Type:     NewPrimitiveType(DataTypeString),
			Nullable: true,
			Metadata: make(map[string]string),
		},
		{
			Name: "map",
			Type: NewMapType(
				NewPrimitiveType(DataTypeString),
				NewMapType(
					NewPrimitiveType(DataTypeInteger),
					NewPrimitiveType(DataTypeDouble),
					false),
				true),
			Nullable: true,
			Metadata: make(map[string]string),
		},
		{
			Name: "foo",
			Type: NewStructType([]SchemaField{
				{
					Name:     "a",
					Type:     NewPrimitiveType(DataTypeString),
					Nullable: true,
					Metadata: make(map[string]string),
				}}),
			Nullable: true,
			Metadata: make(map[string]string),
		},
		{
			Name:     "arr",
			Type:     NewArrayType(NewPrimitiveType(DataTypeInteger), true),
			Nullable: true,
			Metadata: make(map[string]string),
		},
	},
}

func Test_SchemaUnmarshalJSON(t *testing.T) {
	var result Schema
	err := json.Unmarshal([]byte(schemaJson), &result)
	assert.NoError(t, err)

	assert.Equal(t, schema, result)
}

func Test_SchemaMarshalJSON(t *testing.T) {
	var expected bytes.Buffer
	err := json.Compact(&expected, []byte(schemaJson))

	assert.NoError(t, err)

	result, err := json.Marshal(&schema)

	assert.NoError(t, err)
	assert.Equal(t, expected.Bytes(), result)
}

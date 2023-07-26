package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/thijskoot/delta-go"
	"github.com/thijskoot/delta-go/schema"
	"github.com/thijskoot/delta-go/writer"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
	"k8s.io/utils/pointer"
)

func main() {
	path := os.Args[1]
	bucket, err := blob.OpenBucket(context.Background(), fmt.Sprintf("file:///%s", path))
	if err != nil {
		log.Fatalf("unable to create bucket: %e", err)
	}

	t, err := delta.OpenTable("mytable", bucket)
	if err != nil {
		log.Fatalf("could not load table: %e", err)
	}

	s := schema.Schema{
		Type: string(schema.DataTypeStruct),
		Fields: []schema.SchemaField{
			{Name: "a", Type: schema.NewPrimitiveType(schema.DataTypeString)},
			{Name: "b", Type: schema.NewPrimitiveType(schema.DataTypeInteger)},
		},
	}

	sb, err := s.MarshalJSON()
	if err != nil {
		log.Fatal("error marshaling json", err)
	}

	actions := []delta.Action{
		{
			Protocol: &delta.ActionProtocol{
				MinReaderVersion: 1,
				MinWriterVersion: 2,
			},
		},
		{
			MetaData: &delta.ActionMetadata{
				CreatedTime: pointer.Int64(time.Now().UnixMilli()),
				Name:        pointer.String("mytable"),
				Format: delta.Format{
					Provider: "parquet",
				},
				SchemaString: string(sb),
			},
		},
	}

	tx := t.CreateTransaction(nil)
	tx.AddActions(actions)
	newVersion, err := tx.Commit(nil, nil)
	if err != nil {
		log.Fatal("error committing", err)
	}

	fmt.Println("new version:", newVersion)

	w, err := writer.NewJsonWriterForTable(t)
	if err != nil {
		panic(err)
	}

	data := []json.RawMessage{
		[]byte(`{"a": "foo", "b": 1}`),
		[]byte(`{"a": "bar", "b": 2}`),
	}
	if err := w.Write(data); err != nil {
		log.Fatal("unable to write", err)
	}

	if _, err = w.FlushAndCommit(); err != nil {
		log.Fatal("unable to flush", err)
	}

	fmt.Printf("Table version: %v\n", t.Version)
}

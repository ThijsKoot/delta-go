package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"github.com/apache/arrow/go/v9/parquet"
	"github.com/apache/arrow/go/v9/parquet/pqarrow"
	"github.com/thijskoot/delta-go/delta"
	"github.com/thijskoot/delta-go/writer"
	"gocloud.dev/blob"
)

func main() {
	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("unable to determine current working directory: %e", err)
	}

	println("hi")
	bucket, err := blob.OpenBucket(context.Background(), fmt.Sprintf("file://%s", wd))
	if err != nil {
		log.Fatalf("unable to create bucket: %e", err)
	}

	t := os.Args[1]
	table, err := delta.OpenTable(t, bucket)
	check(err)

	output, err := os.Create("/tmp/ids.txt")
	check(err)
	defer output.Close()


	for _, x := range table.State.Files {
		// fmt.Println(*x.Path)

		content, err := table.Storage.GetObj(path.Join(t, x.Path))
		check(err)
		reader := bytes.NewReader(content)

		// f, err := file.OpenParquetFile(*x.Path, true)
		// check(err)
		// pqreader, err := file.NewParquetReader(reader)
		// check(err)
		//
		// _ = pqreader
		// defer pqreader.Close()

		tbl, err := pqarrow.ReadTable(
			context.Background(),
			reader,
			parquet.NewReaderProperties(memory.DefaultAllocator),
			pqarrow.ArrowReadProperties{Parallel: true},
			memory.DefaultAllocator,
		)
		check(err)


		indices := tbl.Schema().FieldIndices("registrationId")
		if len(indices) != 1 {
			panic(fmt.Sprintf("indices for field should be 1, is: %v", len(indices)))
		}

		idx := indices[0]

		tr := array.NewTableReader(tbl, 0)
		for tr.Next() {
			rec := tr.Record()
			c := rec.Column(idx)
			arr, ok := c.(*array.String)
			if !ok {
				panic("c is not string")
			}
			for i := 0; i < arr.Len(); i++ {
				v := arr.Value(i)
				_, err := fmt.Fprintln(output, v)
				check(err)
			}
		}
	}

	fmt.Println(table.Version)

	jsonWriter, err := writer.NewJsonWriterForTable(table)
	check(err)

	records := []json.RawMessage{[]byte(`{"id": 10000}`)}
	err = jsonWriter.Write(records)
	check(err)
	ver, err := jsonWriter.FlushAndCommit()
	check(err)
	println(ver)
}

func foo() {

}

func check(err error) {
	if err != nil {
		panic(err.Error())
	}
}

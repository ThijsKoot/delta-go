package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/thijskoot/delta-go/delta"
	"github.com/thijskoot/delta-go/writer"
)

func main() {
	t := os.Args[1]
	table, err := delta.OpenTable(t)
	check(err)

	_ = table
	for _, x := range table.State.Files {
		fmt.Println(*x.Path)
	}

	fmt.Println(table.Version)

	jsonWriter, err := writer.NewJsonWriterForTable(table)
	check(err)

	records := []json.RawMessage{[]byte(`{"id": 10000}`)}
	err = jsonWriter.Write(records)
	check(err)
	ver, err := jsonWriter.FlushAndCommit(*table)
	check(err)
	println(ver)
}

func check(err error) {
	if err != nil {
		panic(err.Error())
	}
}

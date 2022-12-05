package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/thijskoot/delta-go/delta"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
)

func main() {
	tableUri := os.Args[1]

	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("unable to determine current working directory: %e", err)
	}

	bucket, err := blob.OpenBucket(context.Background(), fmt.Sprintf("file://%s", wd))
	if err != nil {
		log.Fatalf("unable to create bucket: %e", err)
	}

	t, err := delta.OpenTable(tableUri, bucket)
	if err != nil {
		log.Fatalf("could not load table: %e", err)
	}

	for _, f := range t.State.Files {
		println(f.Path)
	}

	for _, f := range t.State.Tombstones {
		println(f.Path)
	}

	fmt.Printf("Table version: %v\n", t.Version)
}

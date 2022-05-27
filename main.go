package main

import (
	"fmt"
	"os"

	"github.com/thijskoot/delta-go/delta"
)

func main() {
	t := os.Args[1]
	table, err := delta.OpenTable(t)
	check(err)

	_ = table
	for _, x := range table.State.Files {
		fmt.Println(*x.Path)
	}

	for _, x := range table.State.Tombstones {
		fmt.Println("Tombstone:", *x.Path)
	}
}

func check(err error) {
	if err != nil {
		panic(err.Error())
	}
}

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

	fmt.Printf("%+v\n", table)
}

func check(err error) {
	if err != nil {
		panic(err.Error())
	}
}

// Copyright (C) 2019 zhvala.
// Use of this source code is governed by an MIT-style license
// that can be found in the LICENSE file.

package consistent_test

import (
	"fmt"
	"log"

	consistent "github.com/zhvala/goconsistent"
)

func ExampleNew() {
	c := consistent.New()
	c.Add("keyA", "valueA")
	c.Add("keyB", "valueB")
	c.Add("keyC", "valueC")
	users := []string{"raw-1", "raw-2", "raw-3", "raw-4", "raw-5"}
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server.Value)
	}
	// Output:
	// raw-1 => valueC
	// raw-2 => valueA
	// raw-3 => valueA
	// raw-4 => valueC
	// raw-5 => valueC
}

func ExampleAdd() {
	c := consistent.New()
	c.Add("keyA", "valueA")
	c.Add("keyB", "valueB")
	c.Add("keyC", "valueC")
	users := []string{"raw-1", "raw-2", "raw-3", "raw-4", "raw-5"}
	fmt.Println("initial state [A, B, C]")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server.Key)
	}
	c.Add("keyD", "valueE")
	c.Add("keyE", "valueE")
	fmt.Println("\nwith cacheD, cacheE [A, B, C, D, E]")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server.Key)
	}
}

func ExampleRemove() {
	c := consistent.New()
	c.Add("keyA", "valueA")
	c.Add("keyB", "valueB")
	c.Add("keyC", "valueC")
	users := []string{"raw-1", "raw-2", "raw-3", "raw-4", "raw-5"}
	fmt.Println("initial state [A, B, C]")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server.Key)
	}
	c.Remove("keyC")
	fmt.Println("\ncacheC removed [A, B]")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server.Key)
	}
}

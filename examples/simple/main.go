package main

import (
	"fmt"
	"github.com/QuangTung97/go-memcache/memcache"
)

func simpleGetAndSet(client *memcache.Client) {
	pipeline := client.Pipeline()
	defer pipeline.Finish()

	err := pipeline.FlushAll()()
	fmt.Println("Flush Error:", err)

	//======================================
	// Multi Get in a pipeline with 2 get
	//======================================
	fn1 := pipeline.MGet("KEY01", memcache.MGetOptions{})
	fn2 := pipeline.MGet("KEY02", memcache.MGetOptions{})

	getResp, err := fn1()
	fmt.Printf("GET: %+v %+v\n", getResp, err)

	getResp, err = fn2()
	fmt.Printf("GET: %+v %+v\n", getResp, err)

	//======================================
	// Multi Set
	//======================================
	setFn1 := pipeline.MSet("KEY01", []byte("key data 01"), memcache.MSetOptions{})
	setFn2 := pipeline.MSet("KEY02", []byte("key data 02"), memcache.MSetOptions{})

	setResp, err := setFn1()
	fmt.Printf("SET: %+v %+v\n", setResp, err)

	setResp, err = setFn2()
	fmt.Printf("SET: %+v %+v\n", setResp, err)

	//======================================
	// Multi Get Again in a Pipeline, Returns CAS
	//======================================
	fn1 = pipeline.MGet("KEY01", memcache.MGetOptions{CAS: true})
	fn2 = pipeline.MGet("KEY02", memcache.MGetOptions{CAS: true})

	getResp, err = fn1()
	fmt.Printf("GET: %+v %+v\n", getResp, err)

	getResp, err = fn2()
	fmt.Printf("GET: %+v %+v\n", getResp, err)
}

func main() {
	client, err := memcache.New("localhost:11211", 8)
	if err != nil {
		panic(err)
	}

	// Simple Get And Set
	simpleGetAndSet(client)
}

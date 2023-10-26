# High Performance Memcached Client

[![go-memcache](https://github.com/QuangTung97/go-memcache/actions/workflows/go.yml/badge.svg)](https://github.com/QuangTung97/go-memcache/actions/workflows/go.yml)
[![Coverage Status](https://coveralls.io/repos/github/QuangTung97/go-memcache/badge.svg?branch=master)](https://coveralls.io/github/QuangTung97/go-memcache?branch=master)

### Why this Library?

A Simple Memcached Client Library.

Similar to: https://github.com/bradfitz/gomemcache. \
But faster (up to 2x in some benchmarks) and supporting pipelined DELETE & SET operations

And mostly focused on the new meta commands:
https://github.com/memcached/memcached/wiki/MetaCommands \
To facilitate more complex caching algorithms (e.g. memcached lease).

It implemented using Batching & Pipelining to reduce syscalls,
batching can happen between clients of the same connection.

Please checkout this document for better understanding of request options and response values of this library:
https://github.com/memcached/memcached/blob/master/doc/protocol.txt

### Examples

Examples can be found here: https://github.com/QuangTung97/go-memcache/tree/master/examples

```go
pipeline := client.Pipeline()
defer pipeline.Finish()

fn1 := pipeline.MGet("KEY01", memcache.MGetOptions{})
fn2 := pipeline.MGet("KEY02", memcache.MGetOptions{})

getResp, err := fn1()
fmt.Printf("GET: %+v %+v\n", getResp, err)

getResp, err = fn2()
fmt.Printf("GET: %+v %+v\n", getResp, err)
```

In this example, two meta get commands will be sent only in the call:

```go
getResp, err := fn1()
```

It will try to batch as much number of commands
as possible to the underlining TCP Connections.

But if you do like this:

```go
pipeline := client.Pipeline()
defer pipeline.Finish()

fn1 := pipeline.MGet("KEY01", memcache.MGetOptions{})
getResp, err := fn1()
fmt.Printf("GET: %+v %+v\n", getResp, err)

fn2 := pipeline.MGet("KEY02", memcache.MGetOptions{})
getResp, err = fn2()
fmt.Printf("GET: %+v %+v\n", getResp, err)
```

Then no batching is possible.

The line `defer pipeline.Finish()` is the best practise for preventing cases like this:

```go
pipeline := client.Pipeline()
defer pipeline.Finish()

pipeline.MSet("KEY01", []byte("key data 01"), memcache.MSetOptions{})
pipeline.MSet("KEY02", []byte("key data 02"), memcache.MSetOptions{})
```

without `pipeline.Finish()` the two set commands will **NOT** be delivered to the memcached server. \
Because the returned functions should be called, ``pipeline.Finsh()`` will do the calling if one forgotten.

```
fn := pipeline.MSet("KEY01", []byte("key data 01"), memcache.MSetOptions{})
_, _ = fn()
```

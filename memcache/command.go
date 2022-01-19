package memcache

type commandType int

const (
	commandTypeMGet commandType = iota + 1
	commandTypeMSet
	commandTypeMDel
	commandTypeFlushAll
)

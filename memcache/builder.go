package memcache

import "sync"

var cmdDataPool = sync.Pool{
	New: func() interface{} {
		c := newCommand()
		c.data = make([]byte, 0, 256)
		return c
	},
}

type cmdBuilder struct {
	cmd *commandData
}

// MGetOptions ...
type MGetOptions struct {
	N   uint32 // option N of mg command
	CAS bool
}

// MSetOptions ...
type MSetOptions struct {
	CAS uint64
}

// MDelOptions ...
type MDelOptions struct {
	CAS uint64
	I   bool
}

func freeCommandData(cmd *commandData) {
	cmdDataPool.Put(cmd)
}

func initCmdBuilder(b *cmdBuilder) {
	b.cmd = cmdDataPool.Get().(*commandData)
	b.cmd.data = b.cmd.data[:0]
	b.cmd.cmdCount = 0
	b.cmd.completed = false
	b.cmd.resetReader = false
	b.cmd.lastErr = nil
}

func appendNumber(data []byte, n uint64) []byte {
	index := len(data)
	k := 0
	for n > 0 {
		d := n % 10
		data = append(data, byte(d)+'0')
		n /= 10
		k++
	}
	for offset := 0; offset < k/2; offset++ {
		i := index + offset
		j := index + k - 1 - offset
		data[i], data[j] = data[j], data[i]
	}
	return data
}

func (b *cmdBuilder) addMGet(key string, opts MGetOptions) {
	b.cmd.cmdCount++

	b.cmd.data = append(b.cmd.data, "mg "...)
	b.cmd.data = append(b.cmd.data, key...)

	if opts.CAS {
		b.cmd.data = append(b.cmd.data, " c"...)
	}

	if opts.N > 0 {
		b.cmd.data = append(b.cmd.data, " N"...)
		b.cmd.data = appendNumber(b.cmd.data, uint64(opts.N))
	}

	b.cmd.data = append(b.cmd.data, " v\r\n"...)
}

func (b *cmdBuilder) addMSet(key string, data []byte, opts MSetOptions) {
	b.cmd.cmdCount++

	b.cmd.data = append(b.cmd.data, "ms "...)
	b.cmd.data = append(b.cmd.data, key...)
	b.cmd.data = append(b.cmd.data, ' ')

	b.cmd.data = appendNumber(b.cmd.data, uint64(len(data)))
	if opts.CAS > 0 {
		b.cmd.data = append(b.cmd.data, " C"...)
		b.cmd.data = appendNumber(b.cmd.data, opts.CAS)
	}

	b.cmd.data = append(b.cmd.data, "\r\n"...)

	b.cmd.data = append(b.cmd.data, data...)
	b.cmd.data = append(b.cmd.data, "\r\n"...)
}

func (b *cmdBuilder) addMDel(key string, opts MDelOptions) {
	b.cmd.cmdCount++

	b.cmd.data = append(b.cmd.data, "md "...)
	b.cmd.data = append(b.cmd.data, key...)

	if opts.CAS > 0 {
		b.cmd.data = append(b.cmd.data, " C"...)
		b.cmd.data = appendNumber(b.cmd.data, opts.CAS)
	}

	if opts.I {
		b.cmd.data = append(b.cmd.data, " I"...)
	}

	b.cmd.data = append(b.cmd.data, "\r\n"...)
}

func (b *cmdBuilder) getCmd() *commandData {
	return b.cmd
}

package memcache

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
	TTL uint32
}

// MDelOptions ...
type MDelOptions struct {
	CAS uint64
	I   bool   // set as stale instead of delete completely
	TTL uint32 // only apply if I = true
}

func initCmdBuilder(b *cmdBuilder) {
	b.cmd = newCommand()
}

func appendNumber(data []byte, n uint64) []byte {
	if n == 0 {
		data = append(data, '0')
		return data
	}

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

	b.cmd.requestData = append(b.cmd.requestData, "mg "...)
	b.cmd.requestData = append(b.cmd.requestData, key...)

	if opts.CAS {
		b.cmd.requestData = append(b.cmd.requestData, " c"...)
	}

	if opts.N > 0 {
		b.cmd.requestData = append(b.cmd.requestData, " N"...)
		b.cmd.requestData = appendNumber(b.cmd.requestData, uint64(opts.N))
	}

	b.cmd.requestData = append(b.cmd.requestData, " v\r\n"...)
}

func (b *cmdBuilder) addMSet(key string, data []byte, opts MSetOptions) {
	b.cmd.cmdCount++

	b.cmd.requestData = append(b.cmd.requestData, "ms "...)
	b.cmd.requestData = append(b.cmd.requestData, key...)
	b.cmd.requestData = append(b.cmd.requestData, ' ')

	b.cmd.requestData = appendNumber(b.cmd.requestData, uint64(len(data)))
	if opts.CAS > 0 {
		b.cmd.requestData = append(b.cmd.requestData, " C"...)
		b.cmd.requestData = appendNumber(b.cmd.requestData, opts.CAS)
	}

	if opts.TTL > 0 {
		b.cmd.requestData = append(b.cmd.requestData, " T"...)
		b.cmd.requestData = appendNumber(b.cmd.requestData, uint64(opts.TTL))
	}

	b.cmd.requestData = append(b.cmd.requestData, "\r\n"...)

	b.cmd.requestData = append(b.cmd.requestData, data...)
	b.cmd.requestData = append(b.cmd.requestData, "\r\n"...)
}

func (b *cmdBuilder) addMDel(key string, opts MDelOptions) {
	b.cmd.cmdCount++

	b.cmd.requestData = append(b.cmd.requestData, "md "...)
	b.cmd.requestData = append(b.cmd.requestData, key...)

	if opts.CAS > 0 {
		b.cmd.requestData = append(b.cmd.requestData, " C"...)
		b.cmd.requestData = appendNumber(b.cmd.requestData, opts.CAS)
	}

	if opts.I {
		b.cmd.requestData = append(b.cmd.requestData, " I"...)
		if opts.TTL > 0 {
			b.cmd.requestData = append(b.cmd.requestData, " T"...)
			b.cmd.requestData = appendNumber(b.cmd.requestData, uint64(opts.TTL))
		}
	}

	b.cmd.requestData = append(b.cmd.requestData, "\r\n"...)
}

func (b *cmdBuilder) addFlushAll() {
	b.cmd.cmdCount++
	b.cmd.requestData = append(b.cmd.requestData, "flush_all\r\n"...)
}

func (b *cmdBuilder) getCmd() *commandData {
	return b.cmd
}

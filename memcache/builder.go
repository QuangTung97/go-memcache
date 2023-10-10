package memcache

type cmdBuilder struct {
	cmd         *commandData
	cmdList     *commandData
	lastPointer **commandData

	lastRequestEntry **requestBinaryEntry

	maxCmdCount int
	mgetCount   int
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

func initCmdBuilder(b *cmdBuilder, maxCmdCount int) {
	b.lastPointer = &b.cmdList

	b.addNewCommand()

	b.mgetCount = 0
	b.maxCmdCount = maxCmdCount
}

func (b *cmdBuilder) addNewCommand() {
	b.cmd = newCommand()
	*b.lastPointer = b.cmd
	b.lastPointer = &b.cmd.sibling
	b.lastRequestEntry = &b.cmd.requestBinaries
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

func (b *cmdBuilder) internalIncreaseCount() {
	if b.cmd.cmdCount >= b.maxCmdCount {
		b.internalResetMGetCount()
		b.addNewCommand()
	}
	b.cmd.cmdCount++
}

func (b *cmdBuilder) addMGet(key string, opts MGetOptions) {
	b.internalIncreaseCount()
	b.mgetCount++

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
	b.internalIncreaseCount()

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

	dataLen := uint64(len(data))
	binaryData := getByteSlice(dataLen + 2)
	copy(binaryData, data)
	copy(binaryData[dataLen:], "\r\n")

	reqEntry := &requestBinaryEntry{
		offset: len(b.cmd.requestData),
		data:   binaryData,
	}
	*b.lastRequestEntry = reqEntry
	b.lastRequestEntry = &reqEntry.next
}

func (b *cmdBuilder) addMDel(key string, opts MDelOptions) {
	b.internalIncreaseCount()

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
	b.internalIncreaseCount()
	b.cmd.requestData = append(b.cmd.requestData, "flush_all\r\n"...)
}

func (b *cmdBuilder) getCurrentCommandForTest() *commandData {
	return b.cmd
}

func (b *cmdBuilder) getCommandList() *commandData {
	return b.cmdList
}

func (b *cmdBuilder) clearCmd() {
	b.cmd = nil
	b.cmdList = nil
}

func (b *cmdBuilder) internalResetMGetCount() {
	b.cmd.responseBinaries = make([][]byte, 0, b.mgetCount)
	b.mgetCount = 0
}

func (b *cmdBuilder) finish() *commandData {
	b.internalResetMGetCount()
	return b.cmdList
}

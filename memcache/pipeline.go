package memcache

import "errors"

// ErrAlreadyGotten ...
var ErrAlreadyGotten = errors.New("pipeline error: already gotten")

type pipelineCmd struct {
	published bool
	cmdType   commandType
	resp      interface{}
	err       error

	isRead bool
}

// Pipeline ...
type Pipeline struct {
	builder cmdBuilder

	currentCmdList []*pipelineCmd

	c *clientConn
}

// Pipeline creates a pipeline
func (c *Client) Pipeline() *Pipeline {
	p := &Pipeline{
		c:              c.getNextConn(),
		currentCmdList: make([]*pipelineCmd, 0, 32),
	}
	initCmdBuilder(&p.builder)
	return p
}

func (p *Pipeline) waitAndParseCmdData() {
	currentCmd := p.builder.getCmd()
	currentCmd.waitCompleted()

	var ps parser
	initParser(&ps, currentCmd.data)

	for _, cmd := range p.currentCmdList {
		if currentCmd.lastErr != nil {
			cmd.err = currentCmd.lastErr
			continue
		}

		switch cmd.cmdType {
		case commandTypeMGet:
			cmd.resp, cmd.err = ps.readMGet()

		case commandTypeMSet:
			cmd.resp, cmd.err = ps.readMSet()

		case commandTypeMDel:
			cmd.resp, cmd.err = ps.readMDel()

		case commandTypeFlushAll:
			cmd.err = ps.readFlushAll()

		default:
			panic("invalid cmd type")
		}
	}

	// clear currentCmdList
	for i := range p.currentCmdList {
		p.currentCmdList[i] = nil
	}
	p.currentCmdList = p.currentCmdList[:0]

	freeCommandData(currentCmd)
}

func (p *Pipeline) resetCmdBuilder() {
	initCmdBuilder(&p.builder)
}

func (p *Pipeline) pushAndWaitImpl() {
	p.c.pushCommand(p.builder.getCmd())

	// Set all of published = true
	for _, cmd := range p.currentCmdList {
		cmd.published = true
	}

	p.waitAndParseCmdData()
}

func (p *Pipeline) pushAndWaitResponses(cmd *pipelineCmd) {
	if cmd.published {
		return
	}
	p.pushAndWaitImpl()
	p.resetCmdBuilder()
}

func (p *Pipeline) addCommand(cmdType commandType) *pipelineCmd {
	cmd := &pipelineCmd{
		cmdType: cmdType,
	}
	p.currentCmdList = append(p.currentCmdList, cmd)
	return cmd
}

// Finish ...
func (p *Pipeline) Finish() {
	if len(p.currentCmdList) > 0 {
		p.pushAndWaitImpl()
	}
}

func (p *Pipeline) pushAndWaitIfNotRead(cmd *pipelineCmd) error {
	if cmd.isRead {
		return ErrAlreadyGotten
	}
	cmd.isRead = true
	p.pushAndWaitResponses(cmd)
	return nil
}

// MGet ...
func (p *Pipeline) MGet(key string, opts MGetOptions) func() (MGetResponse, error) {
	cmd := p.addCommand(commandTypeMGet)

	p.builder.addMGet(key, opts)

	return func() (MGetResponse, error) {
		err := p.pushAndWaitIfNotRead(cmd)
		if err != nil {
			return MGetResponse{}, err
		}
		resp, ok := cmd.resp.(MGetResponse)
		if !ok {
			return MGetResponse{}, cmd.err
		}
		return resp, nil
	}
}

// MSet ...
func (p *Pipeline) MSet(key string, value []byte, opts MSetOptions) func() (MSetResponse, error) {
	cmd := p.addCommand(commandTypeMSet)

	p.builder.addMSet(key, value, opts)

	return func() (MSetResponse, error) {
		err := p.pushAndWaitIfNotRead(cmd)
		if err != nil {
			return MSetResponse{}, err
		}
		resp, ok := cmd.resp.(MSetResponse)
		if !ok {
			return MSetResponse{}, cmd.err
		}
		return resp, nil
	}
}

// MDel ...
func (p *Pipeline) MDel(key string, opts MDelOptions) func() (MDelResponse, error) {
	cmd := p.addCommand(commandTypeMDel)

	p.builder.addMDel(key, opts)

	return func() (MDelResponse, error) {
		err := p.pushAndWaitIfNotRead(cmd)
		if err != nil {
			return MDelResponse{}, err
		}
		resp, ok := cmd.resp.(MDelResponse)
		if !ok {
			return MDelResponse{}, cmd.err
		}
		return resp, nil
	}
}

// FlushAll ...
func (p *Pipeline) FlushAll() func() error {
	cmd := p.addCommand(commandTypeFlushAll)

	p.builder.addFlushAll()

	return func() error {
		err := p.pushAndWaitIfNotRead(cmd)
		if err != nil {
			return err
		}
		return cmd.err
	}
}

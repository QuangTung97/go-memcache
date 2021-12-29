package memcache

import "errors"

// ErrAlreadyGotten ...
var ErrAlreadyGotten = errors.New("pipeline error: already gotten")

type pipelineCmd struct {
	published bool
	cmdType   commandType
	resp      interface{}
	err       error
}

// Pipeline ...
type Pipeline struct {
	builder cmdBuilder

	runningCmdMap map[int]*pipelineCmd
	nextKey       int
	buildingKeys  []int

	currentCmdKeys []int

	c *conn
}

// Pipeline creates a pipeline
func (c *Client) Pipeline() *Pipeline {
	p := &Pipeline{
		c:             c.getNextConn(),
		runningCmdMap: map[int]*pipelineCmd{},
		buildingKeys:  make([]int, 0, 32),
		nextKey:       0,
	}
	initCmdBuilder(&p.builder)
	return p
}

func (p *Pipeline) waitAndParseCmdData() {
	currentCmd := p.builder.getCmd()
	currentCmd.waitCompleted()

	var ps parser
	initParser(&ps, currentCmd.data)

	for _, key := range p.currentCmdKeys {
		cmd := p.runningCmdMap[key]

		if currentCmd.lastErr != nil {
			cmd.err = currentCmd.lastErr
			continue
		}

		switch cmd.cmdType {
		case commandTypeMGet:
			cmd.resp, cmd.err = ps.readMGet()

		case commandTypeMSet:
			cmd.resp, cmd.err = ps.readMSet()

		default:
			panic("invalid cmd type")
		}
	}
}

func (p *Pipeline) resetBuilderAndCmdData() {
	initCmdBuilder(&p.builder)
	p.currentCmdKeys = p.currentCmdKeys[:0]
}

func (p *Pipeline) pushAndWaitImpl() {
	p.c.pushCommand(p.builder.getCmd())

	// Set all of published = true
	for _, key := range p.buildingKeys {
		c := p.runningCmdMap[key]
		c.published = true
	}
	p.buildingKeys = p.buildingKeys[:0]

	p.waitAndParseCmdData()
}

func (p *Pipeline) pushAndWaitResponses(cmd *pipelineCmd) {
	if cmd.published {
		return
	}
	p.pushAndWaitImpl()
	p.resetBuilderAndCmdData()
}

func (p *Pipeline) addCommand(cmdType commandType) int {
	cmd := &pipelineCmd{
		cmdType: cmdType,
	}
	p.nextKey++
	keyIndex := p.nextKey

	p.buildingKeys = append(p.buildingKeys, keyIndex)
	p.currentCmdKeys = append(p.currentCmdKeys, keyIndex)

	p.runningCmdMap[keyIndex] = cmd

	return keyIndex
}

// Finish ...
func (p *Pipeline) Finish() {
	if len(p.buildingKeys) > 0 {
		p.pushAndWaitImpl()
	}
}

func (p *Pipeline) getCommand(keyIndex int) (*pipelineCmd, error) {
	cmd, ok := p.runningCmdMap[keyIndex]
	if !ok {
		return nil, ErrAlreadyGotten
	}
	p.pushAndWaitResponses(cmd)
	delete(p.runningCmdMap, keyIndex)
	return cmd, nil
}

// MGet ...
func (p *Pipeline) MGet(key string, opts MGetOptions) func() (MGetResponse, error) {
	keyIndex := p.addCommand(commandTypeMGet)

	p.builder.addMGet(key, opts)

	return func() (MGetResponse, error) {
		cmd, err := p.getCommand(keyIndex)
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
	keyIndex := p.addCommand(commandTypeMSet)

	p.builder.addMSet(key, value, opts)

	return func() (MSetResponse, error) {
		cmd, err := p.getCommand(keyIndex)
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

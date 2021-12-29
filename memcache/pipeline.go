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

type pipelineCommandData struct {
	cmd  *commandData
	keys []int
}

// Pipeline ...
type Pipeline struct {
	builder   cmdBuilder
	published bool

	runningCmdMap map[int]*pipelineCmd
	nextKey       int
	buildingKeys  []int

	cmdDataCurrent int
	cmdDataSet     map[int]*pipelineCommandData

	c *conn
}

// Pipeline creates a pipeline
func (c *Client) Pipeline() *Pipeline {
	p := &Pipeline{
		c:             c.getNextConn(),
		runningCmdMap: map[int]*pipelineCmd{},
		buildingKeys:  make([]int, 0, 32),
		nextKey:       0,

		cmdDataCurrent: 0,
	}
	initCmdBuilder(&p.builder)
	return p
}

func (p *Pipeline) waitAndParseCmdData() {
	current := p.cmdDataSet[p.cmdDataCurrent]
	current.cmd.waitCompleted()

	var ps parser
	initParser(&ps, current.cmd.data)

	for _, key := range current.keys {
		cmd := p.runningCmdMap[key]

		switch cmd.cmdType {
		case commandTypeMGet:
			if current.cmd.lastErr != nil {
				cmd.err = current.cmd.lastErr
				continue
			}
			resp, err := ps.readMGet()
			cmd.resp = resp
			cmd.err = err

		default:
			panic("invalid cmd type")
		}
	}
}

func (p *Pipeline) resetBuilderAndCmdData() {
	initCmdBuilder(&p.builder)

	p.cmdDataCurrent++
	p.cmdDataSet[p.cmdDataCurrent] = &pipelineCommandData{
		cmd:  p.builder.getCmd(),
		keys: make([]int, 32),
	}
}

func (p *Pipeline) pushAndWaitImpl() {
	p.c.pushCommand(p.builder.getCmd())
	p.waitAndParseCmdData()

	// Set all of published = true
	for _, key := range p.buildingKeys {
		c, ok := p.runningCmdMap[key]
		if !ok {
			continue
		}
		c.published = true
	}
	p.buildingKeys = p.buildingKeys[:0]
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

	p.runningCmdMap[keyIndex] = cmd

	return keyIndex
}

// Finish ...
func (p *Pipeline) Finish() {
	p.pushAndWaitImpl()
}

// MGet ...
func (p *Pipeline) MGet(key string, opts MGetOptions) func() (MGetResponse, error) {
	keyIndex := p.addCommand(commandTypeMGet)

	p.builder.addMGet(key, opts)

	return func() (MGetResponse, error) {
		cmd, ok := p.runningCmdMap[keyIndex]
		if !ok {
			return MGetResponse{}, ErrAlreadyGotten
		}
		p.pushAndWaitResponses(cmd)
		return cmd.resp.(MGetResponse), cmd.err
	}
}

package memcache

type pipelineCmd struct {
	cmdType commandType
	resp    interface{}
	err     error
}

// Pipeline ...
type Pipeline struct {
	builder cmdBuilder
	cmdList []pipelineCmd
	c       *conn
}

// Pipeline creates a pipeline
func (c *Client) Pipeline() *Pipeline {
	p := &Pipeline{
		c:       c.getNextConn(),
		cmdList: make([]pipelineCmd, 0, 32),
	}
	initCmdBuilder(&p.builder)
	return p
}

// MGet ...
func (p *Pipeline) MGet(_ string, _ MGetOptions) func() (MGetResponse, error) {
	return func() (MGetResponse, error) {
		return MGetResponse{}, nil
	}
}

package memcache

import (
	"errors"
	"unicode"
)

// ErrAlreadyGotten ...
var ErrAlreadyGotten = errors.New("pipeline error: already gotten")

type pipelineSession struct {
	published bool
	doWaited  bool

	currentCmd     *commandData
	currentCmdList []*pipelineCmd
}

type pipelineCmd struct {
	sess *pipelineSession

	cmdType commandType
	resp    interface{}
	err     error

	isRead bool
}

// Pipeline should NOT be used concurrently
type Pipeline struct {
	builderInstance cmdBuilder
	currentSession  *pipelineSession

	c *clientConn
}

func newPipelineSession(currentCmd *commandData) *pipelineSession {
	return &pipelineSession{
		published: false,
		doWaited:  false,

		currentCmd:     currentCmd,
		currentCmdList: make([]*pipelineCmd, 0, 32),
	}
}

func newPipelineCmd(sess *pipelineSession, cmdType commandType) *pipelineCmd {
	return &pipelineCmd{
		sess:    sess,
		cmdType: cmdType,
	}
}

// Pipeline creates a pipeline
func (c *Client) Pipeline() *Pipeline {
	p := &Pipeline{
		c:              c.getNextConn(),
		currentSession: nil,
	}
	return p
}

func (s *pipelineSession) waitAndParseCmdData() {
	if s.doWaited {
		return
	}
	s.doWaited = true

	s.currentCmd.waitCompleted()

	var ps parser
	initParser(&ps, s.currentCmd.data)

	for _, cmd := range s.currentCmdList {
		if s.currentCmd.lastErr != nil {
			cmd.err = s.currentCmd.lastErr
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

	freeCommandData(s.currentCmd)
}

func (p *Pipeline) getBuilder() *cmdBuilder {
	return &p.builderInstance
}

func (p *Pipeline) resetPipelineSession() {
	p.currentSession = nil
}

func (p *Pipeline) getCurrentSession() *pipelineSession {
	if p.currentSession == nil {
		initCmdBuilder(&p.builderInstance)
		cmd := p.builderInstance.getCmd()
		p.currentSession = newPipelineSession(cmd)
	}
	return p.currentSession
}

func (p *Pipeline) pushCommands() {
	currentCmd := p.getBuilder().getCmd()
	p.c.pushCommand(currentCmd)

	p.resetPipelineSession()
}

func (p *Pipeline) pushCommandsIfNotPublished(sess *pipelineSession) {
	if !sess.published {
		sess.published = true
		p.pushCommands()
	}
}

func (p *Pipeline) pushAndWaitResponses(cmd *pipelineCmd) {
	sess := cmd.sess
	p.pushCommandsIfNotPublished(sess)
	sess.waitAndParseCmdData()
}

func (p *Pipeline) addCommand(cmdType commandType) *pipelineCmd {
	sess := p.getCurrentSession()
	cmd := newPipelineCmd(sess, cmdType)
	sess.currentCmdList = append(sess.currentCmdList, cmd)
	return cmd
}

// Finish ...
func (p *Pipeline) Finish() {
	if p.currentSession != nil {
		sess := p.currentSession
		p.pushCommandsIfNotPublished(sess)
		sess.waitAndParseCmdData()
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
	if err := validateKeyFormat(key); err != nil {
		return func() (MGetResponse, error) {
			return MGetResponse{}, err
		}
	}

	cmd := p.addCommand(commandTypeMGet)

	p.getBuilder().addMGet(key, opts)

	return func() (MGetResponse, error) {
		err := p.pushAndWaitIfNotRead(cmd)
		if err != nil {
			return MGetResponse{}, err
		}
		resp, ok := cmd.resp.(MGetResponse)
		if !ok {
			return MGetResponse{}, cmd.err
		}
		return resp, cmd.err
	}
}

// MSet ...
func (p *Pipeline) MSet(key string, value []byte, opts MSetOptions) func() (MSetResponse, error) {
	if err := validateKeyFormat(key); err != nil {
		return func() (MSetResponse, error) {
			return MSetResponse{}, err
		}
	}

	cmd := p.addCommand(commandTypeMSet)

	p.getBuilder().addMSet(key, value, opts)

	return func() (MSetResponse, error) {
		err := p.pushAndWaitIfNotRead(cmd)
		if err != nil {
			return MSetResponse{}, err
		}
		resp, ok := cmd.resp.(MSetResponse)
		if !ok {
			return MSetResponse{}, cmd.err
		}
		return resp, cmd.err
	}
}

// MDel ...
func (p *Pipeline) MDel(key string, opts MDelOptions) func() (MDelResponse, error) {
	if err := validateKeyFormat(key); err != nil {
		return func() (MDelResponse, error) {
			return MDelResponse{}, err
		}
	}

	cmd := p.addCommand(commandTypeMDel)

	p.getBuilder().addMDel(key, opts)

	return func() (MDelResponse, error) {
		err := p.pushAndWaitIfNotRead(cmd)
		if err != nil {
			return MDelResponse{}, err
		}
		resp, ok := cmd.resp.(MDelResponse)
		if !ok {
			return MDelResponse{}, cmd.err
		}
		return resp, cmd.err
	}
}

// Execute flush operations to memcached (interrupts pipelining)
func (p *Pipeline) Execute() {
	if p.currentSession != nil {
		p.pushCommandsIfNotPublished(p.currentSession)
	}
}

// FlushAll ...
func (p *Pipeline) FlushAll() func() error {
	cmd := p.addCommand(commandTypeFlushAll)

	p.getBuilder().addFlushAll()

	return func() error {
		err := p.pushAndWaitIfNotRead(cmd)
		if err != nil {
			return err
		}
		return cmd.err
	}
}

// ErrInvalidKeyFormat ...
var ErrInvalidKeyFormat = errors.New("memcached: invalid key format")

func validateKeyFormat(key string) error {
	for _, r := range key {
		if unicode.IsControl(r) {
			return ErrInvalidKeyFormat
		}
	}
	return nil
}

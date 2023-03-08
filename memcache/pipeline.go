package memcache

import (
	"errors"
	"io"
	"net"
	"os"
	"unicode"
)

// ErrAlreadyGotten ...
var ErrAlreadyGotten = errors.New("pipeline error: already gotten")

type pipelineSession struct {
	pipeline *Pipeline

	builder cmdBuilder

	published     bool
	alreadyWaited bool

	currentCmdList []*pipelineCmd
}

type pipelineCmd struct {
	sess *pipelineSession

	cmdType commandType
	resp    any
	err     error

	isRead bool
}

// Pipeline should NOT be used concurrently
type Pipeline struct {
	currentSession *pipelineSession

	c *clientConn
}

func (p *Pipeline) newPipelineSession() *pipelineSession {
	sess := &pipelineSession{
		pipeline: p,

		published:     false,
		alreadyWaited: false,

		currentCmdList: make([]*pipelineCmd, 0, 32),
	}
	initCmdBuilder(&sess.builder)
	return sess
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

func (s *pipelineSession) parseCommands(currentCmd *commandData) error {
	var ps parser
	initParser(&ps, currentCmd.responseData)

	if currentCmd.lastErr != nil {
		for _, cmd := range s.currentCmdList {
			cmd.err = currentCmd.lastErr
		}
		return currentCmd.lastErr
	}

	for _, cmd := range s.currentCmdList {
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

	return nil
}

func (s *pipelineSession) doRetryForError(currentCmd *commandData, err error) {
	if err == nil {
		return
	}

	if errors.Is(err, os.ErrDeadlineExceeded) {
		return
	}

	var netErr *net.OpError
	if err != io.EOF && !errors.As(err, &netErr) {
		return
	}

	// Do Retry
	err = s.pipeline.c.core.sender.waitForNewEpoch(currentCmd.epoch)
	if err != nil {
		return
	}

	retryCmd := currentCmd.cloneShareRequest()

	s.pushCommands(retryCmd)
	retryCmd.waitCompleted()

	_ = s.parseCommands(retryCmd)
}

func (s *pipelineSession) waitAndParseCmdData() {
	if s.alreadyWaited {
		return
	}
	s.alreadyWaited = true
	s.pipeline.resetPipelineSession()

	currentCmd := s.builder.getCmd()
	currentCmd.waitCompleted()

	err := s.parseCommands(currentCmd)
	s.doRetryForError(currentCmd, err)

	// clear currentCmdList
	freeCommandData(currentCmd)
}

func (p *Pipeline) resetPipelineSession() {
	p.currentSession = nil
}

func (p *Pipeline) getCurrentSession() *pipelineSession {
	if p.currentSession == nil {
		p.currentSession = p.newPipelineSession()
	} else if p.currentSession.published {
		p.currentSession.waitAndParseCmdData()
		p.currentSession = p.newPipelineSession()
	}
	return p.currentSession
}

func (s *pipelineSession) pushCommands(cmd *commandData) {
	pipe := s.pipeline
	pipe.c.pushCommand(cmd)
}

func (s *pipelineSession) pushCommandsIfNotPublished() {
	if !s.published {
		s.published = true
		s.pushCommands(s.builder.getCmd())
	}
}

func (c *pipelineCmd) pushAndWaitResponses() {
	sess := c.sess
	sess.pushCommandsIfNotPublished()
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
		sess.pushCommandsIfNotPublished()
		sess.waitAndParseCmdData()
	}
}

func (p *Pipeline) pushAndWaitIfNotRead(cmd *pipelineCmd) error {
	if cmd.isRead {
		return ErrAlreadyGotten
	}
	cmd.isRead = true
	cmd.pushAndWaitResponses()
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
	cmd.sess.builder.addMGet(key, opts)

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
	cmd.sess.builder.addMSet(key, value, opts)

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

	cmd.sess.builder.addMDel(key, opts)

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
		p.currentSession.pushCommandsIfNotPublished()
	}
}

// FlushAll ...
func (p *Pipeline) FlushAll() func() error {
	cmd := p.addCommand(commandTypeFlushAll)
	cmd.sess.builder.addFlushAll()

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

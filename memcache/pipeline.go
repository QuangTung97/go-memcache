package memcache

import (
	"errors"
	"unicode"
	"unsafe"
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
	resp    unsafe.Pointer
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
	initCmdBuilder(&sess.builder, p.c.maxCommandsPerBatch)
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

func (s *pipelineSession) parseCommands(currentCmd *commandData) {
	var ps parser
	initParser(&ps, currentCmd.responseData, currentCmd.responseBinaries)

	if currentCmd.lastErr != nil {
		for _, cmd := range s.currentCmdList {
			cmd.err = currentCmd.lastErr
		}
		return
	}

	for _, cmd := range s.currentCmdList {
		switch cmd.cmdType {
		case commandTypeMGet:
			resp, err := ps.readMGet()
			cmd.resp, cmd.err = unsafe.Pointer(&resp), err

		case commandTypeMSet:
			resp, err := ps.readMSet()
			cmd.resp, cmd.err = unsafe.Pointer(&resp), err

		case commandTypeMDel:
			resp, err := ps.readMDel()
			cmd.resp, cmd.err = unsafe.Pointer(&resp), err

		case commandTypeFlushAll:
			cmd.err = ps.readFlushAll()

		default:
			panic("invalid cmd type")
		}

		if cmd.err != nil {
			if IsServerError(cmd.err) {
				continue
			}
			cmd.err = currentCmd.conn.setLastErrorAndClose(cmd.err)
		}
	}
}

func (s *pipelineSession) waitAndParseCmdData() {
	if s.alreadyWaited {
		return
	}
	s.alreadyWaited = true
	s.pipeline.resetPipelineSession()

	currentCmd := s.builder.getCmd()
	currentCmd.waitCompleted()

	s.parseCommands(currentCmd)

	// clear currentCmdList
	freeCommandResponseData(currentCmd)
	s.builder.clearCmd()
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

		builderCmd := s.builder.finish()
		s.pushCommands(builderCmd)
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
		resp := (*MGetResponse)(cmd.resp)
		if resp == nil {
			return MGetResponse{}, cmd.err
		}
		return *resp, cmd.err
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
		resp := (*MSetResponse)(cmd.resp)
		if resp == nil {
			return MSetResponse{}, cmd.err
		}
		return *resp, cmd.err
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
		resp := (*MDelResponse)(cmd.resp)
		if resp == nil {
			return MDelResponse{}, cmd.err
		}
		return *resp, cmd.err
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

// ErrKeyEmpty ...
var ErrKeyEmpty = errors.New("memcached: key is empty")

// ErrKeyTooLong ...
var ErrKeyTooLong = errors.New("memcached: key too long")

// for testing
var enabledCheckLen = true

func validateKeyFormat(key string) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	for _, r := range key {
		if r > unicode.MaxASCII {
			return ErrInvalidKeyFormat
		}
		if unicode.IsControl(r) {
			return ErrInvalidKeyFormat
		}
		if unicode.IsSpace(r) {
			return ErrInvalidKeyFormat
		}
	}
	if enabledCheckLen && len(key) > 250 {
		return ErrKeyTooLong
	}
	return nil
}

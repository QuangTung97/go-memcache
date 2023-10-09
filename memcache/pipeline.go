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

	currentCmdList []pipelineCmd
}

type pipelineCmd struct {
	cmdType commandType

	getResp MGetResponse

	resp unsafe.Pointer
	err  error

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

		currentCmdList: make([]pipelineCmd, 0, 32),
	}
	initCmdBuilder(&sess.builder, p.c.maxCommandsPerBatch)
	return sess
}

func newPipelineCmd(cmdType commandType) pipelineCmd {
	return pipelineCmd{
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

func (s *pipelineSession) parseCommands(cmdList *commandData) {
	pipeCmds := s.currentCmdList
	for current := cmdList; current != nil; current = current.sibling {
		n := current.cmdCount
		parseCommandsForSingleCommandData(pipeCmds[:n], current)
		pipeCmds = pipeCmds[n:]
	}
}

func parseCommandsForSingleCommandData(
	pipelineCommands []pipelineCmd,
	currentCmd *commandData,
) {
	var ps parser
	initParser(&ps, currentCmd.responseData, currentCmd.responseBinaries)

	if currentCmd.lastErr != nil {
		for i := range pipelineCommands {
			cmd := &pipelineCommands[i]
			cmd.err = currentCmd.lastErr
		}
		return
	}

	for i := range pipelineCommands {
		cmd := &pipelineCommands[i]

		switch cmd.cmdType {
		case commandTypeMGet:
			resp, err := ps.readMGet()
			cmd.getResp = resp
			cmd.err = err

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

func commandListWaitCompleted(cmdList *commandData) {
	for current := cmdList; current != nil; current = current.sibling {
		current.waitCompleted()
	}
}

func (s *pipelineSession) waitAndParseCmdData() {
	if s.alreadyWaited {
		return
	}
	s.alreadyWaited = true
	s.pipeline.resetPipelineSession()

	cmdList := s.builder.getCommandList()
	commandListWaitCompleted(cmdList)

	s.parseCommands(cmdList)

	// clear currentCmdList
	for current := cmdList; current != nil; {
		freeCommandResponseData(current)
		clearCmd := current
		current = current.sibling
		clearCmd.sibling = nil
	}
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

type commandIndex struct {
	sess  *pipelineSession
	index int
}

func (c commandIndex) getCmd() *pipelineCmd {
	return &c.sess.currentCmdList[c.index]
}

func (c commandIndex) pushAndWaitResponses() {
	sess := c.sess
	sess.pushCommandsIfNotPublished()
	sess.waitAndParseCmdData()
}

func (p *Pipeline) addCommand(cmdType commandType) commandIndex {
	sess := p.getCurrentSession()
	cmd := newPipelineCmd(cmdType)

	index := len(sess.currentCmdList)
	sess.currentCmdList = append(sess.currentCmdList, cmd)

	return commandIndex{
		sess:  sess,
		index: index,
	}
}

// Finish ...
func (p *Pipeline) Finish() {
	if p.currentSession != nil {
		sess := p.currentSession
		sess.pushCommandsIfNotPublished()
		sess.waitAndParseCmdData()
	}
}

func (c commandIndex) pushAndWaitIfNotRead() error {
	cmd := c.getCmd()

	if cmd.isRead {
		return ErrAlreadyGotten
	}

	cmd.isRead = true
	c.pushAndWaitResponses()

	return nil
}

func (c commandIndex) mgetResponseFunc() (MGetResponse, error) {
	err := c.pushAndWaitIfNotRead()
	if err != nil {
		return MGetResponse{}, err
	}

	cmd := c.getCmd()
	return cmd.getResp, cmd.err
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

	return cmd.mgetResponseFunc
}

// MSet ...
func (p *Pipeline) MSet(key string, value []byte, opts MSetOptions) func() (MSetResponse, error) {
	if err := validateKeyFormat(key); err != nil {
		return func() (MSetResponse, error) {
			return MSetResponse{}, err
		}
	}

	cmdIndex := p.addCommand(commandTypeMSet)
	cmdIndex.sess.builder.addMSet(key, value, opts)

	return func() (MSetResponse, error) {
		err := cmdIndex.pushAndWaitIfNotRead()
		if err != nil {
			return MSetResponse{}, err
		}

		cmd := cmdIndex.getCmd()

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

	cmdIndex := p.addCommand(commandTypeMDel)
	cmdIndex.sess.builder.addMDel(key, opts)

	return func() (MDelResponse, error) {
		err := cmdIndex.pushAndWaitIfNotRead()
		if err != nil {
			return MDelResponse{}, err
		}

		cmd := cmdIndex.getCmd()

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
	cmdIndex := p.addCommand(commandTypeFlushAll)
	cmdIndex.sess.builder.addFlushAll()

	return func() error {
		err := cmdIndex.pushAndWaitIfNotRead()
		if err != nil {
			return err
		}

		cmd := cmdIndex.getCmd()
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

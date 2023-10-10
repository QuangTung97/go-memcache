package memcache

import (
	"errors"
	"unicode"
	"unsafe"
)

// ErrAlreadyGotten returns when the callback functions of MGet, MSet, etc. is called more than once
var ErrAlreadyGotten = errors.New("pipeline error: already gotten")

type pipelineSession struct {
	pipeline *Pipeline
	cmdPool  *pipelineCommandListPool

	builder cmdBuilder

	published     bool
	alreadyWaited bool

	currentCmdList []*pipelineCmd
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
	cmdPool := p.c.cmdPool

	sess := &pipelineSession{
		pipeline: p,
		cmdPool:  cmdPool,

		published:     false,
		alreadyWaited: false,

		currentCmdList: cmdPool.getCommandList(),
	}
	initCmdBuilder(&sess.builder, p.c.maxCommandsPerBatch)
	return sess
}

func (p *Pipeline) newPipelineCmd(cmdType commandType) *pipelineCmd {
	cmd := getPipelineCmdFromPool()
	cmd.cmdType = cmdType
	return cmd
}

func newPipeline(conn *clientConn) *Pipeline {
	return &Pipeline{
		c:              conn,
		currentSession: nil,
	}
}

// Pipeline creates a pipeline
func (c *Client) Pipeline() *Pipeline {
	return newPipeline(c.getNextConn())
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
	pipelineCommands []*pipelineCmd,
	currentCmd *commandData,
) {
	var ps parser
	initParser(&ps, currentCmd.responseData, currentCmd.responseBinaries)

	if currentCmd.lastErr != nil {
		for _, cmd := range pipelineCommands {
			cmd.err = currentCmd.lastErr
		}
		return
	}

	for _, cmd := range pipelineCommands {
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

		case commandTypeVersion:
			resp, err := ps.readVersion()
			cmd.resp, cmd.err = unsafe.Pointer(&resp), err

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

	// clear cmdList
	for current := cmdList; current != nil; {
		freeCommandResponseData(current)
		clearCmd := current
		current = current.sibling
		clearCmd.sibling = nil
	}
	s.builder.clearCmd()

	// release pipeline command list to the pool
	s.cmdPool.putCommandList(s.currentCmdList)
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

type commandRef struct {
	sess *pipelineSession
	cmd  *pipelineCmd
}

func (c commandRef) getCmd() *pipelineCmd {
	return c.cmd
}

func (c commandRef) pushAndWaitResponses() {
	sess := c.sess
	sess.pushCommandsIfNotPublished()
	sess.waitAndParseCmdData()
}

func (p *Pipeline) addCommand(cmdType commandType) commandRef {
	sess := p.getCurrentSession()
	cmd := p.newPipelineCmd(cmdType)

	sess.currentCmdList = append(sess.currentCmdList, cmd)

	return commandRef{
		sess: sess,
		cmd:  cmd,
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

func (c commandRef) pushAndWaitIfNotRead() error {
	cmd := c.getCmd()

	if cmd.isRead {
		return ErrAlreadyGotten
	}

	cmd.isRead = true
	c.pushAndWaitResponses()

	return nil
}

// MGet ...
func (p *Pipeline) MGet(key string, opts MGetOptions) func() (MGetResponse, error) {
	result, err := p.MGetFast(key, opts)
	if err != nil {
		return func() (MGetResponse, error) {
			return MGetResponse{}, err
		}
	}
	return result.Result
}

// MGetFast is similar to MGet, but without one more alloc
func (p *Pipeline) MGetFast(key string, opts MGetOptions) (MGetResult, error) {
	if err := validateKeyFormat(key); err != nil {
		return MGetResult{}, err
	}

	cmdRef := p.addCommand(commandTypeMGet)
	cmdRef.sess.builder.addMGet(key, opts)

	return MGetResult{
		ref: cmdRef,
	}, nil
}

// MGetResult ...
type MGetResult struct {
	ref commandRef
}

// Result returns mget response
func (r MGetResult) Result() (MGetResponse, error) {
	err := r.ref.pushAndWaitIfNotRead()
	if err != nil {
		return MGetResponse{}, err
	}

	cmd := r.ref.getCmd()
	return cmd.getResp, cmd.err
}

// ReleaseMGetResult puts back to pool for reuse
func ReleaseMGetResult(r MGetResult) {
	putPipelineCmdToPool(r.ref.cmd)
}

// MSet ...
func (p *Pipeline) MSet(key string, value []byte, opts MSetOptions) func() (MSetResponse, error) {
	if err := validateKeyFormat(key); err != nil {
		return func() (MSetResponse, error) {
			return MSetResponse{}, err
		}
	}

	cmdRef := p.addCommand(commandTypeMSet)
	cmdRef.sess.builder.addMSet(key, value, opts)

	return func() (MSetResponse, error) {
		err := cmdRef.pushAndWaitIfNotRead()
		if err != nil {
			return MSetResponse{}, err
		}

		cmd := cmdRef.getCmd()

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

	cmdRef := p.addCommand(commandTypeMDel)
	cmdRef.sess.builder.addMDel(key, opts)

	return func() (MDelResponse, error) {
		err := cmdRef.pushAndWaitIfNotRead()
		if err != nil {
			return MDelResponse{}, err
		}

		cmd := cmdRef.getCmd()

		resp := (*MDelResponse)(cmd.resp)
		if resp == nil {
			return MDelResponse{}, cmd.err
		}
		return *resp, cmd.err
	}
}

// Version ...
func (p *Pipeline) Version() func() (VersionResponse, error) {
	cmdRef := p.addCommand(commandTypeVersion)
	cmdRef.sess.builder.addVersion()

	return func() (VersionResponse, error) {
		err := cmdRef.pushAndWaitIfNotRead()
		if err != nil {
			return VersionResponse{}, err
		}

		cmd := cmdRef.getCmd()

		resp := (*VersionResponse)(cmd.resp)
		if resp == nil {
			return VersionResponse{}, cmd.err
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
	cmdRef := p.addCommand(commandTypeFlushAll)
	cmdRef.sess.builder.addFlushAll()

	return func() error {
		err := cmdRef.pushAndWaitIfNotRead()
		if err != nil {
			return err
		}

		cmd := cmdRef.getCmd()
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

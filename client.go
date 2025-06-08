package geerpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"geerpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Call 表示一个正在进行的RPC调用的所需信息
type Call struct {
	Seq           uint64      // sequence number chosen by client（用来区分一个客户端发来的不同请求）
	ServiceMethod string      // format "Service.Method"
	Args          interface{} // arguments to the function
	Reply         interface{} // reply from the function
	Error         error       // if error occurs, it will be set
	Done          chan *Call  // Strobes when call is complete
}

func (c *Call) done() {
	c.Done <- c
}

// Client represents an RPC Client.
// There may be multiple outstanding Calls associated
// with a single Client, and a Client may be used by
// multiple goroutines simultaneously.
type Client struct {
	cc           codec.Codec // Codec本身会持有conn
	opt          *Option
	sending      sync.Mutex       // 为了保证请求的有序发送 protect following
	header       codec.Header     // 每个请求的消息头，只在请求发送时才需要，而请求发送是互斥的，因此每个客户端只需要一个，声明在 Client 结构体中可以复用。
	mu           sync.RWMutex     // protect following
	seq          uint64           // 给发出的请求编号，每个请求拥有唯一的编号。
	pendingCalls map[uint64]*Call // 存储未处理完的请求，键是编号，值是 Call 实例。因为实际上是允许同一个客户端在上一次RPC没有完成时就发起下一次RPC，因此需要一个哈希表来存储哪次请求的调用信息和结果
	closed       bool             // user has called Close
	shutdown     bool             // server has told us to stop
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	codecFn := codec.NewCodecFuncMap[opt.CodecFormat]
	if codecFn == nil {
		err := fmt.Errorf("invalid codec format %s", opt.CodecFormat)
		return nil, err
	}
	// 先需要完成一开始的协议交换，即发送 Option 信息给服务端。
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		conn.Close()
		return nil, err
	}
	return newClientCodec(codecFn(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	c := &Client{
		seq:          1, // seq starts with 1, 0 means invalid call
		cc:           cc,
		opt:          opt,
		pendingCalls: make(map[uint64]*Call),
	}
	// 协商好消息的编解码方式之后，再创建一个子协程调用 receive() 接收响应。
	go c.receive()
	return c
}

type result struct {
	cli *Client
	err error
}

type newClientFunc func(conn net.Conn, opt *Option) (*Client, error)

func dialTimeout(fn newClientFunc, network, address string, opts ...*Option) (cli *Client, err error) {
	parseOptions := func(opts ...*Option) (*Option, error) {
		if len(opts) == 0 || opts[0] == nil {
			return DefaultOption, nil
		} else if len(opts) > 1 {
			return nil, errors.New("number of options is more than 1")
		}
		if opts[0].MagicNum == 0 {
			opts[0].MagicNum = DefaultOption.MagicNum
		}
		if opts[0].CodecFormat == "" {
			opts[0].CodecFormat = DefaultOption.CodecFormat
		}
		return opts[0], nil
	}
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	// close the connection if client is nil
	defer func() {
		if cli == nil {
			conn.Close()
		}
	}()
	resCh := make(chan result, 1)
	go func() {
		// 创建客户端
		cli, err := fn(conn, opt)
		resCh <- result{cli: cli, err: err}
	}()
	if opt.ConnectTimeout == 0 {
		res := <-resCh
		return res.cli, res.err
	}
	// 检查超时
	select {
	case res := <-resCh:
		return res.cli, res.err
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	}
}

// Dial connects to an RPC server at the specified network address
func Dial(network, address string, opts ...*Option) (cli *Client, err error) {
	return dialTimeout(NewClient, network, address, opts...)
}

var ErrClientClosed = errors.New("rpc client closed")

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClientClosed
	}
	c.closed = true
	return c.cc.Close()
}

// IsAvailable return true if the client does work
func (c *Client) IsAvailable() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// closed 和 shutdown 任意一个值置为 true，则表示 Client 处于不可用的状态，但有些许的差别，closed 是用户主动关闭的，即调用 Close 方法，而 shutdown 置为 true 一般是有错误发生。
	return !c.shutdown && !c.closed
}

// pendCall 将参数 call 添加到 client.pendingCalls 中，并更新 client.seq
func (c *Client) pendCall(call *Call) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.shutdown {
		return 0, ErrClientClosed
	}
	call.Seq = c.seq
	c.pendingCalls[call.Seq] = call
	c.seq++
	return call.Seq, nil
}

// removeCall 根据 seq 从 client.pendingCalls 中移除对应的 call
func (c *Client) removeCall(seq uint64) *Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	call := c.pendingCalls[seq]
	delete(c.pendingCalls, seq)
	return call
}

// terminateCalls 服务端或客户端发生错误时调用，将 shutdown 设置为 true，且将错误信息通知所有 pending 状态的 call
func (c *Client) terminateCalls(err error) {
	// 加sending锁是为了保证此时不会有新的请求被发送（因为会修改shutdown和注册到pendingCalls）
	c.sending.Lock()
	defer c.sending.Unlock()
	// 加mu锁是为了保护c.pendingCalls和c.shutdown
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdown = true
	for _, call := range c.pendingCalls {
		c.handleCallError(call, err)
	}
}

func (c *Client) handleCallError(call *Call, err error) {
	call.Error = err
	call.done()
}

func (c *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = c.cc.ReadHeader(&h); err != nil {
			// 退出RPC监听，属于致命错误
			break
		}
		call := c.removeCall(h.Seq)
		switch {
		// call不存在
		case call == nil:
			// it usually means that Write partially failed and call was already removed.
			// 读掉这次的call数据
			err = c.cc.ReadBody(nil)
		// 服务端出错
		case h.Error != "":
			// 读掉这次的call数据
			call.Error = fmt.Errorf(h.Error)
			err = c.cc.ReadBody(nil)
			call.done()
		// call存在且服务端未出错
		default:
			err := c.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = fmt.Errorf("rpc: reading body: %w", err)
			}
			call.done()
		}
	}
	// error occurs, so terminateCalls pending calls
	c.terminateCalls(err)
	log.Printf("rpc client: codec error: %v", err)
}

func (c *Client) send(call *Call) {
	// 确保消息顺序发送
	c.sending.Lock()
	defer c.sending.Unlock()

	// pend this call
	seq, err := c.pendCall(call)
	if err != nil {
		c.handleCallError(call, err)
		return
	}

	// prepare request header
	c.header.ServiceMethod = call.ServiceMethod
	c.header.Seq = seq
	c.header.Error = ""

	// encode and send request
	if err := c.cc.Write(&c.header, call.Args); err != nil {
		call := c.removeCall(seq)
		// call may be nil, it usually means that Write partially failed,
		// client has received the response and handled
		if call != nil {
			c.handleCallError(call, err)
		}
	}
}

// Go invokes the function asynchronously.
// 是一个异步接口，返回这次调用的 call 实例。
// 参数 done chan *Call 可以自定义缓冲区的大小，从而可以实现给多个 client.Go 传入同一个 chan 对象来控制异步请求并发的数量。
func (c *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		// done must has enough buffer for the number of simultaneous
		// RPCs that will be using that channel. If the channel
		// is totally unbuffered, it's best not to run at all.
		panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	go c.send(call)
	return call
}

// Call invokes the named function, waits for it to complete, and returns its error status.
// 是对 Go 的封装，阻塞 call.Done，等待响应返回，是一个同步接口。
func (c *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	// 由于这里是同步接口，所以done的缓冲大小只需为1即可
	call := c.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done(): // 超时了
		c.removeCall(call.Seq)
		return fmt.Errorf("rpc client: call failed: %w", ctx.Err())
	case <-call.Done:
		return call.Error
	}
}

// NewHTTPClient new a Client instance via HTTP as transport protocol
func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	// 向服务端发送 HTTP CONNECT 请求
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))

	// Require successful HTTP response
	// before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

// DialHTTP connects to an HTTP RPC server at the specified network address
// listening on the default HTTP RPC path.
func DialHTTP(address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, "tcp", address, opts...)
}

// XDial calls different functions to connect to a RPC server according the first parameter rpcAddr.
// rpcAddr is a general format (protocol@addr) to represent a rpc server
// eg, http://10.0.0.1:7001, tcp://10.0.0.1:9999, unix:///tmp/geerpc.sock
func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "://")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol://addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP(addr, opts...)
	default:
		// tcp, unix or other transport protocol
		return Dial(protocol, addr, opts...)
	}
}

var _ io.Closer = (*Client)(nil)

package geerpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"geerpc/codec"
	"go/ast"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const kMagicNum = 0x3bef5c

type Option struct {
	MagicNum       int           // MagicNumber marks this's a geerpc request
	CodecFormat    codec.Format  // client may choose different Codec to encode body
	ConnectTimeout time.Duration // 0 means no limit
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNum:    kMagicNum,
	CodecFormat: codec.GobFormat,
}

// w -> bufio -> gob -> conn -> conn -> gob -> r
// 固定采用 JSON 编码 Option，后续的 header 和 body 的编码方式由 Option 中的 CodeType 指定，服务端首先使用 JSON 解码 Option，然后通过 Option 的 CodeType 解码剩余的内容。即报文将以这样的形式发送：
// | Option{MagicNumber: xxx, CodecFormat: xxx} | Header{ServiceMethod ...} | Body interface{} |
// | <------      固定 JSON 编码      ------>    | <-------   编码方式由 CodecFormat 决定  ------->|
// 在一次连接中，Option 固定在报文的最开始，Header 和 Body 可以有多个，即报文可能是这样的。
// | Option | Header1 | Body1 | Header2 | Body2 | ...

// request 存储了一次调用的全部信息
type request struct {
	h            *codec.Header // 该请求的Header
	argv, replyv reflect.Value // 该请求的参数和返回值
	mtype        *methodType   // 该请求想要调用的函数
	svc          *service      // 反射对应的结构体
}

// Server represents an RPC Server.
type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Register(rcvr interface{}) (err error) {
	svc := newService(rcvr)
	if _, exists := s.serviceMap.LoadOrStore(svc.name, svc); exists {
		err = errors.New("rpc server: service already registered: " + svc.name)
	}
	return
}

func (s *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dotPos := strings.LastIndex(serviceMethod, ".")
	if dotPos < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	svcName, methodName := serviceMethod[:dotPos], serviceMethod[dotPos+1:]
	svci, ok := s.serviceMap.Load(svcName)
	if !ok {
		err = errors.New("rpc server: can't find service " + svcName)
		return
	}
	svc = svci.(*service) // interface 在类型断言时需要断言为实现类的指针
	mtype = svc.methods[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
		return
	}
	return
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func (s *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Printf("rpc server: accept error: %v", err)
			continue
		}
		go s.ServeConn(conn)
	}
}

// ServeConn 解码连接中的消息，确定消息CodecFormat，然后剩下的处理交给serveCodec
func (s *Server) ServeConn(conn net.Conn) {
	defer conn.Close()
	// 握手阶段
	// 读取Option（客户端只需要在第一次通讯时发送自己的魔数和编码格式，因此服务端也只需要读取1次）
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	// 事实上除了校验魔数，很可能还会校验checksum
	if opt.MagicNum != kMagicNum {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNum)
		return
	}
	// 正式通信阶段
	codecFn := codec.NewCodecFuncMap[opt.CodecFormat]
	s.serveCodec(codecFn(conn), &opt)
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

// serveCodec 解码多个Header+Body对
func (s *Server) serveCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex) // 为了保证请求的有序发送
	wg := new(sync.WaitGroup)  // wait until all request are handled
	// 在一次长链接中，允许接收多个请求，而每个请求都是一个Header+Body对，因此这里使用for循环来处理每个请求，直到发生错误/连接断开
	for {
		// 解码请求数据
		req, err := s.readRequest(cc)
		if err != nil { // 连接被关闭或请求有误
			if req == nil { // 这里说明是请求有误，没必要进行下去了
				break
			}
			req.h.Error = err.Error()
			// 发送响应数据
			s.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		// 并发执行请求（需要保证请求执行完后响应的发送是顺序的，所以用sending锁来保证顺序）
		go s.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
}

// readRequest 读取请求
func (s *Server) readRequest(cc codec.Codec) (*request, error) {
	readHeaderFn := func() (*codec.Header, error) {
		var h codec.Header
		if err := cc.ReadHeader(&h); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				err = fmt.Errorf("rpc server: read header error: %w", err)
			}
			return nil, err
		}
		return &h, nil
	}
	h, err := readHeaderFn()
	if err != nil {
		return nil, err
	}
	req := &request{
		h: h,
	}
	req.svc, req.mtype, err = s.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()
	// make sure that argvi is a pointer, ReadBody need a pointer as parameter
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr { // 不是指针类型时，取地址
		argvi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvi); err != nil {
		err = fmt.Errorf("rpc server: read body error: %w", err)
	}
	return req, err
}

// handleRequest 处理请求
func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	// 使用非阻塞chan，防止超时以后，无缓存chan sent和called没有办法发送，协程被阻塞了退出不了导致的协程泄漏
	called := make(chan struct{}, 1)
	sent := make(chan struct{}, 1)
	go func() {
		defer func() { sent <- struct{}{} }()
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			return
		}
		s.sendResponse(cc, req.h, req.replyv.Interface(), sending)
	}()
	if timeout == 0 {
		<-called
		<-sent
		return
	}
	// timeout是执行本地方法+发送响应的总时间
	select {
	case <-called: // 服务端本地方法执行完成
		<-sent // 发送响应完成
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		s.sendResponse(cc, req.h, invalidRequest, sending)
	}
}

// sendResponse 发送响应
// golang里文件描述符(FD)的写入已经是 线程安全 的了，因此单纯的写入fd无需加锁。
// 但是这里是先写入用户缓冲区buf，而这个写入不是线程安全的，所以需要sending这把锁来保护。
// TODO 由于写入bufio的用户缓冲区速度很快，所以这里应该可以用自旋锁（不知道golang的自旋锁效率是否高于互斥锁）
func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

/*
对 net/rpc 而言，一个函数需要能够被远程调用，需要满足如下五个条件：

the method’s type is exported.
the method is exported.
the method has two arguments, both exported (or builtin) types.
the method’s second argument is a pointer.
the method has return type error.
更直观一些：
func (t *T) MethodName(argType T1, replyType *T2) error
*/
// methodType 包含了表示一个遵循上述RPC方法签名的完整信息
type methodType struct {
	method    reflect.Method // 方法本身
	ArgType   reflect.Type   // 第一个参数的类型
	ReplyType reflect.Type   // 第二个参数的类型
	numCalls  atomic.Uint64  // 统计方法调用的次数
}

func (m *methodType) NumCalls() uint64 {
	return m.numCalls.Load()
}

// 创建一个ArgType类型的变量（空值）
func (m *methodType) newArgv() (argv reflect.Value) {
	// arg may be a pointer type, or a value type
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem()) // 指针类型取指针
	} else {
		argv = reflect.New(m.ArgType).Elem() // 值类型取值
	}
	return
}

// 创建一个ReplyType类型的变量（空值）
func (m *methodType) newReplyv() (replyv reflect.Value) {
	// reply must be a pointer type
	replyv = reflect.New(m.ReplyType.Elem())
	// 取指针指向的值类型
	switch m.ReplyType.Elem().Kind() { // 初始化引用类型
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	case reflect.Chan:
		panic("rpc server: can not use chan type for reply")
	}
	return
}

// service 表示一个RPC服务
type service struct {
	name    string                 // 反射出的结构体名称
	typ     reflect.Type           // 反射出的结构体类型
	rcvr    reflect.Value          // 反射出的结构体实例本身（在调用实例方法时，需要实例本身作为第0个参数，即函数的receiver）
	methods map[string]*methodType // RPC方法表
}

func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name() // rcvr可能是一个指针，通过Indirect可以返回它指向的对象的类型。不然的话，它的type就是reflect.Ptr。所以不直接用s.typ.Name()
	s.typ = reflect.TypeOf(rcvr)
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

// registerMethods 过滤出了符合签名规则的方法
func (s *service) registerMethods() {
	isExportedOrBuiltinType := func(t reflect.Type) bool {
		return ast.IsExported(t.Name()) || t.PkgPath() == ""
	}
	s.methods = make(map[string]*methodType)
	for i := range s.typ.NumMethod() {
		method := s.typ.Method(i)
		mType := method.Type
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.methods[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}

// call 通过反射结构调用方法
func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	m.numCalls.Add(1)
	fn := m.method.Func
	retVals := fn.Call([]reflect.Value{s.rcvr, argv, replyv})
	if errInter := retVals[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}

const (
	connected        = "200 Connected to Gee RPC"
	defaultRPCPath   = "/_geeprc_"
	defaultDebugPath = "/debug/geerpc"
)

// ServeHTTP implements an http.Handler that answers RPC requests.
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		http.Error(w, "method must CONNECT", http.StatusMethodNotAllowed)
		return
	}
	// 获取其TCP套接字，从而劫持这个连接，使得之后都是基于TCP的RPC通信
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, fmt.Sprintf("HTTP/1.0 %s\n\n", connected))
	s.ServeConn(conn)
}

// HandleHTTP registers an HTTP handler for RPC messages on rpcPath.
// It is still necessary to invoke http.Serve(), typically in a go statement.
func (s *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, s)
	http.Handle(defaultDebugPath, debugHTTP{s})
	log.Println("rpc server debug path:", defaultDebugPath)
}

package geerpc

import (
	"encoding/json"
	"fmt"
	"geerpc/codec"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

const kMagicNum = 0x3bef5c

type Option struct {
	MagicNum    int          // MagicNumber marks this's a geerpc request
	CodecFormat codec.Format // client may choose different Codec to encode body
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

// request stores all information of a call
type request struct {
	h            *codec.Header // header of request
	argv, replyv reflect.Value // argv and replyv for request
}

type Server struct{}

func NewServer() *Server {
	return &Server{}
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

	// 反序列化Option（只需要在第一次通讯时发送自己的魔数和编码格式）
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

	codecFn := codec.NewCodecFuncMap[opt.CodecFormat]
	s.serveCodec(codecFn(conn))
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

// serveCodec 解码多个Header+Body对
func (s *Server) serveCodec(cc codec.Codec) {
	sending := new(sync.Mutex) // 为了保证请求的有序发送
	wg := new(sync.WaitGroup)  // wait until all request are handled
	// 可能有多个Header+Body对
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
		go s.handleRequest(cc, req, sending, wg)
	}
	wg.Wait()
	cc.Close()
}

// readRequest 读取请求
func (s *Server) readRequest(cc codec.Codec) (*request, error) {
	readHeaderFn := func() (*codec.Header, error) {
		var h codec.Header
		if err := cc.ReadHeader(&h); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				err = fmt.Errorf("rpc: read header error: %w", err)
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
	// TODO: now we don't know the type of request argv
	// day 1, just suppose it's string
	req.argv = reflect.New(reflect.TypeOf(""))
	if err = cc.ReadBody(req.argv.Interface()); err != nil {
		err = fmt.Errorf("rpc: read body error: %w", err)
	}
	return req, err
}

// handleRequest 处理请求
func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	// TODO, should call registered rpc methods to get the right replyv
	// day 1, just print argv and send a hello message
	log.Println(req.h, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("geerpc resp %d", req.h.Seq))
	s.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}

// sendResponse 发送响应
// golang里文件描述符(FD)的写入已经是 线程安全 的了，因此单纯的写入fd无需加锁。
// 但是这里是先写入用户缓冲区buf，而这个写入不是线程安全的，所以需要sending这把锁来保护。
func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

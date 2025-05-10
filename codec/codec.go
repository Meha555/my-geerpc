package codec

import "io"

// RPC协议 Header
type Header struct {
	ServiceMethod string // format "Service.Method"
	Seq           uint64 // sequence number chosen by client（用来区分一个客户端发来的不同请求）
	Error         string
}

// Codec 对消息体进行编解码
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

// 抽象出 Codec 的构造函数，客户端和服务端可以通过 Codec 的 Type 得到构造函数，从而创建 Codec 实例。这部分代码和工厂模式类似，与工厂模式不同的是，返回的是构造函数，而非实例。

type NewCodecFunc func(io.ReadWriteCloser) Codec

// 编码格式
type Format string

const (
	GobFormat  Format = "application/gob"
	JsonFormat Format = "application/json"
)

var NewCodecFuncMap map[Format]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Format]NewCodecFunc)
	NewCodecFuncMap[GobFormat] = NewGobCodec
}
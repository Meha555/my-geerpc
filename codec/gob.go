package codec

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
)

// Gob编码格式已经实现了LV编码，因此无需再记录body长度（见函数：func (enc *Encoder) writeMessage(w io.Writer, b *encBuffer)）
type GobCodec struct {
	conn io.ReadWriteCloser // 对端链接（TCP或unix-domain socket）
	buf  *bufio.Writer      // 防止阻塞而创建的带缓冲的 Writer，一般这么做能提升性能
	dec  *gob.Decoder
	enc  *gob.Encoder
}

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf), // 写入缓冲区使用buf而非conn，从而减少系统调用次数
	}
}

func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush() // 将 buf 中的全部内容写入到 conn 中，即发送消息
		if err != nil {
			_ = c.Close()
		}
	}()
	if err = c.enc.Encode(h); err != nil {
		return fmt.Errorf("rpc codec: gob error encoding header: %w", err)
	}
	if err = c.enc.Encode(body); err != nil {
		return fmt.Errorf("rpc codec: gob error encoding body: %w", err)
	}
	return
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}

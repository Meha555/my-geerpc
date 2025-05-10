package main

import (
	"encoding/json"
	"fmt"
	"geerpc"
	"geerpc/codec"
	"log"
	"net"
	"time"
)

// 为了确保server先启动完成，所以这里addr用无缓冲的chan
func startServer(addr chan string) {
	// pick a free port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("start rpc server on %s", lis.Addr())
	addr <- lis.Addr().String()
	server := geerpc.NewServer()
	server.Accept(lis)
}

func main() {
	addr := make(chan string)
	go startServer(addr)

	// in fact, following code is like a simple geerpc client
	conn, _ := net.Dial("tcp", <-addr)
	defer conn.Close()

	// 简单的使用time.sleep()方式隔离协议交换阶段与RPC消息阶段，减少server端解析Option的时候可能会破坏后面RPC消息的完整性的可能性
	// 当客户端消息发送过快而服务端消息积压时（例：Option|Header|Body|Header|Body），服务端使用json解析Option，json.Decode()调用conn.read()读取数据到内部的缓冲区（例：Option|Header），此时后续的RPC消息就不完整了(Body|Header|Body)。
	time.Sleep(time.Second)

	// 1. 发送Option
	_ = json.NewEncoder(conn).Encode(geerpc.DefaultOption)

	cc := codec.NewGobCodec(conn)
	for i := range 5 {
		h := &codec.Header{
			ServiceMethod: "Foo.Sum",
			Seq:           uint64(i),
		}
		// 2. 发送请求
		_ = cc.Write(h, fmt.Sprintf("geerpc req %d", i))
		// 3. 接收响应
		_ = cc.ReadHeader(h)
		var reply string
		_ = cc.ReadBody(&reply)
		log.Printf("call Foo.Sum, req %d, reply %s\n", i, reply)
	}
}

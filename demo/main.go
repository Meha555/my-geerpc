package main

import (
	"context"
	"geerpc"
	"log"
	"net"
	"sync"
	"time"
)

/*
1. go startServer(addr)启动服务器，服务器开始监听端口
2. geerpc.Dial("tcp", <-addr)，客户端在对应端口上创建服务器，同时创建一个对应的客户端来进行调用和接受返回
3. Dial会新建一个客户端，并且将options传给服务端，随后根据options的配置新建一个对应的Codec，同时客户端开始调用receive接收信息
4. for循环中client调用Call发送信息，Call通过Go组装好参数后，再由send发送信息，send的过程中客户端通过Write将信息发送到服务端
5. 服务端在serveCodec中处理并返回信息后，客户端的receive协程将会收到信息，此时将会读取到回复，对应的call.Done接收到变量，意味着执行结束
6. 执行结束后reply传给了call.Reply, 绑定到主函数中的reply string，打印出来即为结果。
*/

type FooService int

type Args struct{ Num1, Num2 int }

func (f FooService) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

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
	if err := server.Register(new(FooService)); err != nil {
		log.Printf("register error: %v", err)
	}
	server.Accept(lis)
}

func main() {
	addr := make(chan string)
	go startServer(addr)

	client, _ := geerpc.Dial("tcp", <-addr)
	defer client.Close()

	// 简单的使用time.sleep()方式隔离协议交换阶段与RPC消息阶段，减少server端解析Option的时候可能会破坏后面RPC消息的完整性的可能性
	// 当客户端消息发送过快而服务端消息积压时（例：Option|Header|Body|Header|Body），服务端使用json解析Option，json.Decode()调用conn.read()读取数据到内部的缓冲区（例：Option|Header），此时后续的RPC消息就不完整了(Body|Header|Body)。
	time.Sleep(time.Second)

	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &Args{Num1: i, Num2: i * i}
			var reply int
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := client.Call(ctx, "FooService.Sum", args, &reply); err != nil {
				log.Fatal("call FooService.Sum error:", err)
			}
			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}
	wg.Wait()
}

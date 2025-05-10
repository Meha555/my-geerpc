package main

import (
	"context"
	"geerpc"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type FooService int

type Args struct{ Num1, Num2 int }

func (f FooService) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func call(addr chan string) {
	client, _ := geerpc.DialHTTP(<-addr)
	// defer client.Close()

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

func main() {
	addr := make(chan string)
	go call(addr)
	// pick a free port
	lis, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("start rpc server on %s", lis.Addr())
	server := geerpc.NewServer()
	if err := server.Register(new(FooService)); err != nil {
		log.Printf("register error: %v", err)
	}
	server.HandleHTTP()
	addr <- lis.Addr().String()
	http.Serve(lis, nil)
}

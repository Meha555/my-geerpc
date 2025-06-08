package main

import (
	"context"
	"fmt"
	"geerpc"
	"geerpc/xclient"
	"log"
	"net"
	"sync"
	"time"
)

type FooService int

type Args struct {
	Num1, Num2 int
}

func (f FooService) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func (f FooService) Sleep(args Args, reply *int) error {
	time.Sleep(time.Second * time.Duration(args.Num1))
	*reply = args.Num1 + args.Num2
	return nil
}

func startServer(addrCh chan string) {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("start rpc server on %s", lis.Addr())
	addrCh <- lis.Addr().String()
	server := geerpc.NewServer()
	if err := server.Register(new(FooService)); err != nil {
		log.Printf("register error: %v", err)
	}
	server.Accept(lis)
}

func foo(xc *xclient.XClient, ctx context.Context, typ, serviceMethod string, args *Args) {
	var reply int
	var err error
	switch typ {
	case "call":
		err = xc.Call(ctx, serviceMethod, args, &reply)
	case "broadcast":
		err = xc.Broadcast(ctx, serviceMethod, args, &reply)
	}
	if err != nil {
		log.Printf("%s %s error: %v", typ, serviceMethod, err)
	} else {
		log.Printf("%s %s success: %d + %d = %d", typ, serviceMethod, args.Num1, args.Num2, reply)
	}
}

// call 调用单个实例
func call(d xclient.Discovery, mode xclient.SelectMode) {
	xc := xclient.NewXClient(d, mode, nil)
	defer xc.Close()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "call", "FooService.Sum", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}

// broadcast 调用所有实例
func broadcast(d xclient.Discovery, mode xclient.SelectMode) {
	xc := xclient.NewXClient(d, mode, nil)
	defer xc.Close()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "broadcast", "FooService.Sum", &Args{Num1: i, Num2: i * i})
			// expect 2 - 5 timeout
			ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
			foo(xc, ctx, "broadcast", "FooService.Sleep", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}

func main() {
	addrCh1 := make(chan string)
	go startServer(addrCh1)
	addrCh2 := make(chan string)
	go startServer(addrCh2)

	var addrs []string
	addrs = append(addrs, fmt.Sprintf("tcp://%s", <-addrCh1))
	addrs = append(addrs, fmt.Sprintf("tcp://%s", <-addrCh2))

	d := xclient.NewMultiServerDiscovery(addrs)
	call(d, xclient.RandomSelect)
	broadcast(d, xclient.RandomSelect)
	select {}
}

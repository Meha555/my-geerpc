package xclient

import (
	"context"
	"fmt"
	"geerpc"
	"reflect"
	"sync"
)

// XClient 支持负载均衡的客户端
type XClient struct {
	d       Discovery
	mode    SelectMode
	opt     *geerpc.Option
	mu      sync.Mutex
	clients map[string]*geerpc.Client // 缓存用于和服务实例通信的客户端
}

func NewXClient(d Discovery, mode SelectMode, opt *geerpc.Option) *XClient {
	return &XClient{
		d:       d,
		mode:    mode,
		opt:     opt,
		clients: make(map[string]*geerpc.Client),
	}
}

func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for addr, client := range xc.clients {
		client.Close()
		delete(xc.clients, addr)
	}
	return nil
}

func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	// choose a server instance
	if serverAddr, err := xc.d.Select(xc.mode); err != nil {
		return fmt.Errorf("rpc xclient call error: %w", err)
	} else {
		return xc.call(serverAddr, ctx, serviceMethod, args, reply)
	}
}

func (xc *XClient) call(serverAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	// 1. 建立连接
	if client, err := xc.dial(serverAddr); err != nil {
		return err
	} else {
		// 2. 发起调用并等待结果
		return client.Call(ctx, serviceMethod, args, reply)
	}
}

func (xc *XClient) dial(serverAddr string) (*geerpc.Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	client, ok := xc.clients[serverAddr]
	// 没有客户端或者有客户端但不可用，此时需要重新建立链接
	if !ok || !client.IsAvailable() {
		if client != nil {
			client.Close()
			delete(xc.clients, serverAddr)
		}
		var err error
		client, err = geerpc.XDial(serverAddr, xc.opt)
		if err != nil {
			return nil, fmt.Errorf("rpc xclient dial error: %w", err)
		}
		xc.clients[serverAddr] = client
	}
	return client, nil
}

// Broadcast 将请求广播到所有的服务实例，如果任意一个实例发生错误，则返回其中一个错误；如果调用成功，则返回其中一个的结果
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) (err error) {
	servers, err := xc.d.SelectAll()
	if err != nil {
		return fmt.Errorf("rpc xclient broadcast error: %w", err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	ctx, cancel := context.WithCancel(ctx)
	for _, server := range servers {
		wg.Add(1)
		// 为了提升性能，请求是并发的
		go func(serverAddr string) {
			defer wg.Done()
			// 每个请求都需要返回自己的reply，所以克隆一份
			var clonedReply interface{}
			if reply != nil {
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			e := xc.call(serverAddr, ctx, serviceMethod, args, clonedReply)
			mu.Lock()
			if e != nil && err == nil {
				err = e
				cancel() // if any call failed, cancel all unfinished calls
			}
			if e == nil && reply != nil {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
			}
			mu.Unlock()
		}(server)
	}
	wg.Wait()
	cancel()
	return
}

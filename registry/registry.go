package registry

import (
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	kDefaultTTL  = 5 * time.Minute    // 默认超时时间设置为 5 min。即任何注册的服务超过 5 min，即视为不可用状态，需要靠心跳来维护
	kDefaultPath = "/geerpc/registry" // 默认注册中心的HTTP路径
)

type Registry interface {
	// 存入一个服务实例地址
	Put(serverAddr string)
	// 返回所有可用的服务实例地址
	Alives() []string
}

// 为了实现上的简单（不再引入新的应用层协议）GeeRegistry 采用 HTTP 协议提供服务，且所有的有用信息都承载在 HTTP Header 中。
type GeeRegistry struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerInfo // <服务地址, 服务实例信息>
}

// ServerInfo 服务实例信息
type ServerInfo struct {
	Addr  string    // 实例地址(如tcp://ip:port)
	start time.Time // 上线时间戳
}

func NewGeeRegistry(timeout time.Duration) *GeeRegistry {
	if timeout <= 0 {
		timeout = kDefaultTTL
	}
	return &GeeRegistry{
		timeout: timeout,
		servers: make(map[string]*ServerInfo),
	}
}

func (r *GeeRegistry) Put(serverAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	log.Printf("rpc registry: put %s", serverAddr)
	r.servers[serverAddr] = &ServerInfo{Addr: serverAddr, start: time.Now()}
}

func (r *GeeRegistry) Alives() []string {
	r.mu.Lock()
	defer r.mu.Lock()
	var alives []string
	for addr, srv := range r.servers {
		if time.Since(srv.start) <= r.timeout {
			alives = append(alives, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	return alives
}

func (r *GeeRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// 按照RESTful语义
	switch req.Method {
	// 返回所有可用的服务列表，通过自定义字段 X-Geerpc-Servers 承载
	case http.MethodGet:
		w.Header().Set("X-Geerpc-Servers", strings.Join(r.Alives(), ","))
	// 添加服务实例或发送心跳，通过自定义字段 X-Geerpc-Server 承载
	case http.MethodPost:
		addr := req.Header.Get("X-Geerpc-Server")
		if addr == "" {
			http.Error(w, "X-Geerpc-Server header is required", http.StatusBadRequest)
			return
		}
		r.Put(addr)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// HandleHTTP registers an HTTP handler for Registry messages on registryPath
func (r *GeeRegistry) HandleHTTP() {
	http.Handle(kDefaultPath, r)
	log.Println("rpc registry path:", kDefaultPath)
}

// Heartbeat send a heartbeat message every once in a while
// it's a helper function for a server to register or send heartbeat
func Heartbeat(registryAddr, serverAddr string, interval time.Duration) (err error) {
	// 确保心跳间隔时间不小于默认的超时时间减去1分钟，保证心跳有效
	if interval <= kDefaultTTL-time.Minute {
		interval = kDefaultTTL - time.Minute
	}
	t := time.NewTicker(interval)
	for err == nil {
		<-t.C
		err = sendHeartbeat(registryAddr, serverAddr)
	}
	return
}

func sendHeartbeat(registryAddr, serverAddr string) error {
	registryUrl, err := url.JoinPath(registryAddr, kDefaultPath)
	if err != nil {
		return err
	}
	log.Println("rpc server: send heart beat to ", registryUrl)
	httpClient := &http.Client{}
	req, _ := http.NewRequest(http.MethodPost, registryUrl, nil)
	req.Header.Set("X-Geerpc-Server", serverAddr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}

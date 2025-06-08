package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// 负载均衡的实例选择策略
type SelectMode int

const (
	RandomSelect             SelectMode = iota // 随机选择
	RoundRobinSelect                           // 轮询选择
	WeightedRoundRobinSelect                   // 加权轮询选择
	ConsistentHashSelect                       // 一致性哈希选择
)

// Discovery 服务发现
// 需要服务发现机制，这样才有得挑选
type Discovery interface {
	// 从注册中心更新服务列表
	Refresh() error
	// 手动更新服务列表
	Update(servers []string) error
	// 根据负载均衡策略，挑选一个服务实例
	Select(mode SelectMode) (string, error)
	// 返回所有的服务实例
	SelectAll() ([]string, error)
}

var (
	ErrNoAvailableServer = errors.New("no available server")
)

// MultiServersDiscovery 是一个不需要注册中心，服务列表由手工维护的Discovery实现
type MultiServerDiscovery struct {
	r       *rand.Rand
	servers []string // 服务列表
	mu      sync.RWMutex
	index   int // 记录 Round Robin 算法已经轮询到的位置
}

func NewMultiServerDiscovery(servers []string) *MultiServerDiscovery {
	d := &MultiServerDiscovery{
		r:       rand.New(rand.NewSource(time.Now().UnixNano())), // 使用时间戳设定随机数种子，避免每次产生相同的随机数序列
		servers: servers,
	}
	d.index = d.r.Intn(math.MaxInt32 - 1) // 为了避免每次从 0 开始，初始化时随机设定一个值
	return d
}

func (d *MultiServerDiscovery) Refresh() error {
	return nil
}

func (d *MultiServerDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}

func (d *MultiServerDiscovery) Select(mode SelectMode) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	n := len(d.servers)
	if n == 0 {
		return "", ErrNoAvailableServer
	}
	switch mode {
	case RandomSelect:
		return d.servers[d.r.Intn(n)], nil
	case RoundRobinSelect:
		server := d.servers[d.index%n]
		d.index = (d.index + 1) % n
		return server, nil
	case WeightedRoundRobinSelect:
		panic("not implemented")
	case ConsistentHashSelect:
		panic("not implemented")
	default:
		return "", errors.New("unsupported select mode")
	}
}

func (d *MultiServerDiscovery) SelectAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	// return a copy of d.servers
	servers := make([]string, len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}

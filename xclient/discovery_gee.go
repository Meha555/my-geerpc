package xclient

import (
	"net/http"
	"strings"
	"time"
)

type GeeDiscovery struct {
	*InMemoryDiscovery               // 继承自 InMemoryDiscovery
	registryUrl        string        // 注册中心地址
	ttl                time.Duration // 服务实例列表的过期时间
	lastUpdate         time.Time     // 上次从注册中心更新服务实例列表的时间，默认 10s 过期，即 10s 之后，需要从注册中心更新新的服务实例列表。
}

const kDefaultUpdateInterval = 10 * time.Second // 本地缓存的服务实例列表更新间隔时间，应该小于心跳时间，因为这个更新间隔时间是用于尽量保证缓存一致性的

func NewGeeDiscovery(registryUrl string, ttl time.Duration) *GeeDiscovery {
	if ttl <= 0 {
		ttl = kDefaultUpdateInterval
	}
	return &GeeDiscovery{
		InMemoryDiscovery: NewInMemoryDiscovery(make([]string, 0)),
		registryUrl:       registryUrl,
		ttl:               ttl,
		lastUpdate:        time.Now(),
	}
}

func (d *GeeDiscovery) Refresh() error {
	if time.Since(d.lastUpdate) > d.ttl {
		// 过期，重新从注册中心获取服务实例列表
		resp, err := http.Get(d.registryUrl)
		if err != nil {
			return err
		}
		servers := strings.Split(resp.Header.Get("X-Geerpc-Servers"), ",")
		if len(servers) > 0 && servers[0] != "" {
			return d.Update(servers)
		}
	}
	// 没过期，不更新
	return nil
}

func (d *GeeDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

func (d *GeeDiscovery) Select(mode SelectMode) (string, error) {
	// NOTE 目前服务发现的方式就在这里，是每次要发起RPC时全量获取一次最新的服务列表然后再按照mode负载均衡一个实例
	// 因此这个模型只能用于一个Registry实例仅记录一种服务类的场景。不能即记录FooService的又记录BarService的，那样会造成部分FooService的RPC选择到BarService的实例，导致找不到方法，变成无效调用。
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.InMemoryDiscovery.Select(mode)
}

func (d *GeeDiscovery) SelectAll() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.InMemoryDiscovery.SelectAll()
}

package pool

import (
	"github.com/fzzy/radix/redis"
)

// A simple connection pool. It will create a small pool of initial connections,
// and if more connections are needed they will be created on demand. If a
// connection is returned and the pool is full it will be closed.
type Pool struct {
	network string
	addr    string
	pool    chan *redis.Client
}

// Creates a new Pool whose connections are all created using
// redis.Dial(network, addr). The size indicates the maximum number of idle
// connections to have waiting to be used at any given moment
func NewPool(network, addr string, size int) (*Pool, error) {
	var err error
	pool := make([]*redis.Client, size)
	for i := range pool {
		if pool[i], err = redis.Dial(network, addr); err != nil {
			return nil, err
		}
	}
	p := Pool{
		network: network,
		addr:    addr,
		pool:    make(chan *redis.Client, len(pool)),
	}
	return &p, nil
}

// Retrieves an available redis client. If there are none available it will
// create a new one on the fly
func (p *Pool) Get() (*redis.Client, error) {
	select {
	case conn := <-p.pool:
		return conn, nil
	default:
		return redis.Dial(p.network, p.addr)
	}
}

// Returns a client back to the pool. If the pool is full the client is closed
// instead. If the client is already closed (due to connection failure or
// what-have-you) it should not be put back in the pool. The pool will create
// more connections as needed.
func (p *Pool) Put(conn *redis.Client) {
	select {
	case p.pool <- conn:
	default:
		conn.Close()
	}
}

// Removes and calls Close() on all the connections currently in the pool.
// Assuming there are no other connections waiting to be Put back this method
// effectively closes and cleans up the pool.
func (p *Pool) Empty() {
	var conn *redis.Client
	for {
		select {
			case conn = <-p.pool:
				conn.Close()
			default:
				return
		}
	}
}

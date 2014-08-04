// The sentinel package provides a convenient interface with a redis sentinel
// which will automatically handle pooling connections and automatic failover.
//
// This package only gaurantees that when GetMaster is called the returned
// connection will be a connection to the master as of the moment that method is
// called. It is still possible that there is a failover as that connection is
// being used by the application, or before the PutMaster. Because of this,
// always check errors and never PutMaster on a connection which has returned an
// error.
//
// As a final note, a Client can be interacted with from multiple routines at
// once safely, except for the Close method. To safely Close, ensure that only
// one routine ever makes the call and that once the call is made no other
// methods are ever called by any routines.
package sentinel

import (
	"errors"
	"github.com/fzzy/radix/redis"
	"strings"

	"github.com/mediocregopher/radix-extra/pool"
	"github.com/mediocregopher/radix-extra/pubsub"
)

// An error wrapper returned by operations in this package. It implements the
// error interface and can therefore be passed around as a normal error.
type ClientError struct {
	err error

	// If this is true the error is due to a problem with the sentinel
	// connection, either it being closed or otherwise unavailable. If false the
	// error is due to some other circumstances. This is useful if you want to
	// implement some kind of reconnecting to sentinel on an error.
	SentinelErr bool
}

func (ce *ClientError) Error() string {
	return ce.err.Error()
}

type getReqRet struct {
	conn *redis.Client
	err  *ClientError
}

type getReq struct {
	name  string
	retCh chan *getReqRet
}

type putReq struct {
	name string
	conn *redis.Client
}

type switchMaster struct {
	name string
	addr string
}

type Client struct {
	poolSize    int
	masterPools map[string]*pool.Pool
	subClient   *pubsub.SubClient

	getCh   chan *getReq
	putCh   chan *putReq
	closeCh chan struct{}

	alwaysErr      *ClientError
	alwaysErrCh    chan *ClientError
	switchMasterCh chan *switchMaster
}

// Creates a sentinel client. Connects to the given sentinel instance, pulls the
// information for the masters of the given names, and creates an intial pool of
// connections for each master. The client will automatically replace the pool
// for any master should sentinel decide to fail the master over.
func NewClient(
	network, address string, poolSize int, names ...string,
) (
	*Client, *ClientError,
) {

	// We use this to fetch initial details about masters before we upgrade it
	// to a pubsub client
	client, err := redis.Dial(network, address)
	if err != nil {
		return nil, &ClientError{err: err}
	}

	masterPools := map[string]*pool.Pool{}
	for _, name := range names {
		r := client.Cmd("SENTINEL", "MASTER", name)
		l, err := r.List()
		if err != nil {
			return nil, &ClientError{err: err, SentinelErr: true}
		}
		addr := l[3] + ":" + l[5]
		pool, err := pool.NewPool("tcp", addr, poolSize)
		if err != nil {
			return nil, &ClientError{err: err}
		}
		masterPools[name] = pool
	}

	subClient := pubsub.NewSubClient(client)
	r := subClient.Subscribe("+switch-master")
	if r.Err != nil {
		return nil, &ClientError{err: r.Err, SentinelErr: true}
	}

	c := &Client{
		poolSize:       poolSize,
		masterPools:    masterPools,
		subClient:      subClient,
		getCh:          make(chan *getReq),
		putCh:          make(chan *putReq),
		closeCh:        make(chan struct{}),
		alwaysErrCh:    make(chan *ClientError),
		switchMasterCh: make(chan *switchMaster),
	}

	go c.subSpin()
	go c.spin()
	return c, nil
}

func (c *Client) subSpin() {
	for {
		r := c.subClient.Receive()
		if r.Timeout() {
			continue
		}
		if r.Err != nil {
			select {
			case c.alwaysErrCh <- &ClientError{err: r.Err, SentinelErr: true}:
			case <-c.closeCh:
			}
			return
		}
		sMsg := strings.Split(r.Message, " ")
		name := sMsg[0]
		newAddr := sMsg[3] + ":" + sMsg[4]
		select {
		case c.switchMasterCh <- &switchMaster{name, newAddr}:
		case <-c.closeCh:
			return
		}
	}
}

func (c *Client) spin() {
	for {
		select {
		case req := <-c.getCh:
			if c.alwaysErr != nil {
				req.retCh <- &getReqRet{nil, c.alwaysErr}
				continue
			}
			pool, ok := c.masterPools[req.name]
			if !ok {
				err := errors.New("unknown name: " + req.name)
				req.retCh <- &getReqRet{nil, &ClientError{err: err}}
				continue
			}
			conn, err := pool.Get()
			if err != nil {
				req.retCh <- &getReqRet{nil, &ClientError{err: err}}
				continue
			}
			req.retCh <- &getReqRet{conn, nil}

		case req := <-c.putCh:
			if pool, ok := c.masterPools[req.name]; ok {
				pool.Put(req.conn)
			}

		case err := <-c.alwaysErrCh:
			c.alwaysErr = err

		case sm := <-c.switchMasterCh:
			if p, ok := c.masterPools[sm.name]; ok {
				p.Empty()
				p = pool.NewOrEmptyPool("tcp", sm.addr, c.poolSize)
				c.masterPools[sm.name] = p
			}

		case <-c.closeCh:
			for name := range c.masterPools {
				c.masterPools[name].Empty()
			}
			c.subClient.Client.Close()
			close(c.getCh)
			close(c.putCh)
			close(c.alwaysErrCh)
			close(c.switchMasterCh)
			return
		}
	}
}

// Retrieves a connection for the master of the given name. If sentinel has
// become unreachable this will always return an error. Close should be called
// in that case
func (c *Client) GetMaster(name string) (*redis.Client, *ClientError) {
	req := getReq{name, make(chan *getReqRet)}
	c.getCh <- &req
	ret := <-req.retCh
	return ret.conn, ret.err
}

// Return a connection for a master of a given name. As with the pool package,
// do not return a connection which is having connectivity issues, or which is
// otherwise unable to perform requests.
func (c *Client) PutMaster(name string, client *redis.Client) {
	c.putCh <- &putReq{name, client}
}

// Closes all connection pools as well as the connection to sentinel.
func (c *Client) Close() {
	close(c.closeCh)
}

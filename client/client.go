package client

import (
	"errors"
	"net/rpc"
	"sync"
	"time"

	"distkv/store"
)


const (
	maxRedirects   = 16              
	requestTimeout = 5 * time.Second 
	retryDelay     = 100 * time.Millisecond
	watchInterval  = time.Second
)


type Client struct {
	mu        sync.Mutex
	endpoints []string

	conn     *rpc.Client
	connAddr string
}

func New(endpoints []string) *Client {
	return &Client{endpoints: append([]string(nil), endpoints...)}
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resetLocked()
}

func (c *Client) Set(key, value string) error {
	_, err := c.do(store.Request{Op: store.OpSet, Key: key, Value: value})
	return err
}

func (c *Client) SetEx(key, value string, ttl time.Duration) error {
	_, err := c.do(store.Request{Op: store.OpSetEx, Key: key, Value: value, TTL: ttl})
	return err
}

func (c *Client) Get(key string) (string, bool, error) {
	resp, err := c.do(store.Request{Op: store.OpGet, Key: key})
	if err != nil {
		return "", false, err
	}
	return resp.Value, resp.Found, nil
}

func (c *Client) Delete(key string) error {
	_, err := c.do(store.Request{Op: store.OpDelete, Key: key})
	return err
}

func (c *Client) List(prefix string) ([]string, error) {
	resp, err := c.do(store.Request{Op: store.OpList, Key: prefix})
	if err != nil {
		return nil, err
	}
	return resp.Keys, nil
}

// --- Config helpers ---

func (c *Client) SetConfig(key, value string) error {
	return c.Set(store.ConfigPrefix+key, value)
}

func (c *Client) GetConfig(key string) (string, bool, error) {
	return c.Get(store.ConfigPrefix + key)
}

func (c *Client) ListConfig() ([]string, error) {
	return c.List(store.ConfigPrefix)
}

func (c *Client) WatchConfig(key string, onChange func(value string)) (stop func()) {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(watchInterval)
		defer ticker.Stop()
		var (
			last string
			seen bool
		)
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				value, found, err := c.GetConfig(key)
				if err != nil || !found {
					continue
				}
				if !seen || value != last {
					last, seen = value, true
					onChange(value)
				}
			}
		}
	}()
	return func() { close(done) }
}

// --- Session helpers -------------------------------------------------------

func (c *Client) SetSession(userID, token string, ttl time.Duration) error {
	return c.SetEx(sessionTokenKey(userID), token, ttl)
}

func (c *Client) GetSession(userID string) (string, bool, error) {
	return c.Get(sessionTokenKey(userID))
}

func (c *Client) DeleteSession(userID string) error {
	return c.Delete(sessionTokenKey(userID))
}

func sessionTokenKey(userID string) string {
	return store.SessionPrefix + userID + "::token"
}

// --- Request plumbing ---

func (c *Client) do(req store.Request) (store.Response, error) {
	deadline := time.Now().Add(requestTimeout)
	var lastErr error
	for {
		resp, err := c.attempt(req)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return store.Response{}, lastErr
		}
		time.Sleep(retryDelay)
	}
}

func (c *Client) attempt(req store.Request) (store.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tried := make(map[string]bool)
	queue := append([]string(nil), c.endpoints...)
	var lastErr error

	for hops := 0; hops < maxRedirects && len(queue) > 0; hops++ {
		addr := queue[0]
		queue = queue[1:]
		if tried[addr] {
			continue
		}
		tried[addr] = true

		conn, err := c.connectLocked(addr)
		if err != nil {
			lastErr = err
			continue
		}

		var resp store.Response
		if err := conn.Call("Distkv.Execute", req, &resp); err != nil {
			lastErr = err
			c.resetLocked()
			continue
		}
		if resp.Served {
			return resp, nil
		}

		lastErr = errors.New("distkv: contacted node is not the leader")
		if resp.Leader != "" && !tried[resp.Leader] {
			queue = append([]string{resp.Leader}, queue...)
		}
	}

	if lastErr == nil {
		lastErr = errors.New("distkv: no cluster node was reachable")
	}
	return store.Response{}, lastErr
}

func (c *Client) connectLocked(addr string) (*rpc.Client, error) {
	if c.conn != nil && c.connAddr == addr {
		return c.conn, nil
	}
	c.resetLocked()

	conn, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c.conn, c.connAddr = conn, addr
	return conn, nil
}

func (c *Client) resetLocked() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn, c.connAddr = nil, ""
	return err
}

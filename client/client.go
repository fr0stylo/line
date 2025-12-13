package client

import (
	"sync"

	"google.golang.org/grpc"

	"github.com/fr0stylo/line/contracts/gen/rpc"
)

type Client struct {
	client rpc.LineBrokerClient
	conn   *grpc.ClientConn
	p      sync.Pool
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr)
	if err != nil {
		return nil, err
	}

	client := rpc.NewLineBrokerClient(conn)
	p := sync.Pool{New: func() interface{} { return make([]byte, 1024) }}
	return &Client{
		conn:   conn,
		client: client,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

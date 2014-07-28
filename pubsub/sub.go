package pubsub

import (
	"errors"

	"github.com/fzzy/radix/redis"
)

type SubReplyType uint8

const (
	ErrorReply SubReplyType = iota
	SubscribeType
	UnsubscribeType
	MessageType
)

// SubClient wraps a Redis client to provide convenience methods for Pub/Sub functionality.
type SubClient struct {
	Client *redis.Client
}

// SubReply wraps a Redis reply and provides convienient access to Pub/Sub info.
type SubReply struct {
	Type     SubReplyType // SubReply type
	Channel  string       // Channel reply is on
	SubCount int          // Count of subs active after this action (SubscribeType or UnsubscribeType)
	Message  string       // Publish message (MessageType)
	Err      error        // SubReply error
	Reply    *redis.Reply // Original Redis reply
}

// Subscribe makes a Redis "SUBSCRIBE" command on the provided channels
func (c *SubClient) Subscribe(channels ...interface{}) *SubReply {
	r := c.Client.Cmd("SUBSCRIBE", channels...)
	return c.parseReply(r)
}

// PSubscribe makes a Redis "PSUBSCRIBE" command on the provided patterns
func (c *SubClient) PSubscribe(patterns ...interface{}) *SubReply {
	r := c.Client.Cmd("PSUBSCRIBE", patterns...)
	return c.parseReply(r)
}

// Unsubscribe makes a Redis "UNSUBSCRIBE" command on the provided channels
func (c *SubClient) Unsubscribe(channels ...interface{}) *SubReply {
	r := c.Client.Cmd("UNSUBSCRIBE", channels...)
	return c.parseReply(r)
}

// PUnsubscribe makes a Redis "PUNSUBSCRIBE" command on the provided patterns
func (c *SubClient) PUnsubscribe(patterns ...interface{}) *SubReply {
	r := c.Client.Cmd("PUNSUBSCRIBE", patterns...)
	return c.parseReply(r)
}

// Receive returns the next publish reply on the Redis client.
func (c *SubClient) Receive() *SubReply {
	r := c.Client.ReadReply()
	return c.parseReply(r)
}

func (c *SubClient) parseReply(reply *redis.Reply) *SubReply {
	sr := &SubReply{Reply: reply}
	switch reply.Type {
	case redis.MultiReply:
		if len(reply.Elems) < 3 {
			sr.Err = errors.New("reply is not formatted as a subscription reply")
			return sr
		}
	case redis.ErrorReply:
		sr.Err = reply.Err
		return sr
	default:
		sr.Err = errors.New("reply is not formatted as a subscription reply")
		return sr
	}

	rtype, err := reply.Elems[0].Str()
	if err != nil {
		sr.Err = errors.New("subscription multireply does not have string value for type")
		return sr
	}
	channel, err := reply.Elems[1].Str()
	if err != nil {
		sr.Err = errors.New("subscription multireply does not have string value for channel")
		return sr
	}
	sr.Channel = channel

	//first element
	switch rtype {
	case "subscribe":
		sr.Type = SubscribeType
		count, err := reply.Elems[2].Int()
		if err != nil {
			sr.Err = errors.New("subscribe reply does not have int value for sub count")
		} else {
			sr.SubCount = count
		}
	case "unsubscribe":
		sr.Type = UnsubscribeType
		count, err := reply.Elems[2].Int()
		if err != nil {
			sr.Err = errors.New("unsubscribe reply does not have int value for sub count")
		} else {
			sr.SubCount = count
		}
	case "message":
		sr.Type = MessageType
		msg, err := reply.Elems[2].Str()
		if err != nil {
			sr.Err = errors.New("message reply does not have string value for body")
		} else {
			sr.Message = msg
		}
	default:
		sr.Err = errors.New("suscription multireply has invalid type: " + rtype)
	}
	return sr
}

package mongodb

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Subscriber struct{}

var _ message.Subscriber = &Subscriber{}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (o <-chan *message.Message, err error) {
	return make(chan *message.Message), nil
}

func (s *Subscriber) Close() error {
	return nil
}

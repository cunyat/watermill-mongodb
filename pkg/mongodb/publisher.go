package mongodb

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher struct {
}

var _ message.Publisher = &Publisher{}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	return nil
}

func (p *Publisher) Close() error {
	return nil
}

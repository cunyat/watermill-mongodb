package mongodb

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher struct {
}

var _ watermill.Publisher = &Publisher{}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	return nil
}

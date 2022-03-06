package mongodb

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

var ErrPublisherClosed = errors.New("publisher is closed")

type Publisher struct {
	db Database

	publishWg *sync.WaitGroup
	closech   chan struct{}
	closed    bool

	logger watermill.LoggerAdapter
}

var _ message.Publisher = &Publisher{}

func NewPublisher(db Database, logger watermill.LoggerAdapter) (*Publisher, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Publisher{
		db: db,

		publishWg: new(sync.WaitGroup),
		closech:   make(chan struct{}),
		closed:    false,

		logger: logger,
	}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return ErrPublisherClosed
	}

	p.publishWg.Add(1)
	defer p.publishWg.Done()

	p.logger.Trace("Inserting message to MongoDB", watermill.LogFields{
		"collection":   "messages",
		"len_messages": len(messages),
	})

	p.db.InsertMessages(context.Background(), topic, messages...)

	return nil
}

// Close closes the publisher, which means that all Publish calls called before are finished
// and no more Publish calls are accepted.
// Close is blocking until all the ongoing Publish calls have returned.
func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true

	close(p.closech)
	p.publishWg.Wait()

	return nil
}

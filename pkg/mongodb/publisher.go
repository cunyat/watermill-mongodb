package mongodb

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var ErrPublisherClosed = errors.New("publisher is closed")

type PublisherConfig struct {
	MessagesCollection string
}

func (c *PublisherConfig) setDefaults() {
	if c.MessagesCollection == "" {
		c.MessagesCollection = "messages"
	}
}

func (c PublisherConfig) validate() error {
	if err := validateCollectionName(c.MessagesCollection); err != nil {
		return err
	}

	return nil
}

type Publisher struct {
	config PublisherConfig
	db     *mongo.Database

	publishWg *sync.WaitGroup
	closech   chan struct{}
	closed    bool

	logger watermill.LoggerAdapter
}

var _ message.Publisher = &Publisher{}

func NewPublisher(db *mongo.Database, config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid publisher config")
	}

	if db == nil {
		return nil, errors.New("db is nil")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Publisher{
		config: config,
		db:     db,

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

	coll := p.db.Collection("messages")
	elements, err :=  p.marshalMessages(topic, messages...)
	if err != nil {
		return err
	}

	_, err = coll.InsertMany(context.Background(), elements)
	if err != nil {
		return errors.Wrap(err, "could not insert messages to collection")
	}

	return nil
}

func (p *Publisher) marshalMessages(topic string, messages ...*message.Message) ([]interface{}, error) {
	var arr = make([]interface{}, len(messages))
	series := p.db.Collection("series")
	filter := bson.D{{"_id", "messages"}}
	update := bson.D{{"$inc", bson.D{{"current", 1}}}}

	for i, msg := range messages {
		// todo: make only one query and increment current by len(messages)
		var serie struct{ Current int `bson:"current"` }
		err := series.FindOneAndUpdate(context.TODO(), filter, update, options.FindOneAndUpdate().SetUpsert(true)).Decode(&serie)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				serie.Current = 0
			} else {
				return nil, err
			}
		}

		arr[i] = bson.D{
			{"_id", serie.Current},
			{"uuid", msg.UUID},
			{"topic", topic},
			{"payload", string(msg.Payload)},
			{"metadata", msg.Metadata},
		}
	}

	return arr, nil
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

// validateCollectionName validates a mongodb collection name
// see https://docs.mongodb.com/manual/reference/limits/#mongodb-limit-Restriction-on-Collection-Names
func validateCollectionName(name string) error {
	if name == "" {
		return errors.New("empty collection name")
	}

	if strings.Contains(name, "$") {
		return errors.New("collection name can not contain '$' character")
	}

	if strings.Contains(name, "\u0000") {
		return errors.New("colleciton name can not include null character")
	}

	if strings.HasPrefix(name, "system.") {
		return errors.New("collection name can not start with 'system.'")
	}

	return nil
}

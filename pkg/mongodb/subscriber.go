package mongodb

import (
	"context"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Subscriber struct {
	db            *mongo.Database
	consumerGroup string

	subscribeWg *sync.WaitGroup
	closing     chan struct{}
	closed      bool

	logger watermill.LoggerAdapter
}

func NewSubscriber(db *mongo.Database, consumerGroup string, logger watermill.LoggerAdapter) (*Subscriber, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Subscriber{
		db:            db,
		subscribeWg:   &sync.WaitGroup{},
		consumerGroup: consumerGroup,
		closing:       make(chan struct{}),
		logger:        logger,
	}, nil
}

var _ message.Subscriber = &Subscriber{}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (o <-chan *message.Message, err error) {
	out := make(chan *message.Message)

	ctx, cancel := context.WithCancel(ctx)

	s.subscribeWg.Add(1)
	go func() {
		s.consume(ctx, topic, out)
		close(out)
		cancel()
	}()

	return out, nil
}

func (s *Subscriber) consume(ctx context.Context, topic string, out chan *message.Message) {
	defer s.subscribeWg.Done()

	logger := s.logger.With(watermill.LogFields{
		"topic":          topic,
		"consumer_group": s.consumerGroup,
	})

	var sleepTime time.Duration = 0
	for {
		select {
		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return

		case <-ctx.Done():
			logger.Info("Stopping consume, context canceled", nil)
			return

		case <-time.After(sleepTime): // Wait if needed
			sleepTime = 0
		}

		filter := bson.D{{"group", s.consumerGroup}, {"topic", topic}}
		opts := options.FindOneAndUpdate().SetUpsert(true)
		var off dbOffset
		err := s.db.Collection("subscribers").FindOne(ctx, filter).Decode(&off)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				off.OffsetAcked = 0
				off.OffsetConsumed = 0
			} else {
				panic(err)
			}
		}

		logger.Trace("Querying for message", watermill.LogFields{
			"offset": off.OffsetAcked,
		})

		var dbMsg dbMessage
		err = s.db.Collection("messages").FindOne(
			ctx,
			bson.D{{"_id", bson.D{{"$gt", off.OffsetAcked}}}, {"topic", topic}},
			options.FindOne().SetSort(bson.D{{"_id", 1}}),
		).Decode(&dbMsg)
		if err == mongo.ErrNoDocuments {
			continue
		}

		if err != nil {
			panic(err)
		}

		msg := message.NewMessage(dbMsg.UUID, []byte(dbMsg.Payload))
		msg.Metadata = dbMsg.Metadata

		logger = logger.With(watermill.LogFields{
			"msg_uuid": msg.UUID,
		})
		logger.Trace("Received message", watermill.LogFields{
			"offset": dbMsg.Offset,
		})

		acked := s.sendMessage(ctx, msg, out, logger)
		if acked {
			logger.Trace("Executing ack update", watermill.LogFields{
				"topic":   topic,
				"message": msg.UUID,
				"offset":  dbMsg.Offset,
			})

			err := s.db.Collection("subscribers").
				FindOneAndUpdate(ctx, filter, bson.D{{"$set", bson.D{{"offset_acked", dbMsg.Offset}}}}, opts).
				Err()
			if err != nil && err != mongo.ErrNoDocuments {
				panic(err)
			}

		}
	}
}

type dbOffset struct {
	ConsumerGroup  string `bson:"_id"`
	Topic          string `bson:"topic"`
	OffsetConsumed int    `bson:"offset_consumed"`
	OffsetAcked    int    `bson:"offset_acked"`
}

type dbMessage struct {
	Offset   int               `bson:"_id"`
	UUID     string            `bson:"uuid"`
	Topic    string            `bson:"topic"`
	Payload  string            `bson:"payload"`
	Metadata map[string]string `bson:"metadata"`
}

func (s *Subscriber) sendMessage(
	ctx context.Context,
	msg *message.Message,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) (acked bool) {
	msgCtx, cancel := context.WithCancel(ctx)
	msg.SetContext(msgCtx)
	defer cancel()

ResendLoop:
	for {
		select {
		case out <- msg:

		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return false

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return false
		}

		select {
		case <-msg.Acked():
			logger.Debug("Message acked by subscriber", nil)
			return true

		case <-msg.Nacked():
			//message nacked, try resending
			logger.Debug("Message nacked, resending", nil)
			msg = msg.Copy()
			msg.SetContext(msgCtx)

			time.Sleep(1 * time.Second) // todo: set sleep time in settings
			continue ResendLoop

		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return false

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return false
		}
	}
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()

	return nil
}

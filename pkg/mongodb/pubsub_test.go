package mongodb

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	logger = watermill.NewStdLogger(true, true)
)

func newPubSub(t *testing.T, db *mongo.Database, consumerGroup string) (message.Publisher, message.Subscriber) {
	pub, err := NewPublisher(db, PublisherConfig{}, logger)
	require.NoError(t, err)

	sub, err := NewSubscriber(db, consumerGroup, logger)
	require.NoError(t, err)

	return pub, sub
}

func newDatabase(t *testing.T) *mongo.Database {
	addr := os.Getenv("WATERMILL_TEST_MONGODB_HOST")
	if addr == "" {
		addr = "localhost"
	}

	connStr := fmt.Sprintf("mongodb://root:secret@%s:27017", addr)
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(connStr))
	require.NoError(t, err)

	err = client.Ping(context.Background(), readpref.Primary())
	require.NoError(t, err)

	return client.Database("watermill")
}

func createMongoDBPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, newDatabase(t), consumerGroup)
}

func createMongoDBPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createMongoDBPubSubWithConsumerGroup(t, "test")
}

func TestMongoDBPublishSubscribe(t *testing.T) {
	features := tests.Features{
		//	ConsumerGroups: true,
	}

	tests.TestPubSub(
		t,
		features,
		createMongoDBPubSub,
		createMongoDBPubSubWithConsumerGroup,
	)
}

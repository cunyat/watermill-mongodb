package mongodb

import (
	"context"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Database interface {
	InsertMessages(ctx context.Context, topic string, messages ...*message.Message) error
}

type MongoDatabaseConfig struct {
	MessagesCollection string
	SeriesCollection   string
}

var _ Database = (*MongoDatabase)(nil)

type MongoDatabase struct {
	db     *mongo.Database
	config MongoDatabaseConfig

	logger watermill.LoggerAdapter
}

type serie struct {
	Current int `bson:"current"`
}

func NewMongoDatabase(
	db *mongo.Database,
	config MongoDatabaseConfig,
	logger watermill.LoggerAdapter,
) (*MongoDatabase, error) {
	config.setDefaults()

	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid database config")
	}

	if db == nil {
		return nil, errors.New("db is nil")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &MongoDatabase{
		db:     db,
		config: config,
		logger: logger,
	}, nil
}

func (m *MongoDatabase) InsertMessages(ctx context.Context, topic string, messages ...*message.Message) error {
	var currSerie serie
	docs := make([]interface{}, len(messages))
	coll := m.db.Collection(m.config.MessagesCollection)
	series := m.db.Collection(m.config.SeriesCollection)
	filter := bson.D{{"_id", "messages"}}
	update := bson.D{{"$inc", bson.D{{"current", len(messages)}}}}
	upsert := options.FindOneAndUpdate().SetUpsert(true)

	err := series.FindOneAndUpdate(ctx, filter, update, upsert).Decode(&currSerie)
	if err == nil && err != mongo.ErrNoDocuments {
		return err
	}

	for i, msg := range messages {
		docs[i] = bson.D{
			{"_id", currSerie.Current + i},
			{"uuid", msg.UUID},
			{"topic", topic},
			{"payload", string(msg.Payload)},
			{"metadata", msg.Metadata},
		}
	}

	_, err = coll.InsertMany(ctx, docs)
	if err != nil {
		return errors.Wrap(err, "could not insert new messages")
	}

	return nil
}

func (c *MongoDatabaseConfig) setDefaults() {
	if c.MessagesCollection == "" {
		c.MessagesCollection = "messages"
	}

	if c.SeriesCollection == "" {
		c.SeriesCollection = "series"
	}
}

func (c MongoDatabaseConfig) validate() error {
	if err := validateCollectionName(c.MessagesCollection); err != nil {
		return err
	}
	if err := validateCollectionName(c.SeriesCollection); err != nil {
		return err
	}

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

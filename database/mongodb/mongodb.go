package mongodb

import (
	"context"
	"data-pipeline/pkg/config"
	"data-pipeline/pkg/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"os/signal"
	"syscall"
)

type mongoDB struct {
	client     *mongo.Client
	database   string
	collection string
}

var mongoClient *mongoDB

func MongoClient() *mongoDB {
	if mongoClient != nil {
		return mongoClient
	}
	return NewMongoDBClient()
}

func NewMongoDBClient() *mongoDB {
	uri := config.GetEnv("MONGO_URI")

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
	if err != nil {
		panic(err)
	}

	database := config.GetEnv("MONGO_DATABASE")
	collection := config.GetEnv("MONGO_COLLECTION")
	if database == "" {
		panic("MONGO_DATABASE is not set")
	}
	if collection == "" {
		panic("MONGO_COLLECTION is not set")
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGKILL,
			syscall.SIGQUIT,
			syscall.SIGABRT,
			syscall.SIGSTOP,
			syscall.SIGTSTP)
		<-sigChan
		_ = client.Disconnect(context.Background())
	}()

	mongoClient = &mongoDB{
		client:     client,
		database:   database,
		collection: collection,
	}
	return mongoClient
}

func (db *mongoDB) FindOne(ctx context.Context, filter any, dto any) error {
	if err := db.client.Database(db.database).Collection(db.collection).FindOne(ctx, filter).Decode(dto); err != nil {
		return err
	}

	return nil
}

func (db *mongoDB) FindMany(ctx context.Context, filter any, dto any) error {
	cursor, err := db.client.Database(db.database).Collection(db.collection).Find(ctx, filter)
	if err != nil {
		return err
	}

	if err = cursor.All(ctx, dto); err != nil {
		return err
	}

	return nil
}

func (db *mongoDB) Insert(ctx context.Context, entity any) (string, error) {
	result, err := db.client.Database(db.database).Collection(db.collection).InsertOne(
		ctx,
		entity,
		nil,
	)
	if err != nil {
		return "", err
	}

	messageID := result.InsertedID.(primitive.ObjectID).Hex()
	log.Logger.Info().Msg("created document: " + messageID)

	return messageID, nil
}

func (db *mongoDB) Upsert(ctx context.Context, entity any) (string, error) {
	opts := options.Update().SetUpsert(true)
	result, err := db.client.Database(db.database).Collection(db.collection).UpdateOne(
		ctx,
		entity,
		opts,
	)
	if err != nil {
		return "", err
	}

	messageID := result.UpsertedID.(primitive.ObjectID).Hex()
	log.Logger.Info().Msg("upserted document: " + messageID)

	return messageID, nil
}

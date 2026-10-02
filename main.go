package main

import (
	"context"
	"fmt"
	linkProcessor "main/link-processor"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/go-redis/redis"
	"github.com/joho/godotenv"
	"github.com/nsqio/go-nsq"
)

var SEED_LINK string = "https://google.com"
var ctx = context.Background()
var nsqTopic = "web-crawler"
var nsqConfig = nsq.NewConfig()
var nsqChannel = "channel"
var wg sync.WaitGroup

func main() {
	wg.Add(1)

	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("Unable to load environment configuration")
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_PORT"),
		Password: "", // no password set
		DB:       0,  // use default db
	})
	defer rdb.Close()

	// ----------------		Clear all resources		--------------
	err = rdb.FlushDB().Err()
	if err != nil {
		fmt.Println("Unable to flush redis")
		os.Exit(1)
	}

	_, err = http.Post("http://"+os.Getenv("NSQ_HTTP_PORT")+"/topic/delete?topic="+nsqTopic, "", nil)
	if err != nil {
		fmt.Println("Unable to delete topic from nsq" + err.Error())
		os.Exit(1)
	}

	// ------------------	Set up producer		-----------------
	producer, err := nsq.NewProducer(os.Getenv("NSQ_PORT"), nsqConfig)
	if err != nil {
		fmt.Println("Cannot connect producer to nsq")
		return
	}
	err = producer.Publish(nsqTopic, []byte(SEED_LINK))
	if err != nil {
		fmt.Println("Cannot write to producer")
		return
	}
	defer producer.Stop()

	// ------–------------	Set up consumer		----------------
	consumer, err := nsq.NewConsumer(nsqTopic, nsqChannel, nsqConfig)
	if err != nil {
		fmt.Println("Cannot initialise consumer")
		return
	}

	consumerLimit, err := strconv.Atoi(os.Getenv("CONSUMER_LIMIT"))
	if err != nil {
		fmt.Println("Consumer limit is not an integer")
		return
	}
	linkProcessLimit, err := strconv.ParseInt(os.Getenv("LINK_LIMIT"), 10, 64) // Parse int to get int64 type
	if err != nil {
		fmt.Println("Link Process limit is not an integer")
		return
	}
	for i := range consumerLimit {
		consumer.AddHandler(&linkProcessor.Handler{
			Rdb:              rdb,
			Producer:         producer,
			Consumer:         consumer,
			NsqTopic:         nsqTopic,
			Wg:               &wg,
			LinkProcessLimit: linkProcessLimit,
			ConsumerId:       i,
		})
	}
	err = consumer.ConnectToNSQD(os.Getenv("NSQ_PORT"))
	if err != nil {
		fmt.Println("Cannot connect consumer to nsq")
		return
	}

	wg.Wait()
}

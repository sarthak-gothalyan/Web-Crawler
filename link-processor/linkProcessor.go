package linkProcessor

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/go-redis/redis"
	"github.com/nsqio/go-nsq"
)

type Handler struct {
	Rdb              *redis.Client
	Producer         *nsq.Producer
	Consumer         *nsq.Consumer // required to close the consumer
	NsqTopic         string
	Wg               *sync.WaitGroup // stop main from exiting early
	LinkProcessLimit int64
	ConsumerId       int
}

// Returning nil automaticallly puts the message back in queue
func (h *Handler) HandleMessage(m *nsq.Message) error {
	// fmt.Printf("Consumer %d\n", h.ConsumerId)
	link := string(m.Body)
	fmt.Println("link:" + link)

	// Check if link was already processed
	_, err := h.Rdb.Get(link).Result()
	if err == nil {
		fmt.Println("link already processed:" + link)
		return nil
	} else if err != redis.Nil {
		fmt.Println("Unable to reach redis")
		os.Exit(1)
	}

	res, err := http.Get(link)
	// in case of no connection or 500 errors push link back in queue; in 400 codes drop the link
	if err != nil {
		fmt.Println("unable to get link: " + err.Error())
		return err
	}
	if res.StatusCode >= 400 && res.StatusCode < 500 {
		fmt.Println("link is malformed or doesnt exist: " + link)
		return nil
	}
	if res.StatusCode >= 500 {
		fmt.Println("server error, resubmitting the link to queue: " + link)
		return fmt.Errorf("server error, unable to get link data")
	}

	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	// the link returned a malformed body put it back in queue for reprocessing
	if err != nil {
		fmt.Println("unable to read body: " + err.Error())
		return err
	}
	sanitizedBody := string(body)

	// Got the contents of the page so now add it to redis
	err = h.Rdb.Set(link, sanitizedBody, 0).Err()
	if err != nil {
		fmt.Println("cannot write link to redis")
		os.Exit(1)
	}

	// extract sub links
	startIndex := 0
	links := make([]string, 1)
	for startIndex < len(sanitizedBody) {
		i := strings.Index(sanitizedBody[startIndex:], "a href")
		// No more links
		if i < 0 {
			break
		}
		i += startIndex // get the actual index in body
		i += 8          // move past "a href"

		// get the link end
		// check if link end -1 before adding i or it will give wrong indexes
		linkEnd := strings.Index(sanitizedBody[i:], "\"")
		if linkEnd < 0 {
			fmt.Println("sub link malformed")
			break
		}
		linkEnd += i // get actual index

		links = append(links, sanitizedBody[i:linkEnd])
		startIndex = linkEnd + 1
	}

	// add all extracted links to queue
	fmt.Println("adding sub links")
	for _, l := range links {
		if len(l) == 0 {
			continue
		}
		// if length is too short for https:// or begining is not http or https add https to beginning of link
		if len(l) < len("https://") || (l[0:8] != "https://" && l[0:7] != "http://") {
			l = "https:/" + l
		}

		err = h.Producer.Publish(h.NsqTopic, []byte(l))
		if err != nil {
			fmt.Println("Unable to add:" + l)
			os.Exit(1)
		}
	}

	totalKeys, err := h.Rdb.DBSize().Result()
	if err != nil {
		fmt.Println("Could not get redis size")
		os.Exit(1)
	}
	fmt.Printf("%d links processed\n", totalKeys)
	if totalKeys >= h.LinkProcessLimit {
		fmt.Println("stopping the consumers")
		// using a goroutine prevents deadlock since we are calling stop from inside a handler
		go func() {
			h.Consumer.Stop()
			<-h.Consumer.StopChan
			h.Wg.Done()
		}()
	}

	// time.Sleep(time.Second * 3)
	return nil
}

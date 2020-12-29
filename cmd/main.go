package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/streemtech/divider"

	"github.com/go-redis/redis/v8"
	rd "github.com/streemtech/divider/redis"
)

func main() {
	fmt.Println("HI")
	siulators()
	makeWatcher(true)
}

var keyStr = "test"
var watchers = 10
var subscribers = 10000

func siulators() {
	for i := 0; i < watchers; i++ {
		go makeWatcher(false)
	}
	time.Sleep(time.Second)
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "Password1234", // no password set
		DB:       0,              // use default DB
	})
	for i := 0; i < subscribers; i++ {
		go makeSub(client, i)
	}
}

func makeWatcher(print bool) {
	var divider divider.Divider
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "Password1234", // no password set
		DB:       0,              // use default DB
	})

	// res, err := client.PubSubChannels(context.TODO(), "poll:*").Result()
	// if err != nil && err.Error() != "redis: nil" {
	// 	fmt.Printf(err.Error())
	// }
	// fmt.Printf("CHANNELS %v (%T)\n", res, res)
	divider = rd.NewDivider(client, keyStr, nil, 10)
	divider.Start()
	//divider.SetAffinity(1000)
	//divider.SetAffinity(2000)
	//divider.SetAffinity(3000)
	//divider.SetAffinity(4000)

	if print {

		for {
			time.Sleep(time.Second)
			now := time.Now()
			vals := divider.GetAssignedProcessingArray()
			dur := now.Sub(time.Now())
			fmt.Printf("%s %v\n", dur.String(), vals)
		}
	} else {
		for {
			time.Sleep(time.Second)
			divider.GetAssignedProcessingArray()
		}
	}
}

func makeSub(client *redis.Client, i int) {

	sub := client.Subscribe(context.Background(), keyStr+":"+strconv.Itoa(i))

	channel := sub.Channel()

	for {
		select {
		case d := <-channel:
			fmt.Printf("data on channel %d: %s\n", i, d.String())
		}
	}
}

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

func siulators() {
	for i := 0; i < 3; i++ {
		go makeWatcher(false)
	}
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "Password1234", // no password set
		DB:       0,              // use default DB
	})
	for i := 0; i < 10; i++ {
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
	divider = rd.NewDivider(client, "poll", nil, 10)
	divider.Start()
	//divider.SetAffinity(1000)
	//divider.SetAffinity(2000)
	//divider.SetAffinity(3000)
	//divider.SetAffinity(4000)

	if print {

		for {
			time.Sleep(time.Second)
			now := time.Now()
			divider.GetAssignedProcessingArray()
			dur := now.Sub(time.Now())
			fmt.Println(dur.String())
		}
	} else {
		for {
			time.Sleep(time.Second)
			divider.GetAssignedProcessingArray()
		}
	}
}

func makeSub(client *redis.Client, i int) {

	sub := client.Subscribe(context.Background(), "poll:"+strconv.Itoa(i))

	channel := sub.Channel()

	for {
		select {
		case d := <-channel:
			fmt.Printf("data on channel %s\n", d.String())
		}
	}
}

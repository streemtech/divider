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


	if massTest {
		siulators()
	}

	makeWatcher(false, -1)
}

var massTest = true
var keyStr = "test"
var watchers = 2
var subscribers = 10000

func siulators() {
	for i := 0; i < watchers; i++ {
		go makeWatcher(false, i)
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

func makeWatcher(print bool, idx int) {
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
		in := divider.GetReceiveStartProcessingChan()
		out := divider.GetReceiveStopProcessingChan()
		for {
			select {
			case i := <-in: //currently thrashing. I suspect that it is because the assignment order is technicall inconsistent.
				fmt.Printf("New  Data for %d: %s\n", idx, i)
			case i := <-out:
				fmt.Printf("Stop Data For %d: %s\n", idx, i)
			}
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

package main

import (
	"fmt"
	"time"

	"github.com/streemtech/divider"

	"github.com/go-redis/redis/v8"
	rd "github.com/streemtech/divider/redis"
)

func main() {
	fmt.Println("HI")
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
	divider = rd.NewDivider(client, "poll", rd.PubSub, nil)
	divider.Start()
	//divider.SetAffinity(1000)
	//divider.SetAffinity(2000)
	//divider.SetAffinity(3000)
	//divider.SetAffinity(4000)

	for {
		time.Sleep(time.Second)
		fmt.Printf("%v\n", divider.GetAssignedProcessingArray())
	}

}

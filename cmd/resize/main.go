package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/streemtech/divider"
	"github.com/streemtech/divider/redisconsistent"
)

func main() {

	r := redis.NewUniversalClient(&redis.UniversalOptions{
		Password: "password",
		Addrs:    []string{"192.168.35.1:6379"},
	})
	l := divider.DefaultLogger{}

	d1 := redisconsistent.NewDivider(r, "con:test", uuid.New().String(), l, time.Second, time.Second, 3, nil, time.Millisecond*500, time.Millisecond*500)
	if d1 == nil {
		panic("Unable to create divider")
	}

	d2 := redisconsistent.NewDivider(r, "con:test", uuid.New().String(), l, time.Second, time.Second, 3, nil, time.Millisecond*500, time.Millisecond*500)
	if d2 == nil {
		panic("Unable to create divider")
	}

	d1.Start()

	d2.Start()

	if true {
		for i := 0; i < 25; i++ {
			d1.StartProcessing(uuid.New().String())
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	time.Sleep(time.Second)
	go func() {
		for {
			w := d1.GetAssignedProcessingArray()
			if len(w) == 0 {
				wg.Done()
				return
			}
			// fmt.Printf("%v\n", w)
			time.Sleep(time.Millisecond * 950)
			n := w[0]
			fmt.Printf("D1 Completed %s\n", n)
			err := d1.StopProcessing(n)
			if err != nil {
				panic(err)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {

		for {

			w := d2.GetAssignedProcessingArray()
			if len(w) == 0 {
				wg.Done()
				return
			}
			// fmt.Printf("%v\n", w)
			time.Sleep(time.Millisecond * 600)
			n := w[0]
			fmt.Printf("D2 Completed %s\n", n)
			err := d2.StopProcessing(n)
			if err != nil {
				panic(err)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
}

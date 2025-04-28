package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/streemtech/divider/redisconsistent"
)

func main() {

	r := redis.NewUniversalClient(&redis.UniversalOptions{
		Password: "",
		Addrs:    []string{"192.168.34.70:31081"},
	})

	data := []string{}
	for i := range 5 {
		data = append(data, strconv.Itoa(i))
	}

	// pp.Println(data)

	d1, err := redisconsistent.New(r, "con:test",
		redisconsistent.WithInstanceID("d1"),
		redisconsistent.WithNodeCount(3),
		redisconsistent.WithStarter(func(ctx context.Context, key string) error {
			fmt.Printf("Starting %s on d1\n", key)
			return nil
		}),
		redisconsistent.WithStopper(func(ctx context.Context, key string) error {
			fmt.Printf("Stopping %s on d1\n", key)
			return nil
		}),
		redisconsistent.WithWorkFetcher(func(r context.Context) (work []string, err error) { return data, nil }),
	)
	if err != nil {
		panic(err.Error())
	}
	if d1 == nil {
		panic("Unable to create divider")
	}

	d2, err := redisconsistent.New(r, "con:test",
		redisconsistent.WithInstanceID("d2"),
		redisconsistent.WithNodeCount(3),
		redisconsistent.WithStarter(func(ctx context.Context, key string) error {
			fmt.Printf("Starting %s on d2\n", key)
			return nil
		}),
		redisconsistent.WithStopper(func(ctx context.Context, key string) error {
			fmt.Printf("Stopping %s on d2\n", key)
			return nil
		}),
		redisconsistent.WithWorkFetcher(func(r context.Context) (work []string, err error) { return data, nil }),
	)
	if err != nil {
		panic(err.Error())
	}
	if d2 == nil {
		panic("Unable to create divider")
	}

	d1.StartWorker(context.Background())
	d2.StartWorker(context.Background())
	time.Sleep(time.Second)

	//start adding new work
	newData := []string{}
	for i := range 5 {
		newData = append(newData, strconv.Itoa(i+5))
		data = append(data, strconv.Itoa(i+5))
	}

	// pp.Println(newData)

	err = d1.StartProcessing(context.Background(), newData...)

	if err != nil {
		panic(err.Error())
	}

	time.Sleep(time.Second)
	data = append(data, newData...)

	time.Sleep(time.Second * 15)

	i, _ := d1.GetWork(context.Background())
	for v := range i {
		fmt.Printf("D1: %s\n", v)
	}

	i, _ = d2.GetWork(context.Background())
	for v := range i {
		fmt.Printf("D2: %s\n", v)
	}

	fmt.Println("STOP D1")
	d1.StopWorker(context.Background())
	time.Sleep(time.Millisecond * 100)

	i, _ = d2.GetWork(context.Background())
	for v := range i {
		fmt.Printf("D2: %s\n", v)
	}

	time.Sleep(time.Millisecond * 100)
	fmt.Println("RESTART D1")
	d1.StartWorker(context.Background())

	time.Sleep(time.Millisecond * 100)

	i, _ = d1.GetWork(context.Background())
	for v := range i {
		fmt.Printf("D1: %s\n", v)
	}

	i, _ = d2.GetWork(context.Background())
	for v := range i {
		fmt.Printf("D2: %s\n", v)
	}

	fmt.Println("Remove Individual work 0,1")

	time.Sleep(time.Millisecond * 100)

	_ = d1.StopProcessing(context.Background(), "0", "1", "8")
	time.Sleep(time.Millisecond * 100)

	i, _ = d1.GetWork(context.Background())
	for v := range i {
		fmt.Printf("D1: %s\n", v)
	}

	i, _ = d2.GetWork(context.Background())
	for v := range i {
		fmt.Printf("D2: %s\n", v)
	}
}

/*

del con:test:new_work
del con:test:timeout
del con:test:nodes
del con:test:work
del con:test:new_worker

*/

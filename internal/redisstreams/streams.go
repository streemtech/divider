package redisstreams

import (
	"context"
	"log/slog"
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/streemtech/divider"
)

//TODO consider adding prometheus tracking to stream listener.

type StreamListener struct {
	Client   redis.UniversalClient
	Key      string
	Logger   divider.LoggerGen
	Callback func(context.Context, string)
	Ctx      context.Context
}

func (s *StreamListener) Listen() {

	//create the output chanel

	go func() {
		previousItem := "$" //Special case to ask for all events after it joined.
		for {
			//listen for events
			entries, err := s.Client.XRead(s.Ctx, &redis.XReadArgs{
				Block:   time.Duration(0),
				Streams: []string{s.Key, previousItem},
				Count:   1,
			}).Result()
			if err != nil {
				select {
				case <-s.Ctx.Done():
					//context done. Able to just return.
					return
				default:
				}
				s.Logger(s.Ctx).Panic("failed to read data from stream", slog.String("err.error", err.Error()), slog.String("streamlistener.stream", s.Key))
			}

			//for all events, get teh data and send it to the channel.
			for _, ent := range entries {
				for _, msg := range ent.Messages {
					//set it so that I get the next ID from the list of IDs.
					previousItem = msg.ID

					//get the data
					data, ok := msg.Values["data"]
					if !ok {
						s.Logger(s.Ctx).Error("failed to get data from values", slog.String("streamlistener.stream", s.Key))
						continue
					}
					dataStr, ok := data.(string)
					if !ok {
						s.Logger(s.Ctx).Error("failed to convert data to string", slog.String("streamlistener.stream", s.Key))
						continue
					}
					//send the data to the handler.
					s.Callback(s.Ctx, dataStr)
				}
			}

		}
	}()
}

func (s *StreamListener) Publish(ctx context.Context, data string) (err error) {
	_, err = s.Client.XAdd(ctx, &redis.XAddArgs{
		Stream:     s.Key,
		NoMkStream: false,
		ID:         "*", //Set by Redis client automatically
		MaxLen:     100,
		// MinID: 0, //exclusive with MaxLen.
		Approx: true, //maxLen can be approximate.
		Values: map[string]string{
			"data": data,
		},
	}).Result()
	if err != nil {
		return errors.Wrap(err, "failed to add item to stream")
	}
	return nil
}

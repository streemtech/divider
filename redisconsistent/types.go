package redisconsistent

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/streemtech/divider/internal/redisstreams"
	"github.com/streemtech/divider/internal/set"
	"github.com/streemtech/divider/internal/ticker"
)

type StarterFunc = func(ctx context.Context, key string) error
type WorkFetcherFunc = func(r context.Context) (work []string, err error)

type dividerWorker struct {
	conf    dividerConf
	client  redis.UniversalClient
	storage WorkStorage

	cancel func()
	ctx    context.Context

	//this is the list of currently known work.
	knownWork set.Set[string]

	newWorker    redisstreams.StreamListener
	removeWorker redisstreams.StreamListener
	newWork      redisstreams.StreamListener
	removeWork   redisstreams.StreamListener

	masterUpdateRequiredWork  ticker.TickerFunc
	workerRectifyAssignedWork ticker.TickerFunc
	masterPing                ticker.TickerFunc
	workerPing                ticker.TickerFunc
}

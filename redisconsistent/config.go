package redisconsistent

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/streemtech/divider"
	"github.com/streemtech/divider/internal/redisstreams"
	"github.com/streemtech/divider/internal/set"
)

type dividerConf struct {
	rootKey     string
	instanceID  string
	logger      divider.LoggerGen
	nodeCount   int
	metricsName string
	workFetcher WorkFetcherFunc
	starter     StarterFunc
	stopper     StarterFunc

	//how long the master reserves itself.
	masterTimeout time.Duration

	//how long the workers wait before counting a timeout.
	workerTimeout time.Duration

	//how long the master waits to update the list of all work.
	updateAssignments time.Duration

	//how long the worker waits before doing reconciliation.
	compareKeys time.Duration
}

type DividerOpt = func(*dividerConf)

// sets the ID of the instance. Defaults to random UUID
func WithInstanceID(id string) DividerOpt {
	return func(dc *dividerConf) {
		dc.instanceID = id
	}
}

// set the logger to use. When unset, constructs default logger from slog.Default
func WithLogger(logger divider.LoggerGen) DividerOpt {
	return func(dc *dividerConf) {
		dc.logger = logger
	}
}

// set the number of nodes that this worker will add. defaults 10.
func WithNodeCount(count int) DividerOpt {
	return func(dc *dividerConf) {
		dc.nodeCount = count
	}
}

// set the metrics name to use for the metrics. Defaults to root key
func WithMetricsName(name string) DividerOpt {
	return func(dc *dividerConf) {
		dc.metricsName = name
	}
}

// Set the function to use to get work. Required.
func WithWorkFetcher(f WorkFetcherFunc) DividerOpt {
	return func(dc *dividerConf) {
		dc.workFetcher = f
	}
}

// Set the function to use to start a given set of work. Required.
func WithStarter(f StarterFunc) DividerOpt {
	return func(dc *dividerConf) {
		dc.starter = f
	}
}

// Set the function to use on stopping a given work set. Required.
func WithStopper(f StarterFunc) DividerOpt {
	return func(dc *dividerConf) {
		dc.stopper = f
	}
}

// Time that a master can be in the list after a ping and have not reported.
func WithMasterTimeoutDuration(value time.Duration) DividerOpt {
	return func(dc *dividerConf) {
		dc.masterTimeout = value
	}
}

// Time that a worker can be in the list after a ping and have not reported.
func WithWorkerTimeoutDuration(value time.Duration) DividerOpt {
	return func(dc *dividerConf) {
		dc.workerTimeout = value
	}
}

// time between checking for the full list of work.
func WithUpdateAssignmentsDuration(value time.Duration) DividerOpt {
	return func(dc *dividerConf) {
		dc.updateAssignments = value
	}
}

// Time between rectifying the work expected to be working on, and the work currently being worked on.
func WithCompareKeysDuration(value time.Duration) DividerOpt {
	return func(dc *dividerConf) {
		dc.compareKeys = value
	}
}

func New(client redis.UniversalClient, rootKey string, Opts ...DividerOpt) (divider.Divider, error) {

	conf := &dividerConf{
		metricsName:   rootKey,
		instanceID:    uuid.New().String(),
		nodeCount:     10,
		logger:        divider.DefaultLogger,
		masterTimeout: time.Second * 10,
		workerTimeout: time.Second * 10,

		updateAssignments: time.Second * 10,
		compareKeys:       time.Second * 10,
		rootKey:           rootKey,
	}

	for _, v := range Opts {
		v(conf)
	}

	return &dividerWorker{
		conf:      *conf,
		client:    client,
		knownWork: set.New[string](),

		storage: &workStorageImpl{
			client:        client,
			workerTimeout: conf.workerTimeout,
			rootKey:       conf.rootKey,
		},
		//start tickers and listeners
		newWorker: redisstreams.StreamListener{
			// Ctx:    ctx,
			Client: client,
			Key:    fmt.Sprintf("%s:%s", conf.rootKey, "new_worker"),
			// Callback: d.newWorkerEvent,
			Logger: conf.logger,
		},
		removeWorker: redisstreams.StreamListener{
			// Ctx:    ctx,
			Client: client,
			Key:    fmt.Sprintf("%s:%s", conf.rootKey, "remove_worker"),
			// Callback: d.removeWorkerEvent,
			Logger: conf.logger,
		},
		newWork: redisstreams.StreamListener{
			// Ctx:    ctx,
			Client: client,
			Key:    fmt.Sprintf("%s:%s", conf.rootKey, "new_work"),
			// Callback: d.newWorkEvent,
			Logger: conf.logger,
		},
		removeWork: redisstreams.StreamListener{
			// Ctx:    ctx,
			Client: client,
			Key:    fmt.Sprintf("%s:%s", conf.rootKey, "remove_work"),
			// Callback: d.removeWorkEvent,
			Logger: conf.logger,
		},
	}, nil
}

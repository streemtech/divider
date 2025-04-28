package redisconsistent

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/pkg/errors"
	redis "github.com/redis/go-redis/v9"
	"github.com/streemtech/divider"
)

// Work storage is responsible for storing the work required, the currently active workers, and determining what work needs to be assigned to what workers.
// It takes no action to manage the work or workers automatically, other than removing the workers that are timed out on each given check.
type WorkStorage interface {

	//Get work gets the full list of work between start and end.
	//if start is after end, the expectation is that the work will loop and underflow to end.
	//if start is equal to end, the expectation is that ALL data will be returned.
	//Will automatically tidy up any workers that have timed out.
	GetWorkFromKeyToNextWorkerKey(ctx context.Context, worker string) (workList []string, err error)

	//Returns true if the provided key is within the provided worker's work range.
	CheckWorkInKeyRange(ctx context.Context, worker string, key string) (inRange bool, err error)

	//Returns all work that needs to be done. Is normally done for sorting out what work needs to be completed.
	GetAllWork(ctx context.Context) (workList []string, err error)

	//Set the current time to have a score of key
	//this allows GetNextWorker to skip anything that has been timed out.
	UpdateTimeoutForWorkers(ctx context.Context, workers []string) error

	//This will remove a worker from the list of active workers.
	RemoveWorkers(ctx context.Context, workers []string) error

	//AddWork adds the given work to the work cycle.
	//The key is caluclated automatically from the value.
	AddWorkToDividedWork(ctx context.Context, value []string) error

	//remove work removes this value from the work cycle.
	//The key is calculated automatically from the value.
	RemoveWorkFromDividedWork(ctx context.Context, value []string) error
}

var _ WorkStorage = (*workStorageImpl)(nil)

type workStorageImpl struct {
	workerTimeout time.Duration
	client        redis.UniversalClient
	rootKey       string
}

// returns the key that all work & their IDs is stored in.
func (r *workStorageImpl) getDataKey() string {
	return r.rootKey + ":work"
}

// returns the key that all worker IDs are stored in.
func (r *workStorageImpl) getNodesKey() string {
	return r.rootKey + ":nodes"
}

// returns the key that all worker timeouts are stored in.
func (r *workStorageImpl) getTimeoutsKey() string {
	return r.rootKey + ":timeout"
}

// calculateKey is responsible for generating a consistent key for usage in
// certain work calculations.
func (r *workStorageImpl) calculateKey(value string) (key int64) {
	return divider.StringToIntHash(value)
}

// remove work removes this value from the work cycle.
// The key is calculated automatically from the value.
func (r *workStorageImpl) RemoveWorkFromDividedWork(ctx context.Context, values []string) error {

	for _, value := range values {

		keyStr := strconv.Itoa(int(r.calculateKey(value)))
		err := r.client.ZRemRangeByScore(ctx, r.getDataKey(), keyStr, keyStr).Err()
		if err != nil {
			return fmt.Errorf("failed to remove %s key by score: %w", value, err)
		}
	}
	return nil
}

// gets the next score looping positively and around if necessary.
func (r *workStorageImpl) getNextPotentialWorker(ctx context.Context, keyScore float64) (nextScore float64, err error) {

	//get from key's next score to the end.
	o1 := &redis.ZRangeBy{
		Min:   fmt.Sprintf("%f", keyScore+1),
		Max:   fmt.Sprintf("%d", math.MaxInt64),
		Count: 1,
	}
	res1, err := r.client.ZRangeByScoreWithScores(ctx, r.getNodesKey(), o1).Result()

	if err != nil {
		return 0.0, fmt.Errorf("failed to get first score range for next node key: %w", err)
	}

	//if len is not zero, use that score.
	if len(res1) > 0 {
		return res1[0].Score, nil
	}

	//if len is zero, loop to get next score.

	//get the FIRST possible key (IE the one after the loop.)
	o2 := &redis.ZRangeBy{
		Min:   fmt.Sprintf("%d", -math.MaxInt64),
		Max:   fmt.Sprintf("%d", math.MaxInt64),
		Count: 1,
	}
	rz := r.client.ZRangeByScoreWithScores(ctx, r.getNodesKey(), o2)
	res2, err := rz.Result()

	if err != nil {
		return 0.0, fmt.Errorf("failed to get second score range for next node key: %w", err)
	}

	//if the length returned is zero, get full range, otherwise, use value from results.
	if len(res2) <= 0 {
		//zero results returned on loop around.
		//assume no values in the list at all, and return key such that all work is fetched.
		return keyScore, nil
	}

	return res2[0].Score, nil
}

// remove all workers that have reached the timeout point so that they are no longer being counted by the system.
func (r *workStorageImpl) removeTimedOutWorkers(ctx context.Context) error {

	//get all workers that have passed the timeout range.
	toRemove, err := r.client.ZRangeByScoreWithScores(ctx, r.getTimeoutsKey(), &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", -math.MaxInt64),
		Max: fmt.Sprintf("%f", float64(time.Now().Add(-1*r.workerTimeout).UnixNano())),
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to get nodes to remove: %w", err)
	}

	//for all of those workers, remove them.
	return r.RemoveWorkers(ctx, divider.Map(toRemove, func(key redis.Z) string { return key.Member.(string) }))
}

// given a score, find the score of the next potential worker node. Will automatically wrap the scores.
func (r *workStorageImpl) getNextValidWorkerScore(ctx context.Context, keyScore float64) (nextScore float64, err error) {

	err = r.removeTimedOutWorkers(ctx)
	if err != nil {
		return 0.0, fmt.Errorf("failed to remove timed out workers: %w", err)
	}

	nextScore, err = r.getNextPotentialWorker(ctx, keyScore)
	if err != nil {
		return 0.0, fmt.Errorf("failed to get next valud worker: %w", err)
	}
	return nextScore, nil

}

// Get work gets the full list of work between passed in key, and the key of the next worker.
func (r *workStorageImpl) GetWorkFromKeyToNextWorkerKey(ctx context.Context, key string) (workList []string, err error) {
	startScore := float64(r.calculateKey(key))
	nextWorkScore, err := r.getNextValidWorkerScore(ctx, startScore)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate next working node score: %w", err)
	}

	//account for the scores being the same, and just get everything.
	if nextWorkScore == startScore {
		startScore = -math.MaxFloat64
		nextWorkScore = math.MaxFloat64
	}

	//if the scores are properly ordered, return that set of work.
	if startScore < nextWorkScore {
		var dat []string
		dat, err = r.client.ZRangeByScore(ctx, r.getDataKey(), &redis.ZRangeBy{
			Min: fmt.Sprintf("%f", startScore),
			Max: fmt.Sprintf("%f", nextWorkScore),
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get standard work range: %w", err)
		}
		return dat, nil
	}

	//if scores are not inverted, get from -max to end, combined with start to +max, and combine them.

	upper, err := r.client.ZRangeByScore(ctx, r.getDataKey(), &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", -math.MaxFloat64),
		Max: fmt.Sprintf("%f", nextWorkScore),
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to get first score range for work: %w", err)
	}

	lower, err := r.client.ZRangeByScore(ctx, r.getDataKey(), &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", startScore),
		Max: fmt.Sprintf("%f", math.MaxFloat64),
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to get second score range for work: %w", err)
	}

	return append(lower, upper...), nil
}

// Return literally all work from beginning of the list to the end of the list.
func (r *workStorageImpl) GetAllWork(ctx context.Context) (workList []string, err error) {
	workList, err = r.client.ZRangeByScore(ctx, r.getDataKey(), &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", -math.MaxFloat64),
		Max: fmt.Sprintf("%f", math.MaxFloat64),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("unable to get all work: %w", err)
	}
	return workList, nil
}

// Set the current time to have a score of key
// this allows GetNextWorker to skip anything that has been timed out.
func (r *workStorageImpl) UpdateTimeoutForWorkers(ctx context.Context, Key []string) error {

	nodeDat := make([]redis.Z, len(Key))
	timeoutDat := make([]redis.Z, len(Key))
	now := float64(time.Now().UnixNano())

	for i, key := range Key {
		keyInt := r.calculateKey(key)
		// keyString := strconv.Itoa(int(keyInt))

		nodeDat[i] = redis.Z{
			Member: key,
			Score:  float64(keyInt), //score is the
		}
		timeoutDat[i] = redis.Z{
			Member: key,
			Score:  now, //Score is when this item was added to the timeout sorted set.
		}
	}

	// pp.Println(nodeDat)

	err := r.client.ZAdd(ctx, r.getNodesKey(), nodeDat...).Err()
	if err != nil {
		return fmt.Errorf("failed to access zset for Nodes: %w", err)
	}
	err = r.client.ZAdd(ctx, r.getTimeoutsKey(), timeoutDat...).Err()
	if err != nil {
		return fmt.Errorf("failed to access zset for Timeouts: %w", err)
	}

	return nil
}

func (r *workStorageImpl) RemoveWorkers(ctx context.Context, keys []string) (err error) {

	// keyArgs := divider.Map(keys, func(key string) string { return strconv.Itoa(int(r.calculateKey(key))) })

	//nothing to remove. Return
	if len(keys) <= 0 {
		return nil
	}

	//remove the node from the list of nodes in the score lookup.
	err = r.client.ZRem(ctx, r.getNodesKey(), keys).Err()
	if err != nil {
		return fmt.Errorf("failed to remove data %v from Nodes: %w", keys, err)
	}
	//remove the node from the timeout set.
	err = r.client.ZRem(ctx, r.getTimeoutsKey(), keys).Err()
	if err != nil {
		return fmt.Errorf("failed to remove data %v from timeouts: %w", keys, err)
	}

	return nil
}

// AddWork adds the given work to the work cycle.
// The key is caluclated automatically from the value.
func (r *workStorageImpl) AddWorkToDividedWork(ctx context.Context, value []string) error {
	if len(value) <= 0 {
		return nil
	}
	dat := make([]redis.Z, len(value))
	for i, key := range value {
		dat[i] = redis.Z{
			Member: key,
			Score:  float64(r.calculateKey(key)),
		}
	}
	return r.client.ZAdd(ctx, r.getDataKey(), dat...).Err()
}

func (r *workStorageImpl) CheckWorkInKeyRange(ctx context.Context, worker string, key string) (inRange bool, err error) {
	keyScore := float64(r.calculateKey(key))
	startScore := float64(r.calculateKey(worker))
	nextWorkScore, err := r.getNextValidWorkerScore(ctx, startScore)
	if err != nil {
		return false, errors.Wrap(err, "failed to get next valid worker score")
	}

	// pp.Println(key, worker, keyScore, startScore, nextWorkScore, startScore < keyScore, keyScore < nextWorkScore)
	if nextWorkScore == startScore {
		return true, nil
	}

	if startScore < nextWorkScore {
		//return that its between the range.
		return startScore < keyScore && keyScore < nextWorkScore, nil
	}

	//return that key score is greater than start only (it wraps so its fine) or that its less than next only (again, it wraps so its fine.)
	return startScore < keyScore || keyScore < nextWorkScore, nil

}

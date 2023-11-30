package redisconsistent

import (
	"context"
	"fmt"
	"math"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type Worker interface {
	//CalculateKey is responsible for generating a consistent key for usage in
	//certain work calculations.
	CalculateKey(value string) (key int64)
	// //delete a particular worker takes in a key and deletes anything at that value.
	// //This is done for timed out keys.
	// DeleteWorker(ctx context.Context, key string) error

	//AddWork adds the given work to the work cycle.
	//The key is caluclated automatically from the value.
	AddWork(ctx context.Context, value string) error
	//remove work removes this value from the work cycle.
	//The key is calculated automatically from the value.
	RemoveWork(ctx context.Context, value string) error

	//Get work gets the full list of work between start and end.
	//if start is after end, the expectation is that the work will loop and underflow to end.
	//if start is equal to end, the expectation is that ALL data will be returned.
	GetFollowingWork(ctx context.Context, key string) (workList []string, err error)

	GetAllWork(ctx context.Context) (workList []string, err error)

	//Set the current time to have a score of key
	//this allows GetNextWorker to skip anything that has been timed out.
	UpdateWorkers(ctx context.Context, Key []string) error

	//AddWork adds the given work to the work cycle.
	//The key is caluclated automatically from the value.
	AddWorks(ctx context.Context, value []string) error
}

var _ Worker = (*redisWorkerImpl)(nil)

type redisWorkerImpl struct {
	timeout time.Duration

	r redis.UniversalClient
	// l divider.Informer

	//searchKey     is the key that is searched, <key>:* that
	masterKey string
}

func (r *redisWorkerImpl) getDataKey() string {
	return r.masterKey + ":work"
}

func (r *redisWorkerImpl) getNodesKey() string {
	return r.masterKey + ":nodes"
}

func (r *redisWorkerImpl) getTimeoutsKey() string {
	return r.masterKey + ":timeout"
}

// CalculateKey is responsible for generating a consistent key for usage in
// certain work calculations.
func (r *redisWorkerImpl) CalculateKey(value string) (key int64) {
	return StringToIntHash(value)
}

// AddWork adds the given work to the work cycle.
// The key is caluclated automatically from the value.
func (r *redisWorkerImpl) AddWork(ctx context.Context, value string) error {
	// if r.l != nil {
	// 	r.l.Debugf("Adding work %s", value)
	// }
	return r.r.ZAdd(ctx, r.getDataKey(), redis.Z{
		Score:  float64(r.CalculateKey(value)),
		Member: value,
	}).Err()
}

// remove work removes this value from the work cycle.
// The key is calculated automatically from the value.
func (r *redisWorkerImpl) RemoveWork(ctx context.Context, value string) error {
	// if r.l != nil {
	// 	r.l.Debugf("removing work %s", value)
	// }
	keyStr := fmt.Sprintf("%d", r.CalculateKey(value))
	return r.r.ZRemRangeByScore(ctx, r.getDataKey(), keyStr, keyStr).Err()
}

func (r *redisWorkerImpl) getNextPotentialWorker(ctx context.Context, keyScore float64) (nextScore float64, err error) {

	var re redis.Z
	//get from key's next score to the end.
	o1 := &redis.ZRangeBy{
		Min:   fmt.Sprintf("%f", keyScore+1),
		Max:   fmt.Sprintf("%d", math.MaxInt64),
		Count: 1,
	}
	res1, err := r.r.ZRangeByScoreWithScores(ctx, r.getNodesKey(), o1).Result()

	if err != nil {
		return 0.0, fmt.Errorf("failed to get first score range for next node key: %w", err)
	}

	//if len is zero, loop to get next score.
	//if len is not zero, use that.
	if len(res1) == 0 {

		//get the FIRST possible key (IE the one after the loop.)
		o2 := &redis.ZRangeBy{
			Min:   fmt.Sprintf("%d", -math.MaxInt64),
			Max:   fmt.Sprintf("%d", math.MaxInt64),
			Count: 1,
		}
		rz := r.r.ZRangeByScoreWithScores(ctx, r.getNodesKey(), o2)
		res2, err := rz.Result()

		if err != nil {
			return 0.0, fmt.Errorf("failed to get second score range for next node key: %w", err)
		}

		//if the length returned is zero, get full range, otherwise, use value from results.
		if len(res2) <= 0 {
			//zero results returned on loop around.
			//assume no values in the list at all, and return all work.
			return keyScore, nil
		} else {
			re = res2[0]
		}
	} else {
		//if len was 1 or more, use that first value as the next score.
		re = res1[0]
	}

	return re.Score, nil
}

func (r *redisWorkerImpl) removeTimedOutWorkers(ctx context.Context) error {

	//remove all workers who are out of bounds timewise.

	toRemove, err := r.r.ZRangeByScoreWithScores(ctx, r.getTimeoutsKey(), &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", -math.MaxInt64),
		Max: fmt.Sprintf("%f", float64(time.Now().Add(-1*r.timeout).UnixNano())),
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to get nodes to remove: %w", err)
	}

	//TODO1 replace this and determine why it was broken. Reokace loop with better option.
	for i, v := range toRemove {
		// toRemoveI[i] = v
		err = r.r.ZRem(ctx, r.getNodesKey(), v.Member).Err()
		if err != nil {
			return fmt.Errorf("failed to remove data %d from Nodes: %w", i, err)
		}
		err = r.r.ZRem(ctx, r.getTimeoutsKey(), v.Member).Err()
		if err != nil {
			return fmt.Errorf("failed to remove data %d from timeouts: %w", i, err)
		}
	}

	return nil

}

func (r *redisWorkerImpl) getNextValidWorker(ctx context.Context, keyScore float64) (nextScore float64, err error) {

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

// Get work gets the full list of work between key and the next worker.
func (r *redisWorkerImpl) GetFollowingWork(ctx context.Context, key string) (workList []string, err error) {
	startScore := float64(r.CalculateKey(key))
	nextWorkScore, err := r.getNextValidWorker(ctx, startScore)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate next working node score: %w", err)
	}

	if nextWorkScore == startScore {
		startScore = -math.MaxFloat64
		nextWorkScore = math.MaxFloat64
	}

	if startScore < nextWorkScore {
		dat, err := r.r.ZRangeByScore(ctx, r.getDataKey(), &redis.ZRangeBy{
			Min: fmt.Sprintf("%f", startScore),
			Max: fmt.Sprintf("%f", nextWorkScore),
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get standard work range: %w", err)
		}
		return dat, nil
	}

	upper, err := r.r.ZRangeByScore(ctx, r.getDataKey(), &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", -math.MaxFloat64),
		Max: fmt.Sprintf("%f", nextWorkScore),
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to get first score range for work: %w", err)
	}

	lower, err := r.r.ZRangeByScore(ctx, r.getDataKey(), &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", startScore),
		Max: fmt.Sprintf("%f", math.MaxFloat64),
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to get second score range for work: %w", err)
	}

	return append(lower, upper...), nil
}

func (r *redisWorkerImpl) GetAllWork(ctx context.Context) (workList []string, err error) {
	workList, err = r.r.ZRangeByScore(ctx, r.getDataKey(), &redis.ZRangeBy{
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
func (r *redisWorkerImpl) UpdateWorkers(ctx context.Context, Key []string) error {

	nodeDat := make([]redis.Z, len(Key))
	timeoutDat := make([]redis.Z, len(Key))
	for i, v := range Key {
		ks := r.CalculateKey(v)
		k := float64(ks)
		nodeDat[i] = redis.Z{
			Score:  k,
			Member: ks,
		}
		timeoutDat[i] = redis.Z{
			Score:  float64(time.Now().UnixNano()),
			Member: ks,
		}
	}

	// pp.Println(nodeDat)

	err := r.r.ZAdd(ctx, r.getNodesKey(), nodeDat...).Err()
	if err != nil {
		return fmt.Errorf("failed to access zset for Nodes: %w", err)
	}
	err = r.r.ZAdd(ctx, r.getTimeoutsKey(), timeoutDat...).Err()
	if err != nil {
		return fmt.Errorf("failed to access zset for Timeouts: %w", err)
	}

	return nil
}

// AddWork adds the given work to the work cycle.
// The key is caluclated automatically from the value.
func (r *redisWorkerImpl) AddWorks(ctx context.Context, value []string) error {
	if len(value) <= 0 {
		return nil
	}
	dat := make([]redis.Z, len(value))
	for i, v := range value {
		dat[i] = redis.Z{
			Member: v,
			Score:  float64(r.CalculateKey(v)),
		}
	}
	return r.r.ZAdd(ctx, r.getDataKey(), dat...).Err()
}

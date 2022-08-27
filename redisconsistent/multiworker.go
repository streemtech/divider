package redis_consistent

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	redis "github.com/go-redis/redis/v8"
)

type Worker interface {
	//CalculateKey is responsible for generating a consistent key for usage in
	//certain work calculations.
	CalculateKey(value string) (key int64)
	//Set the current time to have a score of key
	//this allows GetNextWorker to skip anything that has been timed out.
	UpdateWorker(ctx context.Context, Key string) error
	//delete a particular worker takes in a key and deletes anything at that value.
	//This is done for timed out keys.
	DeleteWorker(ctx context.Context, key string) error

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

	//searchKey     is the key that is searched, <key>:* that
	masterKey string
}

func (r *redisWorkerImpl) getDataKey() string {
	return r.masterKey + ":work"
}

func (r *redisWorkerImpl) getNodesKey() string {
	return r.masterKey + ":nodes"
}

//CalculateKey is responsible for generating a consistent key for usage in
//certain work calculations.
func (r *redisWorkerImpl) CalculateKey(value string) (key int64) {
	return StringToIntHash(value)
}

//Set the current time to have a score of key
//this allows GetNextWorker to skip anything that has been timed out.
func (r *redisWorkerImpl) UpdateWorker(ctx context.Context, Key string) error {
	return r.r.ZAddNX(ctx, r.getNodesKey(), &redis.Z{
		Score:  float64(r.CalculateKey(Key)),
		Member: time.Now().UnixNano(),
	}).Err()
}

//delete a particular worker takes in a key and deletes anything at that value.
//This is done for timed out keys.
func (r *redisWorkerImpl) DeleteWorker(ctx context.Context, key string) error {

	keyStr := fmt.Sprintf("%d", r.CalculateKey(key))
	return r.r.ZRemRangeByScore(ctx, r.getNodesKey(), keyStr, keyStr).Err()
}

//AddWork adds the given work to the work cycle.
//The key is caluclated automatically from the value.
func (r *redisWorkerImpl) AddWork(ctx context.Context, value string) error {
	return r.r.ZAdd(ctx, r.getDataKey(), &redis.Z{
		Score:  float64(r.CalculateKey(value)),
		Member: value,
	}).Err()
}

//remove work removes this value from the work cycle.
//The key is calculated automatically from the value.
func (r *redisWorkerImpl) RemoveWork(ctx context.Context, value string) error {
	keyStr := fmt.Sprintf("%d", r.CalculateKey(value))
	return r.r.ZRemRangeByScore(ctx, r.getDataKey(), keyStr, keyStr).Err()
}

func (r *redisWorkerImpl) getNextPotentialWorker(ctx context.Context, keyScore float64) (nextKey float64, keyData string, err error) {

	var re redis.Z
	//get from key's next score to the end.
	res, err := r.r.ZRangeByScoreWithScores(ctx, r.getNodesKey(), &redis.ZRangeBy{
		Min:   fmt.Sprintf("%f", keyScore),
		Max:   fmt.Sprintf("%f", math.MaxFloat64),
		Count: 1,
	}).Result()

	if err != nil {
		return 0.0, "", fmt.Errorf("failed to get first score range for next node key: %w", err)
	}
	//if len is zero, loop to get next score.
	//if len is not zero, use that.
	if len(res) == 0 {

		//get the FIRST possible key (IE the one after the loop.)
		res, err = r.r.ZRangeByScoreWithScores(ctx, r.getNodesKey(), &redis.ZRangeBy{
			Min:   fmt.Sprintf("%f", -math.MaxFloat64),
			Max:   fmt.Sprintf("%f", math.MaxFloat64),
			Count: 1,
		}).Result()

		if err != nil {
			return 0.0, "", fmt.Errorf("failed to get second score range for next node key: %w", err)
		}

		//if the length returned is zero, get full range, otherwise, use value from results.
		if len(res) <= 0 {
			//zero results returned on loop around.
			//assume no values in the list at all, and return all work.
			return keyScore, "", nil
		} else {
			re = res[0]
		}
	} else {
		//if len was 1 or more, use that first value as the next score.
		re = res[0]
	}

	dat, ok := re.Member.(string)
	if !ok {
		return 0.0, "", fmt.Errorf("failed to convert key data to string")
	}

	return re.Score, dat, nil
}

func (r *redisWorkerImpl) getNextValidWorker(ctx context.Context, keyScore float64) (nextKey float64, err error) {

	for {
		nextKey, timeout, err := r.getNextPotentialWorker(ctx, keyScore)
		if err != nil {
			return 0.0, fmt.Errorf("failed to get next valud worker: %w", err)
		}
		//special case of this is the only key
		if timeout == "" {
			return nextKey, nil
		}
		i, err := strconv.ParseInt(timeout, 10, 64)
		if err != nil {
			return 0.0, fmt.Errorf("failed to parse nextPotentialWorker returned timeout int: %w", err)
		}
		t := time.Unix(0, i)
		if t.After(time.Now().Add(r.timeout)) {
			scoreStr := fmt.Sprintf("%f", nextKey)
			err := r.r.ZRemRangeByScore(ctx, r.getNodesKey(), scoreStr, scoreStr).Err()
			if err != nil {
				return 0.0, fmt.Errorf("failed to delete timed out worker: %w", err)
			}
			continue
		}

	}
}

//Get work gets the full list of work between key and the next worker.
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

//Set the current time to have a score of key
//this allows GetNextWorker to skip anything that has been timed out.
func (r *redisWorkerImpl) UpdateWorkers(ctx context.Context, Key []string) error {

	dat := make([]*redis.Z, len(Key))
	for i, v := range Key {
		dat[i] = &redis.Z{
			Score:  float64(r.CalculateKey(v)),
			Member: time.Now().UnixNano(),
		}
	}

	return r.r.ZAddNX(ctx, r.getNodesKey(), dat...).Err()
}

//AddWork adds the given work to the work cycle.
//The key is caluclated automatically from the value.
func (r *redisWorkerImpl) AddWorks(ctx context.Context, value []string) error {
	dat := make([]*redis.Z, len(value))
	for i, v := range value {
		dat[i] = &redis.Z{
			Member: value,
			Score:  float64(r.CalculateKey(v)),
		}
	}
	return r.r.ZAdd(ctx, r.getDataKey(), dat...).Err()
}

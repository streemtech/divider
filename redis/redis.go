package redis

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/streemtech/divider"

	"github.com/go-redis/redis/v8"
)

//TODO4 split pubsub divider and regular divider?

//Divider is a redis backed implementation of divider.Divider. The
type Divider struct {
	redis       *redis.Client
	masterKey   string
	metaKey     string
	affinity    divider.Affinity
	mux         *sync.Mutex
	startChan   chan string
	stopChan    chan string
	done        context.Context
	doneCancel  context.CancelFunc
	uuid        string
	currentKeys map[string]bool
	informer    divider.Informer
	timeout     int // number of seconds to time out at.
}

//NewDivider returns a Divider using the Go-Redis library as a backend.
//The keys beginning in <masterKey>/__meta are used to keep track of the different metainformation to
func NewDivider(redis *redis.Client, masterKey string, informer divider.Informer, timeout int) *Divider {
	var i divider.Informer
	if informer == nil {
		i = divider.DefaultLogger{}
	} else {
		i = informer
	}
	var t int

	if timeout <= 0 {
		t = 10
	} else {
		t = timeout
	}
	return &Divider{
		redis:     redis,
		masterKey: masterKey,
		metaKey:   masterKey + ":__meta",
		affinity:  0,
		mux:       &sync.Mutex{},
		startChan: make(chan string),
		stopChan:  make(chan string),
		uuid:      uuid.New().String(),
		informer:  i,
		timeout:   t,
	}
}

//Start is the trigger to make the divider begin checking for keys, and returning those keys to the channels.
//No values should return to the channels without start being called.
func (r *Divider) Start() {
	r.mux.Lock()
	r.start()
	r.mux.Unlock()
}

//Stop begins the process of stopping processing of all assigned keys.
//Releasing these keys via stop allows them to immediately be picked up by other nodes.
//Start must be called to begin picking up work keys again.
func (r *Divider) Stop() {
	r.mux.Lock()
	r.stop()
	r.mux.Unlock()
}

//Close shuts down, closes and cleans up the process.
//If called before flushed, processing keys will be timed out instead of released.
func (r *Divider) Close() {
	r.mux.Lock()
	r.close()
	r.mux.Unlock()
}

//StopAndClose is a simple helper to call Stop and Close.
func (r *Divider) StopAndClose() {
	r.mux.Lock()
	r.stop()
	r.close()
	r.mux.Unlock()
}

//GetAssignedProcessingArray returns a string array that represents the keys that this node is set to process.
func (r *Divider) GetAssignedProcessingArray() []string {
	r.mux.Lock()
	v := r.getAssignedProcessingArray()
	r.mux.Unlock()
	return v
}

//GetReceiveStartProcessingChan returns a channel of strings.
//The string from this channel represents a key to a processable entity.
//This particular channel is for receiving keys that this node is to begin processing.
func (r *Divider) GetReceiveStartProcessingChan() <-chan string {
	return r.startChan
}

//GetReceiveStopProcessingChan returns a channel of strings.
//The string from this channel represents a key to a processable entity.
//This particular channel is for receiving keys that this node is to stop processing.
func (r *Divider) GetReceiveStopProcessingChan() <-chan string {
	return r.stopChan
}

//ConfirmStopProcessing takes in a string of a key that this node is no longer processing.
//This is to be used to confirm that the processing has stopped for a key gotten from the GetReceiveStopProcessingChan channel.
//To manually release processing of a key, use SendStopProcessing instead.
//ConfirmStopProcessing is expected to be required for the proper implementation of Flush()
func (r *Divider) ConfirmStopProcessing(key string) {
	r.informer.Errorf("Got confirm stop on %s. This divider only works on Pub Subs. Confirm Stop is not required as stop only occurs on pub sub close.", key)
}

//SendStopProcessing takes in a string of a key that this node is no longer processing.
//This is to be used to release the processing to another node.
//To confirm that the processing stoppage is completed, use ConfirmStopProcessing instead.
func (r *Divider) SendStopProcessing(key string) {
	r.sendStopProcessing(key)
}

//SetAffinity allows for the affinity to be set by the node so that some controll can be implemented over what nodes receive work.
func (r *Divider) SetAffinity(Affinity divider.Affinity) {
	r.mux.Lock()
	r.setAffinity(Affinity)
	r.mux.Unlock()
}

//GetAffinity returns the current affinity. Most cases that use get and then Set affinity would be better off using AlterAffinity.
func (r *Divider) GetAffinity() (Affinity divider.Affinity) {
	return r.affinity
}

//AlterAffinity takes in an integer and increases the current affinity by that amount.
//This method of updating the affinity should be implemented as concurrency safe.
func (r *Divider) AlterAffinity(Affinity divider.Affinity) {
	r.mux.Lock()
	r.setAffinity(r.affinity + Affinity)
	r.mux.Unlock()
}

func (r *Divider) start() {

	ctx, cancel := context.WithCancel(context.Background())
	r.done = ctx
	r.doneCancel = cancel
	r.currentKeys = make(map[string]bool)
	if r.affinity == 0 {
		r.affinity = 1000
	}
	r.setAffinity(r.affinity)
	go r.watchForKeys()

}

func (r *Divider) stop() {
	aff := r.GetAffinity()
	r.setAffinity(math.MaxInt64)
	r.doneCancel()
	r.done = nil
	r.doneCancel = nil
	keys := r.getAssignedProcessingArray()
	for _, key := range keys {
		r.sendStopProcessing(key)
	}
	r.setAffinity(aff)
	//pipe all the living ones through the sendStopProcessing.
}

func (r *Divider) close() {
	r.informer.Infof("Close call unnecessary for Redis Divider.")
	//Nothing happens here in this particular implementation as the connection to redis is passed in.
}

func (r *Divider) compareKeys() {
	//take in the keys from the remote, and compare them to the list of current keys. if there are any changes, pipe them to the listener.
	newKeys := r.getAssignedProcessingArray()
	toAdd := make(map[string]bool)
	newKeysMap := make(map[string]bool)
	for _, v := range newKeys {
		newKeysMap[v] = true
		_, ok := r.currentKeys[v]
		if !ok {
			toAdd[v] = true
		}
	}
	toRemove := make(map[string]bool)
	for k := range r.currentKeys {
		_, ok := newKeysMap[k]
		if !ok {
			toRemove[k] = true
		}
	}

	for key := range toAdd {
		r.startChan <- key
	}

	for key := range toRemove {
		r.stopChan <- key
	}
}

func (r *Divider) watchForKeys() {

	for {
		select {
		case <-time.After(time.Millisecond * 500):
			r.compareKeys()
		case <-r.done.Done():
			return
		}
	}
}

//Internal affinity setting.
func (r *Divider) setAffinity(Affinity divider.Affinity) {
	r.cleanup()
	r.affinity = Affinity
	key := r.metaKey + ":affinity"
	keys := []string{key}
	args := []string{r.uuid, strconv.Itoa(int(Affinity))}
	res, err := setAffinityScript.Run(r.done, r.redis, keys, args).Result()
	if err != nil && err.Error() != "redis: nil" {
		r.informer.Errorf(err.Error())
		return
	}
	r.informer.Infof("Redis Divider Affinity updated to %v (%T)", res, res)
}

func (r *Divider) sendStopProcessing(key string) {
	r.cleanup()
	keys := []string{r.metaKey + ":work"}
	args := []string{key}
	res, err := sendStopProcessingScript.Run(r.done, r.redis, keys, args).Result()
	if err != nil && err.Error() != "redis: nil" {
		r.informer.Errorf(err.Error())
		return
	}
	r.informer.Infof("%v", res)
}

func (r *Divider) getAssignedProcessingArray() []string {
	//This is where most of the `WORK` is done.
	r.cleanup()
	// --KEY1 __meta:work
	// --KEY2 __meta:assign:<UUID>
	// --KEY2 __meta:affinity
	keys := []string{r.metaKey + ":work", r.metaKey + ":assign:" + r.uuid, r.metaKey + ":affinity"}
	args := []string{r.masterKey + ":*", r.uuid}
	res, err := getAssignedProcessingScript.Run(r.done, r.redis, keys, args).Result()
	if err != nil && err.Error() != "redis: nil" {
		r.informer.Errorf(err.Error())
		return []string{}
	}
	//r.informer.Infof("RESPONSE: %v, %T", res, res)
	return format(res)
	//return []string{}
}

func format(in interface{}) (out []string) {
	arr := in.([]interface{})
	out = make([]string, len(arr))
	for i, v := range arr {
		out[i] = v.(string)
	}
	return out
}

var cleanupScript = redis.NewScript(`
--KEY1 timeoutKey (where Arg1's timout is to be reset.)
--KEY2 affinity  (where all affinities are stored)
--KEY3 workKey (where all the work is assigned.)
--ARG1 uuid
--ARG2 timeout seconds

--update the timeout on this ticker to be the most up to date.
redis.call('setex', KEYS[1] .. ARGV[1], tonumber(ARGV[2]), ARGV[1])

--check for keys without timeout. 
local nodes = redis.call('hkeys', KEYS[2]) --get everything in affinity.
local work = redis.call("hgetall",KEYS[3]) --get the work assignments.

local workKeys = {}
local workerNodes = {}

--Set the key/value lists for the work asignments.
for i,vgetVal in ipairs(work) do
	if (tonumber(i) % 2 == 1) then
		table.insert(workKeys, vgetVal)
	else
		table.insert(workerNodes, vgetVal)
	end
end

for key,missingNode in pairs(nodes) do
	local nodeKey = KEYS[1] .. missingNode
	local isThere = redis.call('exists', nodeKey)
	--redis.log(3, "value " .. nodeKey .. " IS " .. isThere)

	--if the timeout key is NO there (IE it is no longer updating)
	if isThere == 0 then 


		--delete from the work list.
		redis.call('hdel', KEYS[2], missingNode) 

		--for the list of work noddes (the node it was assigned to)
		for workerIDX,workerNode in ipairs(workerNodes) do
			--redis.log(3, "checking " .. workerNode .. " against " .. missingNode)
			if (workerNode == missingNode) then
				--redis.log(3, "same value.")
				redis.call("hdel", KEYS[3], workKeys[workerIDX])
			end
		end

	end
end

`)

func (r *Divider) cleanup() {
	timeoutKey := r.metaKey + ":timeout:"

	affinity := r.metaKey + ":affinity"
	workKey := r.metaKey + ":work"
	keys := []string{timeoutKey, affinity, workKey}
	args := []string{r.uuid, strconv.Itoa(r.timeout)}
	_, err := cleanupScript.Run(r.done, r.redis, keys, args).Result()
	if err != nil && err.Error() != "redis: nil" {
		r.informer.Errorf(err.Error())
		return
	}
	//r.informer.Infof("CLEANUP %v (%T)", res2, res2)

}

var setAffinityScript = redis.NewScript(`
redis.call('hset', KEYS[1], ARGV[1], ARGV[2] )
local setVal = redis.call('hget', KEYS[1], ARGV[1])
--redis.log(3, "set affinity " .. setVal .. " at " .. KEYS[1])
return setVal
`)

var sendStopProcessingScript = redis.NewScript(`
--ARG1 The Work ID
--KEY1 __meta:work

redis.call('hdel' KEYS[1], ARGV[1]) -- delete from the list of work-node, the asigned work 
`)

var getAssignedProcessingScript = redis.NewScript(`
--ARG1 The lookup key.
--ARG2 The node key.

--KEY1 __meta:work
--KEY2 __meta:assign:<UUID>
--KEY3 __meta:affinity


-- get the channels currently open.
local openChannels = redis.call('pubsub', 'channels', ARGV[1])

local toAssigns = {}
for _,value in ipairs(openChannels) do
	local worker = redis.call('hget', KEYS[1], value)

	--check if I need to set add a worker to anything.
	if (worker == false) then
		--redis.log(3, "no worker for " .. value)
		table.insert(toAssigns, value)
		--get the node to asign this one to. 
	end
end

--return toAssigns
if ( table.getn(toAssigns) > 0) then
	local affKeys = {}
	local affVals = {}
	local total = 0
	local aff = redis.call("hgetall",KEYS[3])


	
	--Set the key/value lists. 
	for i,value in ipairs(aff) do
		
		if (tonumber(i) % 2 == 1) then
			table.insert(affKeys, value)
		else
			total = total + tonumber(value)
			table.insert(affVals, value)
		end
	end



	--loop through and, for each one that needs to be assigned, assign it to a value based on the total 
	for _,work in ipairs(toAssigns) do
		local marker = math.random(0,total)
		local sum = 0

		for i,val in ipairs(affVals) do
			sum = sum + tonumber(val)
			if marker < sum then
				--Add to the key at that value.
				--redis.log(3, "assigning woker " .. affKeys[i] .. " to " .. work)
				redis.call('hmset', KEYS[1] , work, affKeys[i] )
				break
			end
		end
	end
end

local work = redis.call("hgetall",KEYS[1]) --get the work assignments.

local workKeys = {}
local workerNodes = {}

--Set the key/value lists for the work asignments.
for i,vgetVal in ipairs(work) do
	if (tonumber(i) % 2 == 1) then
		table.insert(workKeys, vgetVal)
	else
		table.insert(workerNodes, vgetVal)
	end
end

local doneWork = {}

for i,node in ipairs(workerNodes) do
	--redis.log(3, "checking " .. node .. " Vs " .. ARGV[2])
	if (node == ARGV[2]) then
		table.insert(doneWork, workKeys[i])
	end
end

--TODO convert this to actually do a lookup instead of 
return doneWork
`)

/*

all data prefexed with <masterKey>:__meta:

timeout stored as :timeout:<UUID>
affinity stored in hmset :affinity
work is assigned in hmset :work
	the key is the work
	the value is the <UUID> of the node it is assigned to.
the list of work each node has is in set :assign:<UUID>

//the list of active work is read based on the list of pub-subs
//based on that list, it is compared to the list of known work.
//any known ones are ignored.
//unasigned ones are divied out based on the CURRENT affinity.

work list is hmset :work
	key is the workID, value is the node UUID


*/

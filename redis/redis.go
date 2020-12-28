package redis

import (
	"context"
	"math"
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
		r.currentKeys[key] = true
	}

	for key := range toRemove {
		r.stopChan <- key
		delete(r.currentKeys, key)
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
	r.affinity = Affinity
}

func (r *Divider) sendStopProcessing(key string) {
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
	/*
		-- main.lua is responsible for the cleanup and running of this implementaiton of the divider
		-- KEY1 is the poll meta key. (poll:__meta)
		-- KEY2 is the poll master key (poll)
		-- ARG1 UUID of this node
		-- ARG2 Current Time (Unix Seconds)
		-- ARG3 timeout time (N Seconds)
		-- ARG4 affinity
	*/

	keys := []string{r.metaKey, r.masterKey}
	args := []interface{}{r.uuid, time.Now().Unix(), r.timeout, int(r.affinity)}
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

var sendStopProcessingScript = redis.NewScript(`
--ARG1 The Work ID
--KEY1 __meta:work

redis.call('hdel' KEYS[1], ARGV[1]) -- delete from the list of work-node, the asigned work 
`)

//TODO1 need some way to remove a key from the list of work.
var getAssignedProcessingScript = redis.NewScript(`-- main.lua is responsible for the cleanup and running of this implementaiton of the divider
-- KEY1 is the poll meta key. (poll:__meta)
-- KEY2 is the poll master key (poll)
-- ARG1 UUID of this node
-- ARG2 Current Time (Unix Seconds)
-- ARG3 timeout time (N Seconds)
-- ARG4 affinity


--need some way to store timeouts.
--need some way to check what work is assigned to what node
--

--**********************************
local metaKey = KEYS[1]
local masterKey = KEYS[2]
local nodeUUID = ARGV[1]
local currentTime = ARGV[2]
local timeout = ARGV[3]
local deadTime = currentTime-timeout
local nodeAffinity = ARGV[4]


local nodeSetKey = metaKey .. ":nodes"
local affinityKey = metaKey .. ":affinity:affinity"
local chanceKey = metaKey .. ":assignmentChance"
local workKey = metaKey..":work:assigned" --work stores where the work is assigned. work:id stores what work was assigned to that key.
local currentWorkKey = metaKey..":work:current"
local oldWorkKey = metaKey..":work:old"

local function log() 
	--redis.log(3, "\nmetaKey:" .. metaKey ..
	-- "\nmasterKey: " .. masterKey ..
	-- "\nnodeUUID: " .. nodeUUID ..
	-- "\ncurrentTime: " .. currentTime ..
	-- "\ntimeout: " .. timeout ..
	-- "\ndeadTime: " .. deadTime ..
	-- "\nnodeAffinity: " .. nodeAffinity ..
	-- "\nnodeSetKey: " .. nodeSetKey ..
	-- "\naffinityKey: " .. affinityKey ..
	-- "\nchanceKey: " .. chanceKey ..
	-- "\nworkKey: " .. workKey )

end




--set the work to having that node for it;s worker, and add the work to the set of work that this node is working on.
local function assignWorkToNode(workId, NodeId) 
	--redis.log(3, "asigning work To Node: " .. workId .." Node: ".. NodeId)
	redis.call("hset", workKey, workId, NodeId) --set what node is working on this work
	redis.call("sadd", workKey..":"..NodeId, workId) --set that this node has this work. 
end

local function assignWork (workId)
	--redis.log(3, "Asigning Work: ".. workId)
	local node = redis.call("ZRANGEBYSCORE", chanceKey, math.random(),2,  "LIMIT", 0 ,1)
	assignWorkToNode(workId, node[1])
end

local function deleteWork (workId)
	--redis.log(3, "deleting work: ".. workId)
	local worker = redis.call("hget", workKey, workId) --get what node is working on this work
	redis.call("hdel", workKey, workId)
	redis.call("srem", workKey..":"..worker)

end

--update the list of affinities's chances in the key. wont update if not needed.
local function updateAffinityChances()
	--redis.log(3, "updating Affinity.")


	local sum = 0;
	local affinityVals = redis.call("hvals", affinityKey)
	for _,val in ipairs(affinityVals) do

		sum = sum  + val
	end


	redis.call("del", chanceKey)
	local affinities = redis.call("hgetall", affinityKey)

	local score = 0
	local key = ""
	local weight = 0
	for i,val in ipairs(affinities) do

		if (i % 2 == 1) then
			key = val
		else
			weight = weight + val/sum
			redis.call("zadd", chanceKey, weight, key)
		end
	end

	redis.call("zadd", chanceKey, 1, key)

end




--update the affinity for a specific node, and update the sum affinity.
local function updateAffinity (id, affinity)
	--redis.log(3, "updating affinity " .. id .." to ".. affinity)
	local exists = redis.call("hexists", affinityKey, id)

	if exists == 1 then
		local current = redis.call("hget", affinityKey, id)
		redis.call("hset", affinityKey, id, affinity)
	else
		redis.call("hset", affinityKey, id, affinity)
	end
end

--remove a node from the different lists, removing its affinity. also remove work not assigned from the list.
local function deleteNode (id)
	--redis.log(3, "deleting node " .. id )
	redis.call("zrem", nodeSetKey, id )
	redis.call("hdel", affinityKey, id)

	local nodeWorkKey = workKey..":"..id
	--go through the work that this node was assigned and remove it.
	local assignedWork = redis.call("SMEMBERS", nodeWorkKey)
	for _,work in ipairs(assignedWork) do
		redis.call("hdel", workKey, work)
	end
	redis.call("del", nodeWorkKey)
end

--update values to make sure that this node is recorded.
local function update()
	--redis.log(3,"update")
	--Update set of live nodes to contain self, with current time as the score.
	redis.call('zadd', nodeSetKey,  currentTime, nodeUUID)
	--update the affinity.
	updateAffinity(nodeUUID, nodeAffinity)
end

--clean up all old nodes, if they have been dropped.
local function cleanup()
	--redis.log(3,"cleanup")
	local deadNodes = redis.call('ZRANGEBYSCORE', nodeSetKey, 0, deadTime)
	--Set the key/value lists for the work asignments.
	for _,deadNode in ipairs(deadNodes) do
		deleteNode(deadNode)
	end
end


--assign work to nodes.
local function assign()
	--redis.log(3, "asigning.")
	
	--get the current work
	local work = redis.call("pubsub", "channels", masterKey..":*")
	--get the previous work
	local oldWork = redis.call("hkeys", workKey)

	--save the current work to currentWorkKey
	redis.call("del", currentWorkKey)
	if #work >0 then
		redis.call("sadd", currentWorkKey, unpack(work))
	else
		--redis.log(3, "no work to assign")
	end
	--savePreviousWork to previousWorkKey
	redis.call("del", oldWorkKey)
	if #oldWork > 0 then
		redis.call("sadd", oldWorkKey, unpack(oldWork))
	else
		--redis.log(3, "no old work to assign")
	end
	--get the difference between the two works
	local toDelete = redis.call("sdiff", oldWorkKey, currentWorkKey)
	local toAdd = redis.call("sdiff", currentWorkKey, oldWorkKey)
	
	for _,work in ipairs(toDelete) do
		deleteWork(work)
	end

	if #toAdd >0 then 
		updateAffinityChances()
		for _,work in ipairs(toAdd) do
			assignWork(work)
		end
	end
end


log()

update()
cleanup()
assign()

return redis.call("smembers", workKey..":"..nodeUUID)`)

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

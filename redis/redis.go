package redis

import (
	"context"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/streemtech/divider"

	cache "github.com/go-redis/cache/v8"
	redis "github.com/go-redis/redis/v8"
)

//TODO4 split pubsub divider and regular divider?

//Divider is a redis backed implementation of divider.Divider. The
type Divider struct {
	redis      *redis.Client
	redisCache *cache.Cache

	//searchKey     is the key that is searched, <key>:* that
	searchKey string

	//infoKey       is a string stored after <key>:__meta:info. It is a string object that is parsed and stores the required information to split work.
	infoKey string

	//masterKey     is a string stored after <key>:__meta:master. It is a string that holds the UUID of the master. the only instance allowed to edit info.
	masterKey string

	//updateTimeKey is a string stored after <key>:__meta:lastUpdate. It is a set that holds the last update times of all nodes. It is used to determine what nodes have offlined.
	updateTimeKey string

	//affinityKey is a string stored after <key>:__meta:affinity. It is a set that holds the last update times of all nodes. It is used to determine what nodes have offlined.
	affinityKey string

	affinity      divider.Affinity
	mux           *sync.Mutex
	startChan     chan string
	stopChan      chan string
	done          context.Context
	doneCancel    context.CancelFunc
	uuid          string
	currentKeys   map[string]bool
	informer      divider.Informer
	timeout       int // number of seconds to time out at.
	masterTimeout int //timeout that the master can be gone for.
}

//NewDivider returns a Divider using the Go-Redis library as a backend.
//The keys beginning in <masterKey>:__meta are used to keep track of the different metainformation to
//:* is appended automatically!!
func NewDivider(redis *redis.Client, masterKey, name string, informer divider.Informer, timeout, masterTimeout int) *Divider {
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

	if masterTimeout <= 0 {
		t = 10
	} else {
		t = masterTimeout
	}

	//set the UUID.
	UUID := name
	if UUID == "" {
		UUID = uuid.New().String()
	}

	status, err := redis.Ping(context.TODO()).Result()
	if err != nil {
		informer.Errorf("ping error: %s", err.Error())
	}
	informer.Infof("Ping status result: %s", status)
	d := &Divider{
		redis:         redis,
		searchKey:     masterKey + ":*",
		infoKey:       masterKey + ":__meta:info",
		masterKey:     masterKey + ":__meta:master",
		updateTimeKey: masterKey + ":__meta:lastUpdate",
		affinityKey:   masterKey + ":__meta:affinity",
		affinity:      0,
		mux:           &sync.Mutex{},
		startChan:     make(chan string),
		stopChan:      make(chan string),
		uuid:          UUID,
		informer:      i,
		timeout:       t,
		masterTimeout: masterTimeout,
	}

	return d
}

//Start is the trigger to make the divider begin checking for keys, and returning those keys to the channels.
//No values should return to the channels without start being called.
func (r *Divider) Start() {
	r.mux.Lock()
	//make so I cant start multiple times.
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
	r.watch()

}

func (r *Divider) stop() {
	aff := r.GetAffinity()
	r.setAffinity(0)
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
	r.informer.Errorf("Close call unnecessary for Redis Divider.")
	//Nothing happens here in this particular implementation as the connection to redis is passed in.
}

func (r *Divider) compareKeys() {
	defer func() {
		if rec := recover(); rec != nil {
			r.informer.Errorf("Forced to recover from panic in compare keys: %v", r)
		}
	}()

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
		//TODO5 make only so that it sends if the chan has been grabbed.
		r.startChan <- key
		r.currentKeys[key] = true
	}

	for key := range toRemove {
		//TODO5 make only so that it sends if the chan has been grabbed.
		r.stopChan <- key
		delete(r.currentKeys, key)
	}
}

func (r *Divider) watch() {

	go r.watchForKeys()
	go r.watchForUpdates()
}

//watchForKeys is a function looped to constantly look for new keys that need results output to them.
func (r *Divider) watchForKeys() {

	for {
		select {
		case <-time.After(time.Millisecond * 500):
			r.compareKeys()
			r.updatePing()
		case <-r.done.Done():
			return
		}
	}
}

//watchForUpdates is a function looped to constantly check the system and make sure that the work is divided right.
func (r *Divider) watchForUpdates() {

	for {
		select {
		case <-time.After(time.Millisecond * 500):
			r.updateAssignments()
			r.updatePing()
		case <-r.done.Done():
			return
		}
	}
}

//Internal affinity setting.
func (r *Divider) setAffinity(Affinity divider.Affinity) {
	r.informer.Infof("Setting affinity for %s to %d", r.uuid, Affinity)
	r.affinity = Affinity
}

func (r *Divider) sendStopProcessing(key string) {

}

func (r *Divider) getAssignedProcessingArray() []string {

	data2 := new(DividerData)
	err := r.redisCache.GetSkippingLocalCache(context.TODO(), r.infoKey, data2)

	if err != nil {
		r.informer.Errorf("Unable to get data.")
	}
	work := data2.NodeWork[r.uuid]

	idx := 0
	assigned := make([]string, len(work))
	for i := range work {
		assigned[idx] = i
		idx++
	}
	return assigned
}

//updatePing is used to consistently tell the system that the worker is online, and listening to work.
func (r *Divider) updatePing() {
	defer func() {
		if rec := recover(); rec != nil {
			r.informer.Errorf("Forced to recover from panic in update Ping: %v", r)
		}
	}()

	r.redis.HSet(r.done, r.updateTimeKey, r.uuid, time.Now().UnixNano())

	//set the master key to this value if it does not exist.
	set, err := r.redis.SetNX(r.done, r.masterKey, r.uuid, time.Second*time.Duration(r.masterTimeout)).Result()
	if err != nil {
		r.informer.Errorf("update if master does not exist error: %s!", err.Error())
		return
	}
	if set {
		r.informer.Infof("The master was set to this node: %s", r.uuid)
	}

	r.redis.HSet(r.done, r.affinityKey, r.uuid, int64(r.affinity))

}

//updates all assignments if master.
func (r *Divider) updateAssignments() {

	defer func() {
		if rec := recover(); rec != nil {
			r.informer.Errorf("Forced to recover from panic in update Assignments: %v", r)
		}
	}()

	//check the master key.
	master, err := r.redis.Get(r.done, r.masterKey).Result()
	if err != nil {
		r.informer.Errorf("get current master error: %s!", err.Error())
		return
	}

	//if this is the master, run the update (setting the timeout to 3 seconds)
	if master == r.uuid {
		_, err = r.redis.Set(r.done, r.masterKey, r.uuid, time.Second*time.Duration(r.masterTimeout)).Result()
		if err != nil {
			r.informer.Errorf("update master timeout error: %s!", err.Error())
			return
		}
		err = r.updateData()

		if err != nil {
			r.informer.Errorf("Error when updating data: %s!", err.Error())
			return
		}

	}
	return

}

//updateData does the work to keep track of the work distribution.
//This work should only be done by the master.
func (r *Divider) updateData() error {
	data2 := new(DividerData)
	err := r.redisCache.GetSkippingLocalCache(context.TODO(), r.infoKey, data2)

	if err != nil {
		if errors.Is(err, cache.ErrCacheMiss) {
			r.informer.Infof("Generated ")
			data2 = &DividerData{
				Worker:   make(map[string]string),
				NodeWork: make(map[string]map[string]bool),
			}
		} else {
			r.informer.Errorf("Error when attempting to get data from cache")
			return err
		}
	}

	//get the list of update times.
	timeoutStrings, err := r.redis.HGetAll(r.done, r.updateTimeKey).Result()
	if err != nil {
		r.informer.Errorf("Error when attempting to load timeouts")
		return err
	}
	timeout := time.Now().Add(time.Second * -1 * time.Duration(r.timeout))
	//convert the timeouts from strings to ints.
	timeouts := make(map[string]int64)
	for k, v := range timeoutStrings {
		z, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			r.informer.Errorf("Error when attempting to convert timeouts to ints")
			return err
		}
		//add only if the time is reasonable.
		t := time.Unix(0, z)
		if timeout.Before(t) {
			timeouts[k] = z
		} else {
			r.informer.Infof("Timed out %s. Last updated %s", k, t.String())
		}

	}

	//get the worker affinity values.
	affinityStrings, err := r.redis.HGetAll(r.done, r.affinityKey).Result()
	if err != nil {
		r.informer.Errorf("Error when attempting to ")
		return err
	}

	//convert the worker affinities to ints.
	affinities := make(map[string]int64)
	for k, v := range affinityStrings {
		affinities[k], err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			r.informer.Errorf("Unable to convert %s to affinity for node %s", v, k)
		}
	}

	//get most up to date list of subscriptions.
	work, err := r.redis.PubSubChannels(r.done, r.searchKey).Result()
	if err != nil {
		r.informer.Errorf("Unable to get list of work : %s", err.Error())
		return err
	}

	data2.calculateNewWork(work, timeouts, affinities)

	r.redisCache.Set(&cache.Item{
		Ctx:            context.TODO(),
		Key:            r.infoKey,
		SkipLocalCache: true,
		Value:          data2,
		//TTL not set intentionally. Should be updated continuously, and if not updated, I don't thing I want it staying.
	})
	return nil

}

type NodeCountChange struct {
	Node        string
	ChangeCount int
}

//NodeCountChangesSort is used to implement a sorter on NodeChange.
type NodeCountChangesSort []NodeCountChange

func (a NodeCountChangesSort) Len() int      { return len(a) }
func (a NodeCountChangesSort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a NodeCountChangesSort) Less(i, j int) bool {
	if a[i].ChangeCount > a[j].ChangeCount {
		return true
	}
	if a[i].ChangeCount < a[j].ChangeCount {
		return false
	}
	return strings.Compare(a[i].Node, a[j].Node) > 0
}

//DividerData is a backup of the dividerData
type DividerData struct {
	//Worker is map[workId]workerNode so that you can look up the worker based on the work
	Worker map[string]string
	//NodeWork is a map[workerNode]map[work]true to allow for looking up what work a particular node is working on
	NodeWork map[string]map[string]bool
}

func (r *DividerData) calculateNewWork(workList []string, nodeList map[string]int64, affinities map[string]int64) {
	nodesToRemove := r.getNodesToRemove(nodeList)
	for _, v := range nodesToRemove {
		r.removeNode(v)
	}

	workToAdd, workToRemove := r.getWorkToAddAndRemove(workList)

	for _, v := range workToRemove {
		r.removeWork(v)
	}

	//changes is a map of []{node,change} (sorted based on the change count from highest to lowest)
	//changes does not contain any changes that are less than 1.
	changes := r.calculateWorkCountChange(affinities, len(workList))

	//applyWorkToNodes goes through the list of changes
	r.applyWorkToNodes(changes, workToAdd)
}

func (r *DividerData) applyWorkToNodes(changes []NodeCountChange, workToAdd map[string]string) {

	count := len(r.NodeWork)
	if count == 0 {
		return
	}
	changesIDX := 0
	for work := range workToAdd {
		index := changesIDX % count

		r.addWorkToNode(work, changes[index].Node)
		changes[index].ChangeCount--

		if changes[index].ChangeCount <= 0 {
			changesIDX++
		}
	}

}

func (r *DividerData) calculateWorkCountChange(affinities map[string]int64, workCount int) (changes []NodeCountChange) {
	changes = make([]NodeCountChange, 0)
	sum := 0
	for _, v := range affinities {
		if v < 0 {
			continue
		}
		sum += int(v)
	}

	workPerAff := float64(workCount) / float64(sum)

	//calculate the total per node, rounding down.
	//nodeWorkCount is a map[nodeID]workCount so that you can look up how much work node N is assigned.
	nodeWorkCount := make(map[string]int)
	total := 0
	for k, v := range affinities {
		nodeWorkCount[k] = int(math.Floor(float64(v) * workPerAff))
		total += nodeWorkCount[k]
	}

	for node, ExpectedWorkCount := range nodeWorkCount {
		currentWorkCount := len(r.NodeWork[node])
		change := ExpectedWorkCount - currentWorkCount
		if change > 0 {
			changes = append(changes, NodeCountChange{
				Node:        node,
				ChangeCount: change,
			})
		}
	}

	sort.Sort(NodeCountChangesSort(changes))
	return changes
}

func (r *DividerData) getWorkToAddAndRemove(workList []string) (toAdd, toRemove map[string]string) {
	toAdd = make(map[string]string)
	toRemove = make(map[string]string)
	workMap := make(map[string]string)

	//loop through the list of work
	for _, work := range workList {
		workMap[work] = work

		//if the work is not currently assigned, add to the list of work to add
		_, ok := r.Worker[work]
		if !ok {
			toAdd[work] = work
		}
	}

	//loop through all work currently assigned,
	for _, work := range r.Worker {

		//if the work is NOT in the list of work, add it to the list to remove.
		_, ok := workMap[work]
		if !ok {
			toRemove[work] = work
		}
	}
	return toAdd, toRemove
}

func (r *DividerData) getNodesToRemove(nodeList map[string]int64) (toRemove map[string]string) {
	toRemove = make(map[string]string)
	for node := range nodeList {
		_, ok := r.NodeWork[node]
		if !ok {
			toRemove[node] = node
		}
	}
	return toRemove
}

func (r *DividerData) addWorkToNode(workID, nodeID string) {
	r.NodeWork[nodeID][workID] = true
	r.Worker[workID] = nodeID
}
func (r *DividerData) removeWorkFromNode(workID, nodeID string) {
	delete(r.Worker, workID)
	delete(r.NodeWork[nodeID], workID)
}
func (r *DividerData) removeNode(nodeID string) {
	for work := range r.NodeWork[nodeID] {
		r.removeWorkFromNode(work, nodeID)
	}
	delete(r.NodeWork, nodeID)
	//TODO remove node from hset of affinity and lastUpdate
}
func (r *DividerData) addNode(nodeID string) {
	r.NodeWork[nodeID] = make(map[string]bool)
}
func (r *DividerData) removeWork(workID string) {
	r.removeWorkFromNode(workID, r.Worker[workID])
	delete(r.Worker, workID)
}

func (r *DividerData) removeItemsFromNode(nodeID string, items int) (removed []string) {
	removed = make([]string, items)
	idx := 0
	for work := range r.NodeWork[nodeID] {
		if idx >= items {
			return removed
		}
		removed[idx] = work
		r.removeWorkFromNode(work, nodeID)
		idx++
	}
	return removed
}

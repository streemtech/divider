package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

//Divider is a redis backed implementation of divider.Divider. The
type Divider struct {
	redis redis.UniversalClient
	redis.Client
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
	JSON          bool

	workFetcher divider.WorkFetcher
}

//NewDivider returns a Divider using the Go-Redis library as a backend.
//The keys beginning in <masterKey>:__meta are used to keep track of the different
//metainformation to divide the work.
//
//:* is appended automatically to the work when using default work fetcher.!!
func NewDivider(r redis.UniversalClient, masterKey, name string, informer divider.Informer, timeout, masterTimeout int) *Divider {
	var i divider.Informer
	if informer == nil {
		i = divider.DefaultLogger{}
	} else {
		i = informer
	}
	var t int
	var mt int
	if timeout <= 0 {
		t = 10
	} else {
		t = timeout
	}

	if masterTimeout <= 0 {
		mt = 10
	} else {
		mt = masterTimeout
	}

	//set the UUID.
	UUID := name
	if UUID == "" {
		UUID = uuid.New().String()
	}

	status, err := r.Ping(context.TODO()).Result()
	if err != nil {
		informer.Errorf("ping error: %s", err.Error())
	}
	informer.Infof("Ping status result: %s", status)

	d := &Divider{
		redis: r,
		// redisCache: cache.New(&cache.Options{
		// 	Redis: redis,
		// }),
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
		masterTimeout: mt,
		JSON:          true,
	}
	d.workFetcher = func(ctx context.Context, key string) ([]string, error) {
		return d.redis.PubSubChannels(d.done, d.searchKey).Result()
	}

	return d
}

//SetWorkFetcher tells the deivider how to look up the list of work that needs to be done.
func (r *Divider) SetWorkFetcher(f divider.WorkFetcher) error {
	if f == nil {
		return fmt.Errorf("work divider can not be nill")
	}
	r.workFetcher = f
	return nil
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
			r.informer.Errorf("Forced to recover from panic in compare keys: %v", rec)
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
			r.updatePing()
			r.updateAssignments()
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
	//TODO1 implement sendStopProcessing as a way for this node to say I am done.
}

func (r *Divider) getAssignedProcessingArray() []string {

	data, err := r.getInfo()

	if err != nil {
		r.informer.Errorf("Unable to get assigned processing array: %s.", err.Error())
	}
	work := data.NodeWork[r.uuid]

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
	r.redis.HSet(r.done, r.updateTimeKey, r.uuid, time.Now().Unix())

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
			r.informer.Errorf("Forced to recover from panic in update Assignments: %+v: %+v", r, rec)
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

}

//updateData does the work to keep track of the work distribution.
//This work should only be done by the master.
func (r *Divider) updateData() error {
	data, err := r.getInfo()
	data.divider = r
	if err != nil {
		if errors.Is(err, cache.ErrCacheMiss) {
			r.informer.Infof("Generated ")
			data = &DividerData{
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
	cutoff := time.Now().Unix() - int64(r.timeout)
	//if the last update time is before three seconds ago
	//convert the timeouts from strings to ints.
	timeoutList := make(map[string]int64)
	nodeList := make(map[string]int64)
	for k, v := range timeoutStrings {
		z, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			r.informer.Errorf("Error when attempting to convert timeouts to ints")
			return err
		}
		//add only if the time is reasonable.
		if cutoff < z {
			nodeList[k] = z
		} else {
			timeoutList[k] = z
			r.informer.Infof("Timed out %s. Last updated %s", k, time.Unix(z, 0))
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

	//get most up to date list of subscriptions
	work, err := r.workFetcher(r.done, r.searchKey)
	if err != nil {
		r.informer.Errorf("Unable to get list of work : %s", err.Error())
		return err
	}

	workList := make(map[string]string)
	for _, v := range work {
		workList[v] = v
	}

	data.calculateNewWork(workList, nodeList, affinities, timeoutList)

	return r.setInfo(data)

}

func (r *Divider) getInfo() (*DividerData, error) {
	data := new(DividerData)
	if r.JSON {

		str, err := r.redis.Get(r.done, r.infoKey).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return &DividerData{
					Worker:   make(map[string]string),
					NodeWork: make(map[string]map[string]bool),
				}, nil
			}
			return data, err
		}
		//unmarshal
		err = json.Unmarshal([]byte(str), data)
		if err != nil {
			return data, err
		}

		return data, err
	}

	err := r.redisCache.GetSkippingLocalCache(context.TODO(), r.infoKey, data)
	return data, err

}

func (r *Divider) setInfo(data *DividerData) error {
	if r.JSON {

		dataStr, err := json.Marshal(data)
		if err != nil {
			return err
		}

		_, err = r.redis.Set(r.done, r.infoKey, dataStr, time.Hour).Result()
		if err != nil {
			return err
		}
		return nil
	}

	r.redisCache.Set(&cache.Item{
		Ctx:            context.TODO(),
		Key:            r.infoKey,
		SkipLocalCache: true,
		Value:          data,
		TTL:            time.Hour,
		//TTL not set intentionally. Should be updated continuously, and if not updated, I don't thing I want it staying.
	})
	return nil

}

//NodeCountChange is a simple structure to allow for sorting by what node needs to add what data.
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

	//divider so that it is possible to use
	divider *Divider
}

func (r *DividerData) calculateNewWork(workList map[string]string, nodeList, affinities, timeoutList map[string]int64) {
	nodesToRemove, orphanedWork := r.getNodesToRemove(nodeList, affinities, timeoutList, workList)
	for _, v := range nodesToRemove {
		r.removeNode(v)
	}

	//TODO1 check Worker for any work that is no longer in the work list
	//TODO1 check NodeWork Nodes for any work that is no longer in the list.

	workToAdd, workToRemove := r.getWorkToAddAndRemove(workList)

	for _, v := range workToRemove {
		r.removeWork(v)
	}

	for _, v := range orphanedWork {
		r.removeWork(v)
	}

	//changes is a map of []{node,change} (sorted based on the change count from highest to lowest)
	//changes does not contain any changes that are less than 1.
	changes := r.calculateWorkCountChange(nodeList, affinities, len(workList))

	//applyWorkToNodes goes through the list of changes
	r.applyWorkToNodes(changes, workToAdd)
}

func (r *DividerData) applyWorkToNodes(changes []NodeCountChange, workToAdd map[string]string) {

	count := len(changes)
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

func (r *DividerData) calculateWorkCountChange(nodeList, affinities map[string]int64, workCount int) (changes []NodeCountChange) {
	changes = make([]NodeCountChange, 0)
	sum := 0
	for node, aff := range affinities {
		_, ok := nodeList[node]
		if !ok {
			continue
		}
		if aff < 0 {
			continue
		}

		sum += int(aff)
	}

	workPerAff := float64(workCount) / float64(sum)

	//calculate the total per node, rounding down.
	//nodeWorkCount is a map[nodeID]workCount so that you can look up how much work node N is assigned.
	nodeWorkCount := make(map[string]int)
	total := 0
	for k, v := range affinities {
		//if not in the live node list, skip.
		_, ok := nodeList[k]
		if !ok {
			continue
		}
		nodeWorkCount[k] = int(math.Ceil(float64(v) * workPerAff))
		total += nodeWorkCount[k]
	}

	//Had 4 work, 4 assigned to each of the current nodes.
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

func (r *DividerData) getWorkToAddAndRemove(workList map[string]string) (toAdd, toRemove map[string]string) {
	toAdd = make(map[string]string)
	toRemove = make(map[string]string)

	//loop through the list of work
	for _, work := range workList {
		//workMap[work] = work

		//if the work is not currently assigned, add to the list of work to add
		_, ok := r.Worker[work]
		if !ok {
			toAdd[work] = work
		}
	}

	//loop through all work currently assigned,
	for work, _ := range r.Worker {

		//if the work is NOT in the list of work, add it to the list to remove.
		_, ok := workList[work]
		if !ok {
			toRemove[work] = work
		}
	}
	return toAdd, toRemove
}

func (r *DividerData) getNodesToRemove(nodeList, affinities, timeoutList map[string]int64, workList map[string]string) (toRemove, orphanedWork map[string]string) {
	//nodeList is the list of nodes that are confirmed good.
	//affinities are the nodes that are in the affinities set.

	//getNodesToRemove needs the list of nodes that are no longer in the node list, but have an affinity
	orphanedWork = make(map[string]string)
	toRemove = make(map[string]string)

	//check worker for any nodes that are not in the list.
	for work, node := range r.Worker {
		_, ok := nodeList[node]
		if !ok {
			orphanedWork[work] = work
			toRemove[node] = node
		}
		_, ok = workList[work]
		if !ok {
			orphanedWork[work] = work
		}
	}

	//go through the list of affinities and remove any nodes that are not in the list.
	for node := range affinities {
		_, ok := nodeList[node]
		if !ok {
			toRemove[node] = node
		}
	}

	for node := range timeoutList {
		_, ok := nodeList[node]
		if !ok {
			toRemove[node] = node
		}
	}

	return toRemove, orphanedWork
}

func (r *DividerData) addWorkToNode(workID, nodeID string) {
	if r.NodeWork[nodeID] == nil {
		r.NodeWork[nodeID] = make(map[string]bool)
	}
	r.NodeWork[nodeID][workID] = true
	r.Worker[workID] = nodeID
}
func (r *DividerData) removeWorkFromNode(workID, nodeID string) {
	delete(r.Worker, workID)
	if r.NodeWork[nodeID] != nil {
		delete(r.NodeWork[nodeID], workID)
	}
}
func (r *DividerData) removeNode(nodeID string) {
	for work := range r.NodeWork[nodeID] {
		r.removeWorkFromNode(work, nodeID)
	}
	delete(r.NodeWork, nodeID)
	r.divider.redis.HDel(r.divider.done, r.divider.affinityKey, nodeID)
	r.divider.redis.HDel(r.divider.done, r.divider.updateTimeKey, nodeID)
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

package redis

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/streemtech/divider"

	redis "github.com/go-redis/redis/v8"
)

//TODO4 split pubsub divider and regular divider?

//Divider is a redis backed implementation of divider.Divider. The
type Divider struct {
	redis *redis.Client

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
//The keys beginning in <masterKey>:__meta are used to keep track of the different metainformation to
//:* is appended automatically!!
func NewDivider(redis *redis.Client, masterKey, name string, informer divider.Informer, timeout int) *Divider {
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

	//set the UUID.
	UUID := name
	if UUID == "" {
		UUID = uuid.New().String()
	}

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
	}

	return d
}

//Start is the trigger to make the divider begin checking for keys, and returning those keys to the channels.
//No values should return to the channels without start being called.
func (r *Divider) Start() {
	r.mux.Lock()
	//TODO
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
		//TODO make only so that it sends if the chan has been grabbed.
		r.startChan <- key
		r.currentKeys[key] = true
	}

	for key := range toRemove {
		//TODO make only so that it sends if the chan has been grabbed.
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
		case <-r.done.Done():
			return
		}
	}
}

//Internal affinity setting.
func (r *Divider) setAffinity(Affinity divider.Affinity) {
	r.redis.HSet(r.done, r.affinityKey, r.uuid, int64(Affinity))
}

func (r *Divider) sendStopProcessing(key string) {

}

func (r *Divider) getAssignedProcessingArray() []string {

	//update the time.
	r.redis.HSet(r.done, r.updateTimeKey, r.uuid, time.Now().UnixNano())

	str, err := r.redis.Get(r.done, r.infoKey).Result()
	if err != nil {
		return []string{}
	}
	//unmarshal
	dat := &DividerData{}

	err = json.Unmarshal([]byte(str), dat)
	if err != nil {
		return []string{}
	}
	return dat.NodeWork[r.uuid]
}

//updates all assignments if master.
func (r *Divider) updateAssignments() {
	r.redis.HSet(r.done, r.updateTimeKey, r.uuid, time.Now().UnixNano())

	//set the master key to this value if it does not exist.
	set, err := r.redis.SetNX(r.done, r.masterKey, r.uuid, time.Second*3).Result()
	if err != nil {
		r.informer.Errorf("update if master does not exist error: %s!", err.Error())
		return
	}
	if set {
		r.informer.Infof("The master was set to this node: %s", r.uuid)
	}

	//check the master key.
	master, err := r.redis.Get(r.done, r.masterKey).Result()
	if err != nil {
		r.informer.Errorf("get current master error: %s!", err.Error())
		return
	}

	//if this is the master, run the update (setting the timeout to 3 seconds)
	if master == r.uuid {
		_, err = r.redis.Set(r.done, r.masterKey, r.uuid, time.Second*3).Result()
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
func (r *Divider) updateData() error {
	//check if the info exists or not.
	i, err := r.redis.Exists(r.done, r.infoKey).Result()
	if err != nil {
		return err
	}
	str := ""
	dat := &DividerData{}
	newData := &DividerData{}
	//if the data exists, get it.
	if i == 1 {
		str, err = r.redis.Get(r.done, r.infoKey).Result()
		if err != nil {
			return err
		}
		//unmarshal
		err = json.Unmarshal([]byte(str), dat)
		if err != nil {
			return err
		}

	} else {
		dat = &DividerData{
			work:     make(map[string]string),
			Worker:   make(map[string]string),
			NodeWork: make(map[string][]string),
		}
	}

	//calculate the new data.
	newData, err = r.calculateData(dat)
	if err != nil {
		return err
	}
	//store into a string.
	newDataStr, err := json.Marshal(newData)
	if err != nil {
		return err
	}

	str, err = r.redis.Set(r.done, r.infoKey, newDataStr, 0).Result()
	if err != nil {
		return err
	}
	return nil
}

func (r *Divider) calculateData(old *DividerData) (new *DividerData, err error) {
	//if not zero, decrease. This allows for a timeout to allow for ballancing the inputs.

	//get the list of update times.
	timeoutStrings, err := r.redis.HGetAll(r.done, r.updateTimeKey).Result()
	if err != nil {
		return &DividerData{}, err
	}
	//convert the timeouts from strings to ints.
	timeouts := make(map[string]int64)
	for k, v := range timeoutStrings {
		timeouts[k], err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return &DividerData{}, err
		}
	}

	//get the worker affinity values.
	affinityStrings, err := r.redis.HGetAll(r.done, r.affinityKey).Result()
	if err != nil {
		return &DividerData{}, err
	}
	//convert the worker affinities to ints.
	affinities := make(map[string]int64)
	for k, v := range affinityStrings {
		affinities[k], err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return &DividerData{}, err
		}
	}

	//get most up to date list of subscriptions.
	data, err := r.redis.PubSubChannels(r.done, r.searchKey).Result()
	if err != nil {
		return &DividerData{}, err
	}

	//add the subscriptions to a map of work to be completed.
	workMap := make(map[string]string)
	for _, v := range data {
		workMap[v] = v
	}
	new = &DividerData{
		work:            workMap,
		Worker:          old.Worker,
		NodeWork:        old.NodeWork,
		lastUpdateTimes: timeouts,
		affinities:      affinities,
		sumAffinity:     0,
		nodeChange:      make(map[string]int),
	}
	//if work didnt exist, add this to the list.
	if new.Worker == nil {
		new.Worker = make(map[string]string)
	}
	if new.NodeWork == nil {
		new.NodeWork = make(map[string][]string)
	}

	r.deleteExpiredWorkers(new)
	r.deleteRemovedWork(new)
	for _, v := range new.affinities {
		//recalculate the affinity
		new.sumAffinity += int(v)
	}
	r.calculateNodeChanges(new)
	r.updateNodeWork(new)
	r.updateWorker(new)

	return new, nil
}

//loop through the list of times. If the timeout time is after the last update, add to a to delete, and then delete.
func (r *Divider) deleteExpiredWorkers(data *DividerData) {
	toDelete := make([]string, 0)
	timeout := time.Now().Add(time.Second * -1 * time.Duration(r.timeout))

	//go through the list of last update times.
	for k, v := range data.lastUpdateTimes {
		t := time.Unix(0, v)
		//any value that is after now, delete it
		if timeout.After(t) {
			r.informer.Infof("Timed out %s. Last updated %s", k, t.String())
			toDelete = append(toDelete, k)
		}
	}

	//find any nodes in affinities that no longer has an update time, and add it to the to delete list.
	for k := range data.affinities {
		_, ok := data.lastUpdateTimes[k]
		if !ok {
			toDelete = append(toDelete, k)
		}
	}

	//delete from the list of work, updatetimes, and affinities this value.
	for _, v := range toDelete {
		r.informer.Infof("Deleting node %s", v)

		delete(data.lastUpdateTimes, v)
		delete(data.NodeWork, v)
		delete(data.affinities, v)
		go r.deleteNode(data, v)
	}
}

//delete node removes the work node that is input
func (r *Divider) deleteNode(data *DividerData, node string) {
	//TODO

	//remove the node from the set of update times.
	e := r.redis.HDel(r.done, r.updateTimeKey, node).Err()
	if e != nil {
		r.informer.Errorf("Divider: Error deleting node %s: %s", node, e.Error())
	}
	//remove the node from the set of affinities.
	e = r.redis.HDel(r.done, r.affinityKey, node).Err()
	if e != nil {
		r.informer.Errorf("Divider: Error deleting node %s: %s", node, e.Error())
	}
}

//delete work that is no longer being processed.
func (r *Divider) deleteRemovedWork(data *DividerData) {
	for k := range data.Worker {
		if _, ok := data.work[k]; !ok {
			delete(data.Worker, k)
		}
	}
}

//calculateNodeChanges calculates what work is to be done by each node, and how to change the work from one node to the next.
func (r *Divider) calculateNodeChanges(data *DividerData) {
	workCount := len(data.work)
	workPerAff := float64(workCount) / float64(data.sumAffinity)

	//calculate the total per node, rounding down.
	//nodeWorkCount is a map[nodeID]workCount so that you can look up how much work node N is assigned.
	nodeWorkCount := make(map[string]int)
	total := 0
	for k, v := range data.affinities {
		nodeWorkCount[k] = int(math.Floor(float64(v) * workPerAff))
		total += nodeWorkCount[k]
	}

	//add remainder to make sure that the total matches the ammount given.
	toAdd := workCount - total
	//the order output is based on sorting the divider data deterministically.
	for _, k := range r.sortNodeChanges(data) {
		if toAdd == 0 {
			break
		}
		nodeWorkCount[k.uuid]++
		toAdd--
	}

	if toAdd > 0 {
		r.informer.Errorf("Unable to assign all work. %d work remaning. work: %d, affinityTotal: %d, workPer: %f, nodeWorkCount %v", toAdd, workCount, data.sumAffinity, workPerAff, nodeWorkCount)
	}

	//calculate the actual change in each node.
	for k, v := range nodeWorkCount {
		c := v - len(data.NodeWork[k])
		if c != 0 {
			r.informer.Infof("Node %s has a change in capacity: %d", k, c)
			data.nodeChange[k] = c
		}
	}
}

//make it so that items with the same affinity have nodes assigned to them in order.
func (r *Divider) sortNodeChanges(data *DividerData) NodeChangesSort {
	//TODO sort the nodeChange based A: the
	changes := make(NodeChangesSort, 0)
	//different affinities should have, at most one difference in count

	for k := range data.NodeWork {
		changes = append(changes, NodeChange{
			affinity: data.affinities[k],
			uuid:     k,
		})
	}

	sort.Sort(changes)
	return changes
}

//NodeChange is a simple object that holds the node's ID and it's affinity.
type NodeChange struct {
	uuid     string
	affinity int64
}

//NodeChangeSort is used to implement a sorter on NodeChange.
type NodeChangesSort []NodeChange

func (a NodeChangesSort) Len() int      { return len(a) }
func (a NodeChangesSort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a NodeChangesSort) Less(i, j int) bool {
	if a[i].affinity > a[j].affinity {
		return true
	}
	if a[i].affinity < a[j].affinity {
		return false
	}
	return a[i].uuid > a[j].uuid
}

func (r *Divider) updateNodeWork(data *DividerData) {
	//Remove work from nodes that have negative node change.

	//delete from the NodeWork based on nodeChange.
	for node, change := range data.nodeChange {

		//only need to delete from node work for nodes that need to loose work
		if change < 0 {
			//get the ammount of work that a node has
			workCount := len(data.NodeWork[node])

			if workCount+change <= 0 {
				//if there is NO work, because the total change is zero, just delete from the list
				delete(data.NodeWork, node)
			} else {
				//otherwise, just remove the most recently added work.
				data.NodeWork[node] = data.NodeWork[node][0 : workCount+change]
			}
			//delete as it wont be needed for the positive change loop.
			delete(data.nodeChange, node)
		}
	}

	//remove all work from the work list that is currently assigned to a node.
	for _, workSet := range data.NodeWork {
		if workSet != nil {
			//for each node, for each item that it has been assigned
			for _, work := range workSet {
				delete(data.work, work)
			}
		}
	}

	//convert remaning work to an array of work to be assigned.
	workArray := make([]string, 0)
	for _, v := range data.work {
		workArray = append(workArray, v)
	}

	//loop through the positive node changes, appending to the NodeWorker based on the
	workIdx := 0
	for node, change := range data.nodeChange {
		if change > 0 {
			for i := 0; i < change; i++ {
				//if the list of work to assign to that node is null, create it
				if data.NodeWork[node] == nil {
					data.NodeWork[node] = make([]string, 0)
				} //This is erroring because there is no work. There is no WORK because it has already been asigned to another node. for SOME reason tho,
				//add the work to this node, and increase the index to search for the next work.
				work := workArray[workIdx]
				data.NodeWork[node] = append(data.NodeWork[node], work)
				workIdx++
				//TODO I think I can assign the worker here
				//data.Worker[work] = node
			}
		}
	}
}

func (r *Divider) updateWorker(data *DividerData) {
	for Node, workSet := range data.NodeWork {
		for _, work := range workSet {
			data.Worker[work] = Node
		}
	}
}

//DividerData holds all information that is used to divide work.
type DividerData struct {
	//Work is the list of work to be done.
	work map[string]string
	//the time in which the key Node was last updated
	lastUpdateTimes map[string]int64
	//Affinities is the map of node to affinity.
	affinities map[string]int64
	//sumAffinity is the change in affinity that each node has.
	sumAffinity int
	//Worker is map[workId]workerNode so that you can look up the worker based on the work
	Worker map[string]string
	//NodeWork is a map[workerNode][]work to allow for looking up what work a particular node is working on
	NodeWork map[string][]string
	//nodeChanges is the map[workerNode]change that calculates how many nodes the
	nodeChange map[string]int
}

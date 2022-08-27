package redis_consistent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/streemtech/divider"
	"github.com/streemtech/divider/internal/ticker"

	redis "github.com/go-redis/redis/v8"
)

var _ divider.Divider = (*Divider)(nil)

//Divider is a redis backed, consistent hashing implementation of divider.Divider. The
// affinity value in this case is the number of virtual nodes that the divider uses.
type Divider struct {
	redis redis.UniversalClient //contains the main redis instance that data is stored into.

	masterKey string //the key that everything is built on.

	mux           *sync.Mutex        //a mutex to make sure work is done safely.
	startChan     chan string        //the channel to start work
	stopChan      chan string        //the channel to stop work
	done          context.Context    //a context that will end all requests if doneCancel is called
	doneCancel    context.CancelFunc //a function that, if called, will end all requests.
	instanceName  string             //the name of this instance of the divider
	currentKeys   []string           //the list of currently owned keys by this divider
	informer      divider.Informer   //the basic logger.
	timeout       time.Duration      // duration to time out requests.
	masterTimeout time.Duration      //time that the master can be gone for.
	//the number of virtual nodes to assign. Defaults to 5.
	nodecount int

	r           redisWorkerImpl
	workFetcher divider.WorkFetcher
}

//NewDivider returns a Divider using the Go-Redis library as a backend, and consistent hashing.
//The keys beginning in <masterKey>:__meta are used to keep track of the different
//metainformation to divide the work.
// timeout is the time that requrests may take up to. if empty, set to 1 second.
//master timeout is the maximum length of time that the master may go without showing up. if empty, set to 10 seconds
//name is the unique name of the instance. If "", name is a random uuid.
func NewDivider(r redis.UniversalClient, masterKey, name string, informer divider.Informer, timeout, masterTimeout time.Duration, nodecount int) *Divider {
	var i divider.Informer
	if informer == nil {
		i = divider.DefaultLogger{}
	} else {
		i = informer
	}
	var t time.Duration
	var mt time.Duration
	if timeout <= 0 {
		t = time.Second * 1
	} else {
		t = timeout
	}

	if masterTimeout <= 0 {
		mt = time.Second * 10
	} else {
		mt = masterTimeout
	}

	if nodecount <= 0 {
		nodecount = 5
	}

	//set the instanceID.
	instanceID := name
	if instanceID == "" {
		instanceID = uuid.New().String()
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	status, err := r.Ping(ctx).Result()
	done()
	if err != nil {
		informer.Errorf("ping error: %s", err.Error())
	}
	informer.Infof("Ping status result: %s", status)

	d := &Divider{
		redis:         r,
		masterKey:     masterKey + ":leader",
		mux:           &sync.Mutex{},
		instanceName:  instanceID,
		informer:      i,
		timeout:       t,
		masterTimeout: mt,
		nodecount:     nodecount,
		r:             redisWorkerImpl{timeout: t, r: r, masterKey: masterKey},
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

//GetAssignedProcessingArray returns a string array that represents the keys that this node is set to process.
func (r *Divider) GetAssignedProcessingArray() []string {
	return r.currentKeys
}

//GetReceiveStartProcessingChan returns a channel of strings.
//The string from this channel represents a key to a processable entity.
//This particular channel is for receiving keys that this node is to begin processing.
func (r *Divider) GetReceiveStartProcessingChan() <-chan string {
	if r.startChan == nil {
		r.startChan = make(chan string)
	}
	return r.startChan
}

//GetReceiveStopProcessingChan returns a channel of strings.
//The string from this channel represents a key to a processable entity.
//This particular channel is for receiving keys that this node is to stop processing.
func (r *Divider) GetReceiveStopProcessingChan() <-chan string {
	if r.stopChan == nil {
		r.stopChan = make(chan string)
	}
	return r.stopChan
}

//SendStopProcessing takes in a string of a key that this node is no longer processing.
//This is to be used to release the processing to another node.
//To confirm that the processing stoppage is completed, use ConfirmStopProcessing instead.
func (r *Divider) StopProcessing(key string) error {
	return r.r.RemoveWork(r.done, key)
}

//SendStopProcessing takes in a string of a key that this node is no longer processing.
//This is to be used to release the processing to another node.
//To confirm that the processing stoppage is completed, use ConfirmStopProcessing instead.
func (r *Divider) StartProcessing(key string) error {
	return r.r.AddWork(r.done, key)
}

//start the listeners/watchers for updating keys.
func (r *Divider) start() {

	if r.done != nil {
		return
	}

	//TODO0 replace all calls to r.done with active timeout.
	ctx, cancel := context.WithCancel(context.Background())
	r.done = ctx
	r.doneCancel = cancel
	r.currentKeys = make([]string, 0)

	r.watch()

}

//stop the updater for watching for keys.
func (r *Divider) stop() {

	r.doneCancel()
	r.done = nil
	r.doneCancel = nil
	r.currentKeys = make([]string, 0)
}

func (r *Divider) close() {
	r.informer.Errorf("Close call unnecessary for Redis Divider.")
	//Nothing happens here in this particular implementation as the connection to redis is passed in.
}

//goes through and adds compares the keys that the divider currently has to the keys that it should have
//It then outputs the updated keys to the respective channels
func (r *Divider) compareKeys() {
	defer func() {
		if rec := recover(); rec != nil {
			r.informer.Errorf("Forced to recover from panic in compare keys: %v", rec)
		}
	}()

	//take in the keys from the remote, and compare them to the list of current keys. if there are any changes, pipe them to the listener.
	newKeys := r.getAssignedProcessingArray()

	add, remove := getToRemoveToKeep(r.currentKeys, newKeys)

	//send out keys that need to be started
	for _, key := range add {
		if r.startChan != nil {
			r.startChan <- key
		}
	}

	//send out keys thjat need to be stopped
	for _, key := range remove {
		if r.stopChan != nil {
			r.stopChan <- key
		}
	}

	r.currentKeys = newKeys
}

func (r *Divider) watch() {

	//start the timer for saying this node is still connected.
	ticker.TickerFunc{
		C:      r.done,
		D:      time.Millisecond * 500,
		Logger: r.informer,
		F:      r.updatePing,
	}.Do()

	//start the timer for updating the work if work updating is centralized.
	ticker.TickerFunc{
		C:      r.done,
		D:      time.Millisecond * 500,
		Logger: r.informer,
		F:      r.updateAssignments,
	}.Do()

	//get the work that this node needs to do.
	ticker.TickerFunc{
		C:      r.done,
		D:      time.Millisecond * 500,
		Logger: r.informer,
		F:      r.compareKeys,
	}.Do()

}

//returns the list of things that this node is supposed to watch.
func (r *Divider) getAssignedProcessingArray() []string {
	workers := r.getWorkerList()

	var err error
	total := 0
	outWork := make([][]string, len(workers))
	for i, v := range workers {
		outWork[i], err = r.r.GetFollowingWork(r.done, v)
		if err != nil {
			r.informer.Errorf("failed to get work: %s", err.Error())
			return []string{}
		}
		total += len(outWork[i])
	}

	work := make([]string, 0, total)

	for _, v := range outWork {
		work = append(work, v...)
	}

	return work
}

func (r *Divider) getWorkerList() []string {
	z := make([]string, r.nodecount)
	for i := 0; i < r.nodecount; i++ {
		z[i] = fmt.Sprintf("%s:%d", r.instanceName, i)
	}
	return z
}

//updatePing is used to consistently tell the system that the worker is online, and listening to work.
func (r *Divider) updatePing() {

	defer func() {
		if rec := recover(); rec != nil {
			r.informer.Errorf("Forced to recover from panic in update Ping: %v", r)
		}
	}()

	//update this nodes workers to be still connected.
	err := r.r.UpdateWorkers(r.done, r.getWorkerList())
	if err != nil {
		r.informer.Errorf("failed to update workers: %s", err.Error())
		return
	}

	//set the master key to this value if it does not exist.
	set, err := r.redis.SetNX(r.done, r.masterKey, r.instanceName, r.masterTimeout).Result()
	if err != nil {
		r.informer.Errorf("update if master does not exist error: %s!", err.Error())
		return
	}

	if set {
		r.informer.Infof("The master was set to this node: %s", r.instanceName)
	}

	//check the master key.
	master, err := r.redis.Get(r.done, r.masterKey).Result()
	if err != nil {
		r.informer.Errorf("get current master error: %s!", err.Error())
		return
	}

	//if this is the master, run the update (setting the timeout to 3 seconds)
	if master == r.instanceName {
		_, err = r.redis.Set(r.done, r.masterKey, r.instanceName, r.masterTimeout).Result()
		if err != nil {
			r.informer.Errorf("update master timeout error: %s!", err.Error())
			return
		}
	}

}

//updates all assignments if master.
func (r *Divider) updateAssignments() {

	//only run if workfetcher actually exists.
	if r.workFetcher == nil {
		return
	}

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

	//if this is the master, run the update
	if master == r.instanceName {

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

	//only run if workfetcher actually exists.
	if r.workFetcher == nil {
		return nil
	}

	work := r.workFetcher(r.done)
	if work == nil {
		return nil
	}

	knownWork, err := r.r.GetAllWork(r.done)
	if err != nil {
		return err
	}

	add, remove := getToRemoveToKeep(knownWork, work)

	err = r.r.AddWorks(r.done, add)
	if err != nil {
		return err
	}

	for _, v := range remove {
		r.r.RemoveWork(r.done, v)
	}

	return nil
}

func getToRemoveToKeep(oldWork, newWork []string) (add, remove []string) {

	oldSet := make(map[string]struct{})
	newSet := make(map[string]struct{})
	for _, v := range oldWork {
		oldSet[v] = struct{}{}
	}
	for _, v := range newWork {
		newSet[v] = struct{}{}
	}

	add = make([]string, 0)
	remove = make([]string, 0)

	for s := range oldSet {
		if _, ok := newSet[s]; !ok {
			remove = append(remove, s)
		}
	}

	for s := range newSet {
		if _, ok := oldSet[s]; !ok {
			add = append(add, s)
		}
	}

	return add, remove
}

package redisconsistent

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"time"

	// "github.com/k0kubun/pp/v3"
	"github.com/pkg/errors"

	"github.com/streemtech/divider"
	"github.com/streemtech/divider/internal/set"
	"github.com/streemtech/divider/internal/ticker"
)

// Start is the trigger to make the divider begin checking for keys, and returning those keys to the channels.
// No values should return to the channels without start being called.
// Start and stop processing can be called without calling start.
func (d *dividerWorker) StartWorker(ctx context.Context) {

	if d.ctx != nil {
		return
	}

	//can not start worker if expected functions are nil.
	if d.conf.starter == nil {
		panic("missing starter func")
	}
	if d.conf.stopper == nil {
		panic("missing stopper func")
	}
	if d.conf.workFetcher == nil {
		panic("missing work fetcher func")
	}

	d.ctx, d.cancel = context.WithCancel(ctx)

	//start tickers and listeners
	d.newWorker.Ctx = d.ctx
	d.newWorker.Callback = d.newWorkerEvent
	d.removeWorker.Ctx = d.ctx
	d.removeWorker.Callback = d.removeWorkerEvent
	d.newWork.Ctx = d.ctx
	d.newWork.Callback = d.newWorkEvent
	d.removeWork.Ctx = d.ctx
	d.removeWork.Callback = d.removeWorkEvent

	d.masterUpdateRequiredWork = ticker.TickerFunc{
		C:      d.ctx,
		Logger: d.conf.logger,
		D:      d.conf.updateAssignments,
		F:      d.masterUpdateRequiredWorkFunc,
	}
	d.workerRectifyAssignedWork = ticker.TickerFunc{
		C:      d.ctx,
		Logger: d.conf.logger,
		D:      d.conf.compareKeys,
		F:      d.workerRectifyAssignedWorkFunc,
	}

	InitMetrics(d.conf.metricsName)

	d.knownWork = set.New[string]()
	ObserveGauge(DividerAssignedItemsGauge, d.conf.metricsName, d.knownWork.Len())

	d.conf.logger(ctx).Info("Starting worker", slog.String("divider.id", d.conf.instanceID))

	//add workers to work holder
	err := d.storage.UpdateTimeoutForWorkers(ctx, d.getWorkerNodeKeys())
	if err != nil {
		d.conf.logger(ctx).Panic("failed to update timeout for workers", err, slog.String("divider.id", d.conf.instanceID))
	}

	//start all listeners and tickers.
	d.newWorker.Listen()
	d.removeWorker.Listen()
	d.newWork.Listen()
	d.removeWork.Listen()

	err = d.masterUpdateRequiredWork.Do()
	if err != nil {
		d.conf.logger(ctx).Panic("failed to start ticker", err, slog.String("divider.id", d.conf.instanceID))
	}
	err = d.workerRectifyAssignedWork.Do()
	if err != nil {
		d.conf.logger(ctx).Panic("failed to start ticker", err, slog.String("divider.id", d.conf.instanceID))
	}

	d.masterUpdateRequiredWork.F()
	d.workerRectifyAssignedWork.F()

	//inform the other nodes to check their work list for work & start.stop as appropriate.
	err = d.newWorker.Publish(ctx, d.conf.instanceID)
	if err != nil {
		d.conf.logger(ctx).Error("failed to publish removing of workers from work list", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
	}
	//No need to perform manual work rectification as this node will automatically rectify on getting the new worker event.

}

// Stop begins the process of stopping processing of all assigned keys.
// Releasing these keys via stop allows them to immediately be picked up by other nodes.
// Start must be called to begin picking up work keys again.
func (d *dividerWorker) StopWorker(ctx context.Context) {

	if d.ctx == nil {
		return
	}

	//close all listeners/tickers
	d.cancel()
	d.ctx = nil
	d.cancel = nil

	err := d.removeMaster(ctx)
	if err != nil {
		d.conf.logger(ctx).Error("failed to remove workers as master", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
	}

	//remove workers from work holder.
	err = d.storage.RemoveWorkers(ctx, d.getWorkerNodeKeys())
	if err != nil {
		d.conf.logger(ctx).Error("failed to remove workers from work list", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
	}

	//send out event that workers are being removed and to re-calculate.
	err = d.removeWorker.Publish(ctx, d.conf.instanceID)
	if err != nil {
		d.conf.logger(ctx).Error("failed to publish removing of workers from work list", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
	}

	//reset known work here as well so that I cant accidentally pull the old work from the iterator.
	d.knownWork = set.New[string]()
	ObserveGauge(DividerAssignedItemsGauge, d.conf.metricsName, d.knownWork.Len())
}

// StopProcessing takes in a string of a key that this node is no longer processing.
// This is to be used to disable the work temporairly. Should be used when the work fetcher will not return the work anymore.
func (d *dividerWorker) StopProcessing(ctx context.Context, works ...string) error {
	start := time.Now()
	err := d.storage.RemoveWorkFromDividedWork(ctx, works)
	if err != nil {
		ObserveInc(RemoveWorkFromDividerStorageError, d.conf.metricsName, 1)
		return errors.Wrap(err, "failed to Remove Work From Divided Work")
	}
	for _, work := range works {
		err = d.removeWork.Publish(ctx, work)
		if err != nil {
			ObserveInc(PublishRemoveWorkFromDividerError, d.conf.metricsName, 1)
			return errors.Wrap(err, "failed to publish work removal")
		}
	}

	ObserveDuration(StopProcessingTime, d.conf.metricsName, time.Since(start))
	ObserveInc(StopProcessingKeyCount, d.conf.metricsName, len(works))

	return nil
}

// StartProcessing adds this key to the list of work to be completed. This will temporairly force a worker to start working on the work.
// Should be used when the work fetcher will start returning the work from now on.
func (d *dividerWorker) StartProcessing(ctx context.Context, works ...string) error {
	start := time.Now()

	err := d.storage.AddWorkToDividedWork(ctx, works)
	if err != nil {
		ObserveInc(AddWorkToDividerError, d.conf.metricsName, 1)
		return errors.Wrap(err, "failed to Add Work To Divided Work")
	}
	for _, work := range works {
		d.conf.logger(ctx).Debug("sending start processing for: "+work, slog.String("divider.id", d.conf.instanceID))

		err = d.newWork.Publish(ctx, work)
		if err != nil {
			ObserveInc(PublishAddWorkToDividerError, d.conf.metricsName, 1)
			return errors.Wrap(err, "failed to publish work start")
		}
	}

	ObserveDuration(StartProcessingTime, d.conf.metricsName, time.Since(start))
	ObserveInc(StartProcessingKeyCount, d.conf.metricsName, len(works))
	return nil
}

// returns all work assigned to this particular divider node.
// this method does not update the work list, and only pulls down the most recent work list.
func (d *dividerWorker) GetWork(ctx context.Context) (iter.Seq[string], error) {
	return d.knownWork.Iterator(), nil
}

//Events and tickers

// force refresh of work, and if necessary, drop any work no-longer assigned to me.
func (d *dividerWorker) newWorkerEvent(ctx context.Context, key string) {
	if d.cancel == nil {
		return
	}
	start := time.Now()

	d.conf.logger(ctx).Debug("newWorkerEvent triggered: "+key, slog.String("divider.id", d.conf.instanceID))
	err := d.rectifyWork(ctx)
	if err != nil {
		ObserveInc(RectifyWorkError, d.conf.metricsName, 1)
		d.conf.logger(ctx).Error("failed to rectify work", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
	}

	ObserveDuration(NewWorkerEventTime, d.conf.metricsName, time.Since(start))
}

// grab new work and check if a new master needs to be created.
func (d *dividerWorker) removeWorkerEvent(ctx context.Context, key string) {
	if d.cancel == nil {
		return
	}
	start := time.Now()

	d.conf.logger(ctx).Debug("removeWorkerEvent triggered: "+key, slog.String("divider.id", d.conf.instanceID))

	//trigger a force update of the master
	d.masterUpdateRequiredWorkFunc()

	//rectify all of your work.
	err := d.rectifyWork(ctx)
	if err != nil {
		ObserveInc(RectifyWorkError, d.conf.metricsName, 1)
		d.conf.logger(ctx).Error("failed to rectify work", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
	}

	ObserveDuration(RemoveWorkerEventTime, d.conf.metricsName, time.Since(start))
}

// check if work belongs to me and if needed, start it.
func (d *dividerWorker) newWorkEvent(ctx context.Context, key string) {
	if d.cancel == nil {
		return
	}
	start := time.Now()

	d.conf.logger(ctx).Debug("newWorkEvent triggered: "+key, slog.String("divider.id", d.conf.instanceID))
	for _, v := range d.getWorkerNodeKeys() {
		inRange, err := d.storage.CheckWorkInKeyRange(ctx, v, key)
		if err != nil {
			ObserveInc(CheckWorkInKeyRangeError, d.conf.metricsName, 1)
			d.conf.logger(ctx).Error("failed to check if work is in the range of this worker", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
			return
		}
		if inRange {
			err = d.conf.starter(ctx, key)
			if err != nil {
				ObserveInc(StartWorkExternalError, d.conf.metricsName, 1)
				d.conf.logger(ctx).Error("failed to execute starter", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
				return
			}

			d.knownWork.Add(key)
			ObserveGauge(DividerAssignedItemsGauge, d.conf.metricsName, d.knownWork.Len())

			return
		}
	}
	d.conf.logger(ctx).Debug("newWorkEvent not in range: "+key, slog.String("divider.id", d.conf.instanceID))

	ObserveDuration(NewWorkEventTime, d.conf.metricsName, time.Since(start))
}

// check if I am running the work, and if needed, remove it from my list of things to work on.
func (d *dividerWorker) removeWorkEvent(ctx context.Context, key string) {
	if d.cancel == nil {
		return
	}
	start := time.Now()

	d.conf.logger(ctx).Debug("removeWorkEvent triggered: "+key, slog.String("divider.id", d.conf.instanceID))
	if d.knownWork.Contains(key) {
		err := d.conf.stopper(ctx, key)
		if err != nil {
			d.conf.logger(ctx).Error("failed to execute stopper", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
			return
		}

		d.knownWork.Remove(key)
		ObserveGauge(DividerAssignedItemsGauge, d.conf.metricsName, d.knownWork.Len())

	}

	ObserveDuration(RemoveWorkEventTime, d.conf.metricsName, time.Since(start))
}

// run (master only) the work to update the list of all work required. Will not output that the work is
func (d *dividerWorker) masterUpdateRequiredWorkFunc() {
	start := time.Now()
	defer func() {
		ObserveDuration(MasterUpdateWorkTime, d.conf.metricsName, time.Since(start))
	}()

	d.conf.logger(d.ctx).Debug("masterUpdateRequiredWorkFunc triggered", slog.String("divider.id", d.conf.instanceID))

	//if not master, dont do the work fetcher.
	if !d.materUpdate(d.ctx) {
		return
	}

	//Get all the newWork that needs to be assigned.
	newWork, err := d.conf.workFetcher(d.ctx)
	if err != nil {
		d.conf.logger(d.ctx).Error("failed to execute work fetcher", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
		ObserveInc(WorkFetcherError, d.conf.metricsName, 1)
		return
	}

	//Add the work to the list of work in the system.
	err = d.storage.AddWorkToDividedWork(d.ctx, newWork)
	if err != nil {
		d.conf.logger(d.ctx).Error("failed to add work to divided work", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
		ObserveInc(AddWorkToDivideWorkError, d.conf.metricsName, 1)
		return
	}

	//get existing work
	existingWork, err := d.storage.GetAllWork(d.ctx)
	if err != nil {
		d.conf.logger(d.ctx).Error("failed to get list of all work", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
		ObserveInc(GetAllWorkError, d.conf.metricsName, 1)
		return
	}

	//determine what work to remove
	_, remove := getToRemoveToKeep(existingWork, newWork)

	// pp.Println(d.conf.metricsName, existingWork, newWork, remove)
	//remove all that work
	err = d.storage.RemoveWorkFromDividedWork(d.ctx, remove)
	if err != nil {
		d.conf.logger(d.ctx).Error("failed to remove the old work", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
		ObserveInc(RemoveWorkFromDividerStorageError, d.conf.metricsName, 1)
		return
	}

	for _, v := range remove {
		err = d.removeWork.Publish(d.ctx, v)
		if err != nil {
			d.conf.logger(d.ctx).Error("failed to publish old work removal", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
			ObserveInc(RemoveWorkPublishError, d.conf.metricsName, 1)
		}
	}
	//note: Not triggering the add work event as that event should be triggered manually by the add work call, not by this.
	//The work will be picked up by the rectify call later.
	//Am manually calling the remove work as that should happen as soon as rectified if needed. (This can result in a double call, but thats ok in this case.)
}

// get work assigned to this node, compare with known work, and start/stop all work as needed.
func (d *dividerWorker) workerRectifyAssignedWorkFunc() {
	start := time.Now()
	defer func() {
		ObserveDuration(WorkerRectifyTime, d.conf.metricsName, time.Since(start))
	}()

	d.conf.logger(d.ctx).Debug("workerRectifyAssignedWorkFunc triggered", slog.String("divider.id", d.conf.instanceID))
	err := d.rectifyWork(d.ctx)
	if err != nil {
		d.conf.logger(d.ctx).Error("failed to rectify work", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
	}

	d.workerPing(d.ctx)
}

func (d *dividerWorker) removeMaster(ctx context.Context) (err error) {

	masterKey := fmt.Sprintf("%s:%s", d.conf.rootKey, "master")

	master, err := d.client.Get(ctx, masterKey).Result()
	if err != nil {
		d.conf.logger(ctx).Panic("Error getting current master", err, slog.String("divider.id", d.conf.instanceID))
		return errors.Wrap(err, "failed to get current master")
	}

	//if this is the master, run the update to keep the master inline.
	if master == d.conf.instanceID {
		_, err = d.client.Del(ctx, masterKey).Result()

		if err != nil {
			d.conf.logger(ctx).Panic("deleting master key", err, slog.String("divider.id", d.conf.instanceID))
			return errors.Wrap(err, "failed to delete master key")
		}
	}
	return nil
}

// set master as still attached
func (d *dividerWorker) materUpdate(ctx context.Context) (isMaster bool) {

	// d.conf.logger(d.ctx).Debug("masterPingFunc triggered")
	masterKey := fmt.Sprintf("%s:%s", d.conf.rootKey, "master")
	//set the master key to this value if it does not exist.
	set, err := d.client.SetNX(ctx, masterKey, d.conf.instanceID, d.conf.masterTimeout).Result()
	if err != nil {
		d.conf.logger(ctx).Panic("Error updating node master", err, slog.String("divider.id", d.conf.instanceID))
		return
	}

	if set {
		d.conf.logger(ctx).Info("Master set to this node", slog.String("divider.id", d.conf.instanceID))
	}

	//check the master key.
	master, err := d.client.Get(ctx, masterKey).Result()
	if err != nil {
		d.conf.logger(ctx).Panic("Error getting current master", err, slog.String("divider.id", d.conf.instanceID))
		return
	}

	//if this is the master, run the update to keep the master inline.
	if master == d.conf.instanceID {
		_, err = d.client.Set(ctx, masterKey, d.conf.instanceID, d.conf.masterTimeout).Result()
		if err != nil {
			d.conf.logger(ctx).Panic("Error updating master timeout", err, slog.String("divider.id", d.conf.instanceID))
			return
		}
		return true
	}
	return false
}

// update nodes in storage as still attached.
func (d *dividerWorker) workerPing(ctx context.Context) {
	start := time.Now()
	defer func() {
		ObserveDuration(WorkerPingTime, d.conf.metricsName, time.Since(start))
	}()
	// d.conf.logger(d.ctx).Debug("workerPingFunc triggered")
	//add workers to work holder
	err := d.storage.UpdateTimeoutForWorkers(ctx, d.getWorkerNodeKeys())
	if err != nil {
		d.conf.logger(d.ctx).Panic("failed to update timeout for workers", err, slog.String("divider.id", d.conf.instanceID))
	}
}

// helpers
func (d *dividerWorker) getWorkerNodeKeys() []string {
	workers := []string{}
	for i := range d.conf.nodeCount {
		workers = append(workers, fmt.Sprintf("%s:%d", d.conf.instanceID, i))
	}
	return workers
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

// take existing work, and compare to expected work.
func (d *dividerWorker) rectifyWork(ctx context.Context) (err error) {
	oldWork := d.knownWork.Clone()
	newWork, err := d.getKnownWork(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get known work")
	}

	toRemove := oldWork.Difference(newWork)
	toAdd := newWork.Difference(oldWork)
	// pp.Println(d.conf.instanceID, "work, old:", oldWork.Array(), "new: ", newWork.Array())

	// pp.Println(d.conf.instanceID, "toRemove", toRemove.Array())
	for key := range toRemove.Iterator() {
		ctx2, can := context.WithTimeout(ctx, time.Second*5)
		defer can()
		err = d.conf.stopper(ctx2, key)
		if err != nil {
			ObserveInc(StopWorkExternalError, d.conf.metricsName, 1)

			d.conf.logger(ctx2).Error("failed to execute stopper, not removing from known work", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
			continue
		}
		d.knownWork.Remove(key)
	}

	// pp.Println(d.conf.instanceID, "toAdd", toAdd.Array())
	for key := range toAdd.Iterator() {
		ctx2, can := context.WithTimeout(ctx, time.Second*5)
		defer can()
		err = d.conf.starter(ctx2, key)
		if err != nil {
			ObserveInc(StartWorkExternalError, d.conf.metricsName, 1)
			d.conf.logger(ctx2).Error("failed to execute starter, not adding to known work", slog.String("err.error", err.Error()), slog.String("divider.id", d.conf.instanceID))
			continue
		}
		d.knownWork.Add(key)
	}

	ObserveGauge(DividerAssignedItemsGauge, d.conf.metricsName, d.knownWork.Len())

	return nil
}

func (d *dividerWorker) getKnownWork(ctx context.Context) (set.Set[string], error) {

	keys := d.getWorkerNodeKeys()
	var wrapperErr error
	allWorkArray := divider.Map(keys, func(key string) []string {
		data, err := d.storage.GetWorkFromKeyToNextWorkerKey(ctx, key)
		if err != nil {
			wrapperErr = err
			return nil
		}
		return data
	})

	if wrapperErr != nil {
		return set.Set[string]{}, errors.Wrap(wrapperErr, "failed to get one or more sets of work for worker")
	}

	s := set.New[string]()
	//Add all the work to the knownWork set
	for _, nodeWorkArray := range allWorkArray {
		s.Add(nodeWorkArray...)
	}

	return s, nil

}

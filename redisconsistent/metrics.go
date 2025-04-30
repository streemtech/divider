package redisconsistent

import (
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var HistogramBuckets = []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1}

var NewWorkerEventTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "new_worker_event",
	Help:      "Returns the length of time that processing a worker was added took",
	Buckets:   HistogramBuckets,
}, []string{"divider"})
var RemoveWorkerEventTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "remove_worker_event",
	Help:      "Returns the length of time that processing a worker was removed took",
	Buckets:   HistogramBuckets,
}, []string{"divider"})
var NewWorkEventTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "new_work_event",
	Help:      "Returns the length of time processing that there is new work took",
	Buckets:   HistogramBuckets,
}, []string{"divider"})
var RemoveWorkEventTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "remove_work_event",
	Help:      "Returns the length of time that processing a particular work was removed took",
	Buckets:   HistogramBuckets,
}, []string{"divider"})
var MasterUpdateWorkTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "master_update_work_time",
	Help:      "Returns the length of time that getting and updating the full list of work took",
	Buckets:   HistogramBuckets,
}, []string{"divider"})
var WorkerRectifyTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "worker_rectify_assigned_work_time",
	Help:      "Returns the length of time that rectifying the work assigned to this node took",
	Buckets:   HistogramBuckets,
}, []string{"divider"})
var WorkerPingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "worker_ping_time",
	Help:      "Returns the length of time that updating the worker took.",
	Buckets:   HistogramBuckets,
}, []string{"divider"})
var StartProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "start_processing_time",
	Help:      "Returns the length of time that stop_processing request took",
	Buckets:   HistogramBuckets,
}, []string{"divider"})
var StopProcessingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "stop_processing_time",
	Help:      "Returns the length of time that stop_processing request took",
	Buckets:   HistogramBuckets,
}, []string{"divider"})

var StartProcessingKeyCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "start_processing_key_count",
	Help:      "Returns the number of keys that have requested the start of processing directly",
}, []string{"divider"})
var StopProcessingKeyCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "stop_processing_key_count",
	Help:      "Returns the number of keys that have requested the stop of processing directly",
}, []string{"divider"})

var DividerAssignedItemsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "assigned_items",
	Help:      "Returns the number of items assigned to this divider",
}, []string{"divider"})

// error tracking counters
var AddWorkToDividerError = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "error_add_work_to_divider",
	Help:      "An error was encountered attempting to add work to the stored work that needs to be done.",
}, []string{"divider"})
var CheckWorkInKeyRangeError = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "error_check_work_in_key_range",
	Help:      "an error was encountered attempting to check against storage if a given work key is in the worker's work coverage ranges.",
}, []string{"divider"})
var PublishAddWorkToDividerError = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "error_publish_add_work_to_divider",
	Help:      "An error was encountered attempting to publish the work to the new work stream",
}, []string{"divider"})
var PublishRemoveWorkFromDividerError = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "error_remove_work_from_divider",
	Help:      "An error was encountered attempting to publish the work to the remove work stream",
}, []string{"divider"})
var RectifyWorkError = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "error_rectify_work",
	Help:      "An error was encountered attempting to rectify the worker's work that needs to be processed/completed.",
}, []string{"divider"})
var RemoveWorkFromDividerStorageError = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "error_remove_work_from_divider_storage",
	Help:      "An error was encountered attempting to remove work from the stored work that needs to be done in redis",
}, []string{"divider"})
var StartWorkExternalError = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "error_start_work_external",
	Help:      "an error in the starter callback was encountered attempting to start wokring on one of the desired work",
}, []string{"divider"})
var StopWorkExternalError = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "error_stop_work_external",
	Help:      "an error in the stopper callback was encountered attempting to stop wokring on one of the desired work",
}, []string{"divider"})

var WorkFetcherError = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "error_external_work_fetcher_callback",
	Help:      "an error in the master work calculation flow where attempting to call the external callback to calculate work",
}, []string{"divider"})
var AddWorkToDivideWorkError = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "error_internal_divide_work",
	Help:      "an error in the master work calculation flow when attempting to divide the work between the appropriate nodes",
}, []string{"divider"})
var GetAllWorkError = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "error_get_all_existing_work",
	Help:      "an error in the master work calculation flow when attempting to get all work that needs to be divided",
}, []string{"divider"})
var RemoveWorkPublishError = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "error_remove_work_publish",
	Help:      "an error in the master work calculation flow when attempting to publish that work needs to be removed",
}, []string{"divider"})

func ObserveDuration(metric *prometheus.HistogramVec, name string, taken time.Duration) {
	met, err := metric.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric with labels to observe").Error())
	}

	met.Observe(float64(taken) / float64(time.Second))
}

func ObserveInc(metric *prometheus.CounterVec, name string, count int) {
	met, err := metric.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric with labels to observe").Error())
	}

	met.Add(float64(count))
}

func ObserveGauge(metric *prometheus.GaugeVec, name string, value int) {
	met, err := metric.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric with labels to observe").Error())
	}

	met.Set(float64(value))
}

func InitMetrics(name string) {

	mets := []*prometheus.MetricVec{
		NewWorkerEventTime.MetricVec,
		RemoveWorkerEventTime.MetricVec,
		NewWorkEventTime.MetricVec,
		RemoveWorkEventTime.MetricVec,
		MasterUpdateWorkTime.MetricVec,
		WorkerRectifyTime.MetricVec,
		WorkerPingTime.MetricVec,
		StartProcessingTime.MetricVec,
		StopProcessingTime.MetricVec,
		StartProcessingKeyCount.MetricVec,
		StopProcessingKeyCount.MetricVec,
		DividerAssignedItemsGauge.MetricVec,
		AddWorkToDividerError.MetricVec,
		CheckWorkInKeyRangeError.MetricVec,
		PublishAddWorkToDividerError.MetricVec,
		PublishRemoveWorkFromDividerError.MetricVec,
		RectifyWorkError.MetricVec,
		RemoveWorkFromDividerStorageError.MetricVec,
		StartWorkExternalError.MetricVec,
		StopWorkExternalError.MetricVec,
		WorkFetcherError.MetricVec,
		AddWorkToDivideWorkError.MetricVec,
		GetAllWorkError.MetricVec,
		RemoveWorkPublishError.MetricVec,
	}

	for _, v := range mets {
		_, err := v.GetMetricWithLabelValues(name)
		if err != nil {
			panic(errors.Wrap(err, "failed to get metric"))
		}
	}

}

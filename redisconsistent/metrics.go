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
var MasterPingTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "streemtech",
	Subsystem: "redis_work_divider",
	Name:      "master_ping_time",
	Help:      "Returns the length of time that updating the master took.",
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

	var err error

	_, err = NewWorkerEventTime.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

	_, err = RemoveWorkerEventTime.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

	_, err = NewWorkEventTime.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

	_, err = RemoveWorkEventTime.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

	_, err = MasterUpdateWorkTime.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

	_, err = WorkerRectifyTime.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

	_, err = MasterPingTime.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

	_, err = WorkerPingTime.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

	_, err = StartProcessingTime.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

	_, err = StopProcessingTime.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

	_, err = StartProcessingKeyCount.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

	_, err = StopProcessingKeyCount.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

	_, err = DividerAssignedItemsGauge.GetMetricWithLabelValues(name)
	if err != nil {
		panic(errors.Wrap(err, "failed to get metric"))
	}

}

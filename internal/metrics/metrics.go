package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type MQMetrics struct {
	EnqueueCounter   *prometheus.CounterVec
	DequeueCounter   *prometheus.CounterVec
	AckCounter       *prometheus.CounterVec
	NackCounter      *prometheus.CounterVec
	TotalMessages    *prometheus.GaugeVec
	InFlightMessages *prometheus.GaugeVec
	DLQMessages      *prometheus.GaugeVec
}

func NewMQMetrics() *MQMetrics {
	mqm := &MQMetrics{
		EnqueueCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mq_enqueue_total",
				Help: "Total number of messages enqueued",
			},
			[]string{"queue"},
		),
		DequeueCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mq_dequeue_total",
				Help: "Total number of messages dequeued",
			},
			[]string{"queue"},
		),
		AckCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mq_ack_total",
				Help: "Total number of messages acknowledged",
			},
			[]string{"queue"},
		),
		NackCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mq_nack_total",
				Help: "Total number of messages not acknowledged",
			},
			[]string{"queue"},
		),
		TotalMessages: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "mq_total_messages",
				Help: "Current total number of messages in the queue",
			},
			[]string{"queue"},
		),
		InFlightMessages: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "mq_in_flight_messages",
				Help: "Current number of messages being processed",
			},
			[]string{"queue"},
		),
		DLQMessages: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "mq_dlq_messages",
				Help: "Current number of messages in the dead-letter queue",
			},
			[]string{"queue"},
		),
	}
	mqm.MustRegisterMetrics()
	return mqm
}

func (mqm *MQMetrics) MustRegisterMetrics() {
	prometheus.MustRegister(mqm.EnqueueCounter)
	prometheus.MustRegister(mqm.DequeueCounter)
	prometheus.MustRegister(mqm.AckCounter)
	prometheus.MustRegister(mqm.NackCounter)
	prometheus.MustRegister(mqm.TotalMessages)
	prometheus.MustRegister(mqm.InFlightMessages)
	prometheus.MustRegister(mqm.DLQMessages)

	mqm.EnqueueCounter.WithLabelValues("default").Add(0)
	mqm.DequeueCounter.WithLabelValues("default").Add(0)
	mqm.AckCounter.WithLabelValues("default").Add(0)
	mqm.NackCounter.WithLabelValues("default").Add(0)
	mqm.TotalMessages.WithLabelValues("default").Set(0)
	mqm.InFlightMessages.WithLabelValues("default").Set(0)
	mqm.DLQMessages.WithLabelValues("default").Set(0)
}

// Package pubsubmon implements a PeerMonitor component for IPFS Cluster that
// uses PubSub to send and receive metrics.
package pubsubmon

import (
	"bytes"
	"context"
	"sync"

	"github.com/ipfs/ipfs-cluster/api"
	"github.com/ipfs/ipfs-cluster/monitor/util"

	rpc "github.com/hsanjuan/go-libp2p-gorpc"
	logging "github.com/ipfs/go-log"
	floodsub "github.com/libp2p/go-floodsub"
	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	msgpack "github.com/multiformats/go-multicodec/msgpack"
)

var logger = logging.Logger("monitor")

// PubsubTopic specifies the topic used to publish Cluster metrics.
var PubsubTopic = "pubsubmon"

var msgpackHandle = msgpack.DefaultMsgpackHandle()

// Monitor is a component in charge of monitoring peers, logging
// metrics and detecting failures
type Monitor struct {
	ctx       context.Context
	cancel    func()
	rpcClient *rpc.Client
	rpcReady  chan struct{}

	host         host.Host
	pubsub       *floodsub.PubSub
	subscription *floodsub.Subscription

	metrics *util.MetricStore
	checker *util.MetricsChecker

	config *Config

	shutdownLock sync.Mutex
	shutdown     bool
	wg           sync.WaitGroup
}

// New creates a new monitor. It receives the window capacity
// (how many metrics to keep for each peer and type of metric) and the
// monitoringInterval (interval between the checks that produce alerts)
// as parameters
func New(h host.Host, cfg *Config) (*Monitor, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	metrics := util.NewMetricStore()
	checker := util.NewMetricsChecker(metrics)

	pubsub, err := floodsub.NewFloodSub(ctx, h)
	if err != nil {
		cancel()
		return nil, err
	}

	subscription, err := pubsub.Subscribe(PubsubTopic)
	if err != nil {
		cancel()
		return nil, err
	}

	mon := &Monitor{
		ctx:      ctx,
		cancel:   cancel,
		rpcReady: make(chan struct{}, 1),

		host:         h,
		pubsub:       pubsub,
		subscription: subscription,

		metrics: metrics,
		checker: checker,
		config:  cfg,
	}

	go mon.run()
	return mon, nil
}

func (mon *Monitor) run() {
	select {
	case <-mon.rpcReady:
		go mon.logFromPubsub()
		go mon.checker.Watch(mon.ctx, mon.getPeers, mon.config.CheckInterval)
	case <-mon.ctx.Done():
	}
}

// logFromPubsub logs metrics received in the subscribed topic.
func (mon *Monitor) logFromPubsub() {
	for {
		select {
		case <-mon.ctx.Done():
			return
		default:
			msg, err := mon.subscription.Next(mon.ctx)
			if err != nil { // context cancelled enters here
				continue
			}

			data := msg.GetData()
			buf := bytes.NewBuffer(data)
			dec := msgpack.Multicodec(msgpackHandle).Decoder(buf)
			metric := api.Metric{}
			err = dec.Decode(&metric)
			if err != nil {
				logger.Error(err)
				continue
			}
			logger.Debugf(
				"received pubsub metric '%s' from '%s'",
				metric.Name,
				metric.Peer,
			)

			err = mon.LogMetric(metric)
			if err != nil {
				logger.Error(err)
				continue
			}
		}
	}
}

// SetClient saves the given rpc.Client  for later use
func (mon *Monitor) SetClient(c *rpc.Client) {
	mon.rpcClient = c
	mon.rpcReady <- struct{}{}
}

// Shutdown stops the peer monitor. It particular, it will
// not deliver any alerts.
func (mon *Monitor) Shutdown() error {
	mon.shutdownLock.Lock()
	defer mon.shutdownLock.Unlock()

	if mon.shutdown {
		logger.Warning("Monitor already shut down")
		return nil
	}

	logger.Info("stopping Monitor")
	close(mon.rpcReady)

	// not necessary as this just removes subscription
	// mon.subscription.Cancel()
	mon.cancel()

	mon.wg.Wait()
	mon.shutdown = true
	return nil
}

// LogMetric stores a metric so it can later be retrieved.
func (mon *Monitor) LogMetric(m api.Metric) error {
	mon.metrics.Add(m)
	logger.Debugf("pubsub mon logged '%s' metric from '%s'. Expires on %d", m.Name, m.Peer, m.Expire)
	return nil
}

// PublishMetric broadcasts a metric to all current cluster peers.
func (mon *Monitor) PublishMetric(m api.Metric) error {
	if m.Discard() {
		logger.Warningf("discarding invalid metric: %+v", m)
		return nil
	}

	var b bytes.Buffer

	enc := msgpack.Multicodec(msgpackHandle).Encoder(&b)
	err := enc.Encode(m)
	if err != nil {
		logger.Error(err)
		return err
	}

	logger.Debugf(
		"publishing metric %s to pubsub. Expires: %d",
		m.Name,
		m.Expire,
	)

	err = mon.pubsub.Publish(PubsubTopic, b.Bytes())
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// getPeers gets the current list of peers from the consensus component
func (mon *Monitor) getPeers() ([]peer.ID, error) {
	// Ger current list of peers
	var peers []peer.ID
	err := mon.rpcClient.Call(
		"",
		"Cluster",
		"ConsensusPeers",
		struct{}{},
		&peers,
	)
	if err != nil {
		logger.Error(err)
	}
	return peers, err
}

// LatestMetrics returns last known VALID metrics of a given type. A metric
// is only valid if it has not expired and belongs to a current cluster peers.
func (mon *Monitor) LatestMetrics(name string) []api.Metric {
	latest := mon.metrics.Latest(name)

	// Make sure we only return metrics in the current peerset
	peers, err := mon.getPeers()
	if err != nil {
		return []api.Metric{}
	}

	return util.PeersetFilter(latest, peers)
}

// Alerts returns a channel on which alerts are sent when the
// monitor detects a failure.
func (mon *Monitor) Alerts() <-chan api.Alert {
	return mon.checker.Alerts()
}

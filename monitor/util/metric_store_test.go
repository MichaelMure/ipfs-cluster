package util

import (
	"testing"
	"time"

	"github.com/ipfs/ipfs-cluster/api"
	"github.com/ipfs/ipfs-cluster/test"
)

func TestMetricsLatest(t *testing.T) {
	metrics := NewMetricStore()

	metr := api.Metric{
		Name:  "test",
		Peer:  test.TestPeerID1,
		Value: "1",
		Valid: true,
	}
	metr.SetTTL(200 * time.Millisecond)
	metrics.Add(metr)

	latest := metrics.Latest("test")
	if len(latest) != 1 {
		t.Error("expected 1 metric")
	}

	time.Sleep(220 * time.Millisecond)

	latest = metrics.Latest("test")
	if len(latest) != 0 {
		t.Error("expected no metrics")
	}
}

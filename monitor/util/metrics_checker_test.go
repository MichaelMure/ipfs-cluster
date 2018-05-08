package util

import (
	"context"
	"testing"
	"time"

	peer "github.com/libp2p/go-libp2p-peer"

	"github.com/ipfs/ipfs-cluster/api"
	"github.com/ipfs/ipfs-cluster/test"
)

func TestMetricsChecker(t *testing.T) {
	metrics := NewMetricStore()

	checker := NewMetricsChecker(metrics)

	metr := api.Metric{
		Name:  "test",
		Peer:  test.TestPeerID1,
		Value: "1",
		Valid: true,
	}
	metr.SetTTL(2 * time.Second)

	metrics.Add(metr)

	checker.CheckMetrics([]peer.ID{test.TestPeerID1})
	select {
	case <-checker.Alerts():
		t.Error("there should not be an alert yet")
	default:
	}

	time.Sleep(3 * time.Second)
	err := checker.CheckMetrics([]peer.ID{test.TestPeerID1})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-checker.Alerts():
	default:
		t.Error("an alert should have been triggered")
	}

	checker.CheckMetrics([]peer.ID{test.TestPeerID2})
	select {
	case <-checker.Alerts():
		t.Error("there should not be alerts for different peer")
	default:
	}
}

func TestMetricsCheckerWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	metrics := NewMetricStore()

	checker := NewMetricsChecker(metrics)

	metr := api.Metric{
		Name:  "test",
		Peer:  test.TestPeerID1,
		Value: "1",
		Valid: true,
	}
	metr.SetTTL(200 * time.Millisecond)
	metrics.Add(metr)

	peersF := func() ([]peer.ID, error) {
		return []peer.ID{test.TestPeerID1}, nil
	}

	checker.Watch(ctx, peersF, 100*time.Millisecond)

	select {
	case a := <-checker.Alerts():
		t.Log("received alert:", a)
	case <-ctx.Done():
		t.Fatal("should have received an alert")
	}
}

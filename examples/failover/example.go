// Failover example for AEGIS.
package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/StarkTech-coder/aegis/pkg/aegis"
	"sync"
	"time"
)

func main() {
	store := aegis.NewInMemoryLeaderStore()

	probeAlive := map[string]bool{"node-a": true, "node-b": true}
	var probeMu sync.RWMutex
	probe := aegis.HeartbeatProbeFunc(func(_ context.Context, target aegis.NodeEndpoint, _ aegis.HeartbeatMessage) (aegis.HeartbeatResponse, error) {
		probeMu.RLock()
		alive := probeAlive[target.NodeID]
		probeMu.RUnlock()
		if !alive {
			return aegis.HeartbeatResponse{}, errors.New("node unreachable")
		}
		return aegis.HeartbeatResponse{}, nil
	})

	hbCfg := aegis.DefaultHeartbeatConfig("controller")
	hbCfg.Interval = 200 * time.Millisecond
	hbCfg.Timeout = 100 * time.Millisecond
	hbCfg.SuspectThreshold = 1
	hbCfg.UnhealthyThreshold = 1
	hb, err := aegis.NewHeartbeatMonitor(hbCfg, probe, nil)
	if err != nil {
		panic(err)
	}
	if err := hb.AddNode(aegis.NodeEndpoint{NodeID: "node-a", Address: "10.0.0.11:9000"}); err != nil {
		panic(err)
	}
	if err := hb.AddNode(aegis.NodeEndpoint{NodeID: "node-b", Address: "10.0.0.12:9000"}); err != nil {
		panic(err)
	}
	if err := hb.Start(context.Background()); err != nil {
		panic(err)
	}
	defer func() { _ = hb.Stop() }()

	nodeA, err := aegis.NewLeaderElector(aegis.DefaultLeaderElectionConfig("node-a"), store, nil, hb, nil)
	if err != nil {
		panic(err)
	}
	nodeB, err := aegis.NewLeaderElector(aegis.DefaultLeaderElectionConfig("node-b"), store, nil, hb, nil)
	if err != nil {
		panic(err)
	}

	if err := nodeA.Start(context.Background()); err != nil {
		panic(err)
	}
	defer func() { _ = nodeA.Stop() }()
	if err := nodeB.Start(context.Background()); err != nil {
		panic(err)
	}
	defer func() { _ = nodeB.Stop() }()

	routeMu := sync.RWMutex{}
	activeRoute := ""

	fmCfg := aegis.DefaultFailoverConfig()
	fmCfg.CheckInterval = 100 * time.Millisecond
	fm, err := aegis.NewFailoverManager(fmCfg, nodeB, store, nil, hb, nil)
	if err != nil {
		panic(err)
	}
	_, err = fm.RegisterHandler(func(_ context.Context, event aegis.FailoverEvent) error {
		if event.LeaderID == "" {
			return nil
		}
		routeMu.Lock()
		activeRoute = event.LeaderID
		routeMu.Unlock()
		fmt.Printf("routing update -> leader=%s term=%d fencing=%d reason=%s\n", event.LeaderID, event.Term, event.FencingToken, event.Reason)
		return nil
	})
	if err != nil {
		panic(err)
	}

	if err := fm.Start(context.Background()); err != nil {
		panic(err)
	}
	defer func() { _ = fm.Stop() }()

	time.Sleep(700 * time.Millisecond)
	fmt.Println("simulating node-a failure")
	probeMu.Lock()
	probeAlive["node-a"] = false
	probeMu.Unlock()

	time.Sleep(1200 * time.Millisecond)
	routeMu.RLock()
	fmt.Printf("final routed leader: %s\n", activeRoute)
	routeMu.RUnlock()
}

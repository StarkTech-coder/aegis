package main

import (
	"aegis/pkg/aegis"
	"context"
	"fmt"
	"time"
)

type staticDiscoverySource struct{}

func (staticDiscoverySource) Discover(_ context.Context) ([]aegis.DiscoveryAnnouncement, error) {
	now := time.Now()
	return []aegis.DiscoveryAnnouncement{
		{NodeID: "node-a", Address: "10.0.0.11:9000", SeenAt: now, Metadata: map[string]string{"zone": "east"}},
		{NodeID: "node-b", Address: "10.0.0.12:9000", SeenAt: now, Metadata: map[string]string{"zone": "west"}},
	}, nil
}

func main() {
	hbCfg := aegis.DefaultHeartbeatConfig("controller")
	hbCfg.Interval = 500 * time.Millisecond
	hbCfg.Timeout = 200 * time.Millisecond

	probe := aegis.HeartbeatProbeFunc(func(_ context.Context, target aegis.NodeEndpoint, _ aegis.HeartbeatMessage) (aegis.HeartbeatResponse, error) {
		return aegis.HeartbeatResponse{SourceNodeID: target.NodeID, TargetNodeID: "controller", ReceivedAt: time.Now()}, nil
	})

	hb, err := aegis.NewHeartbeatMonitor(hbCfg, probe, nil)
	if err != nil {
		panic(err)
	}

	dCfg := aegis.DefaultDiscoveryConfig()
	dCfg.Interval = 700 * time.Millisecond
	dCfg.Expiration = 3 * time.Second
	dCfg.StartHeartbeat = true

	dm, err := aegis.NewDiscoveryManager(dCfg, staticDiscoverySource{}, hb, nil)
	if err != nil {
		panic(err)
	}

	if err := dm.Start(context.Background()); err != nil {
		panic(err)
	}
	defer func() {
		_ = dm.Stop()
	}()

	time.Sleep(1200 * time.Millisecond)
	for _, n := range dm.ListNodes() {
		fmt.Printf("discovered node=%s addr=%s health=%s seen=%s\n", n.NodeID, n.Address, n.Health, n.LastSeenAt.Format(time.RFC3339Nano))
	}
}

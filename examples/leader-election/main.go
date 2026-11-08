package main

import (
	"aegis/pkg/aegis"
	"context"
	"fmt"
	"time"
)

func main() {
	store := aegis.NewInMemoryLeaderStore()

	nodeA, err := aegis.NewLeaderElector(
		aegis.DefaultLeaderElectionConfig("node-a"),
		store,
		nil,
		nil,
		nil,
	)
	if err != nil {
		panic(err)
	}

	nodeB, err := aegis.NewLeaderElector(
		aegis.DefaultLeaderElectionConfig("node-b"),
		store,
		nil,
		nil,
		nil,
	)
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

	time.Sleep(600 * time.Millisecond)

	fmt.Printf("node-a leader? %v | current=%s term=%d\n", nodeA.IsLeader(), nodeA.CurrentLeader().LeaderID, nodeA.CurrentLeader().Term)
	fmt.Printf("node-b leader? %v | current=%s term=%d\n", nodeB.IsLeader(), nodeB.CurrentLeader().LeaderID, nodeB.CurrentLeader().Term)

	if ok, err := nodeA.Resign(context.Background()); err != nil {
		panic(err)
	} else {
		fmt.Printf("node-a resign attempted: %v\n", ok)
	}

	time.Sleep(600 * time.Millisecond)

	fmt.Printf("after resignation -> node-a leader? %v | current=%s term=%d\n", nodeA.IsLeader(), nodeA.CurrentLeader().LeaderID, nodeA.CurrentLeader().Term)
	fmt.Printf("after resignation -> node-b leader? %v | current=%s term=%d\n", nodeB.IsLeader(), nodeB.CurrentLeader().LeaderID, nodeB.CurrentLeader().Term)
}

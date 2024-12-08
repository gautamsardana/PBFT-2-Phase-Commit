package logic

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/durationpb"
	"math"
	"time"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/client/config"
)

func PrintBalance(ctx context.Context, req *common.PrintBalanceRequest, conf *config.Config) (*common.PrintBalanceResponse, error) {
	userCluster := int32(math.Ceil(float64(req.User) / float64(conf.DataItemsPerShard)))
	result := map[int32]float32{}

	for _, serverNo := range conf.MapClusterToServers[userCluster] {
		server, err := conf.Pool.GetServer(mapServerNoToServerAddr[serverNo])
		if err != nil {
			return nil, err
		}

		resp, err := server.PrintBalance(context.Background(), req)
		if err != nil {
			return nil, err
		}

		for k, v := range resp.Balance {
			result[k] = v
		}
	}

	return &common.PrintBalanceResponse{Balance: result}, nil
}

func PrintDB(ctx context.Context, req *common.PrintDBRequest, conf *config.Config) (*common.PrintDBResponse, error) {
	serverAddr := mapServerNoToServerAddr[req.Server]
	server, err := conf.Pool.GetServer(serverAddr)
	if err != nil {
		return nil, err
	}
	resp, err := server.PrintDB(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func Performance(_ context.Context, conf *config.Config) (*common.PerformanceResponse, error) {
	fmt.Println(conf.LatencyQueue)

	var totalLatency time.Duration
	completedTxns := len(conf.LatencyQueue)
	for i := 0; i < completedTxns; i++ {
		totalLatency += conf.LatencyQueue[i]
	}

	var throughput float64
	if totalLatency > 0 {
		throughput = float64(completedTxns) / totalLatency.Seconds()
	}

	resp := &common.PerformanceResponse{
		Latency:    durationpb.New(totalLatency),
		Throughput: float32(throughput),
	}
	return resp, nil
}

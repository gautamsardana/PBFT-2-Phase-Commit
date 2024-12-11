package api

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/emptypb"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/client/config"
	"GolandProjects/2pcbyz-gautamsardana/client/logic"
)

type Client struct {
	common.UnimplementedByz2PCServer
	Config *config.Config
}

func (c *Client) ProcessTxnSet(ctx context.Context, req *common.TxnSet) (*emptypb.Empty, error) {
	err := logic.ProcessTxnSet(ctx, req, c.Config)
	if err != nil {
		fmt.Printf("Error processing txn from load balancer: %v", err)
		return nil, err
	}
	return nil, nil
}

func (c *Client) Callback(ctx context.Context, req *common.ProcessTxnResponse) (*emptypb.Empty, error) {
	logic.Callback(ctx, c.Config, req)
	return nil, nil
}

func (c *Client) PrintBalance(ctx context.Context, req *common.PrintBalanceRequest) (*common.PrintBalanceResponse, error) {
	resp, err := logic.PrintBalance(ctx, req, c.Config)
	if err != nil {
		fmt.Printf("Error printing balance: %v", err)
		return nil, err
	}
	return resp, nil
}

func (c *Client) PrintDB(ctx context.Context, req *common.PrintDBRequest) (*common.PrintDBResponse, error) {
	resp, err := logic.PrintDB(ctx, req, c.Config)
	if err != nil {
		fmt.Printf("Error printing logs: %v", err)
		return nil, err
	}
	return resp, nil
}

func (c *Client) Performance(ctx context.Context, _ *emptypb.Empty) (*common.PerformanceResponse, error) {
	resp, err := logic.Performance(ctx, c.Config)
	if err != nil {
		fmt.Printf("Error evaluating performance: %v", err)
		return nil, err
	}
	return resp, nil
}

func (c *Client) Benchmark(ctx context.Context, req *common.BenchmarkRequest) (*common.PerformanceResponse, error) {
	resp, err := logic.Benchmark(ctx, c.Config, req)
	if err != nil {
		fmt.Printf("Error evaluating benchmark metrics: %v", err)
		return nil, err
	}
	return resp, nil
}

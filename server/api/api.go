package api

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"sync"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/logic"
)

type Server struct {
	common.UnimplementedByz2PCServer
	Config *config.Config
}

func (s *Server) UpdateServerState(ctx context.Context, req *common.UpdateServerStateRequest) (*emptypb.Empty, error) {
	fmt.Printf("isAlive set to %t\n", req.IsAlive)
	s.Config.IsAlive = req.IsAlive
	s.Config.ClusterNumber = req.ClusterNumber
	s.Config.DataItemsPerShard = req.DataItemsPerShard

	s.Config.UserLocks = make([]sync.Mutex, s.Config.DataItemsPerShard)

	for key, cluster := range req.Clusters {
		s.Config.MapClusterToServers[key] = cluster.Values
	}
	return nil, nil
}

func (s *Server) ProcessTxn(ctx context.Context, req *common.TxnRequest) (*emptypb.Empty, error) {
	go func() {
		err := logic.ProcessTxn(ctx, s.Config, req)
		if err != nil {
			log.Printf("ProcessTxnError: %v\n", err)
		}
	}()

	return nil, nil
}

func (s *Server) PrePrepare(ctx context.Context, req *common.PBFTRequestResponse) (*common.PBFTRequestResponse, error) {
	resp, err := logic.ReceivePrePrepare(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("PrePrepareError: %v\n", err)
		return nil, err
	}
	return resp, nil
}

func (s *Server) Prepare(ctx context.Context, req *common.PBFTRequestResponse) (*common.PBFTRequestResponse, error) {
	resp, err := logic.ReceivePrepare(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("PrepareError: %v\n", err)
		return nil, err
	}
	return resp, nil
}

func (s *Server) Commit(ctx context.Context, req *common.PBFTRequestResponse) (*emptypb.Empty, error) {
	err := logic.ReceiveCommit(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("PrepareError: %v\n", err)
		return nil, err
	}
	return nil, nil
}

func (s *Server) PrintBalance(ctx context.Context, req *common.PrintBalanceRequest) (*common.PrintBalanceResponse, error) {
	fmt.Printf("received PrintBalance request\n")
	resp, err := logic.PrintBalance(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("PrintBalanceError: %v\n", err)
	}
	return resp, nil
}

func (s *Server) PrintDB(ctx context.Context, req *common.PrintDBRequest) (*common.PrintDBResponse, error) {
	fmt.Printf("received PrintDB request\n")
	resp, err := logic.PrintDB(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("PrintDBError: %v\n", err)
	}
	return resp, nil
}

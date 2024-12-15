package api

import (
	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/logic"
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
)

type Server struct {
	common.UnimplementedByz2PCServer
	Config *config.Config
}

func (s *Server) UpdateServerState(ctx context.Context, req *common.UpdateServerStateRequest) (*emptypb.Empty, error) {
	fmt.Printf("isAlive set to %t\n", req.IsAlive)
	s.Config.IsAlive = req.IsAlive
	s.Config.IsByzantine = req.IsByzantine
	//s.Config.ClusterNumber = req.ClusterNumber
	//s.Config.DataItemsPerShard = req.DataItemsPerShard

	//for key, cluster := range req.Clusters {
	//	s.Config.MapClusterToServers[key] = cluster.Values
	//}
	return nil, nil
}

func (s *Server) ProcessTxn(ctx context.Context, req *common.TxnRequest) (*emptypb.Empty, error) {
	go func() {
		err := logic.ProcessTxn(ctx, s.Config, req, false)
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
		fmt.Printf("CommitError: %v\n", err)
		return nil, err
	}
	return nil, nil
}

func (s *Server) Sync(ctx context.Context, req *common.PBFTRequestResponse) (*common.PBFTRequestResponse, error) {
	resp, err := logic.ReceiveSyncRequest(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("SyncError: %v\n", err)
		return nil, err
	}
	return resp, nil
}

func (s *Server) TwoPCPrepareRequest(ctx context.Context, req *common.PBFTRequestResponse) (*emptypb.Empty, error) {
	err := logic.ReceiveTwoPCPrepareRequest(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("TwoPCPrepareRequestError: %v\n", err)
		return nil, err
	}
	return nil, nil
}

func (s *Server) TwoPCPrepareResponse(ctx context.Context, req *common.PBFTRequestResponse) (*emptypb.Empty, error) {
	err := logic.ReceiveTwoPCPrepareResponse(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("ReceiveTwoPCResponseError: %v\n", err)
		return nil, err
	}
	return nil, nil
}

func (s *Server) TwoPCCommitRequest(ctx context.Context, req *common.PBFTRequestResponse) (*emptypb.Empty, error) {
	err := logic.ReceiveTwoPCCommit(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("TwoPCCommitRequestError: %v\n", err)
		return nil, err
	}
	return nil, nil
}

func (s *Server) TwoPCCommit(ctx context.Context, req *common.TxnRequest) (*emptypb.Empty, error) {
	err := logic.TwoPCCommit(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("TwoPCCommitError: %v\n", err)
		return nil, err
	}
	return nil, nil
}

func (s *Server) TwoPCAbort(ctx context.Context, req *common.TxnRequest) (*emptypb.Empty, error) {
	err := logic.TwoPCAbort(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("TwoPCAbortError: %v\n", err)
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

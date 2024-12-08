package api

import (
	"context"
	"fmt"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/logic"
)

type Server struct {
	common.UnimplementedByz2PCServer
	Config *config.Config
}

func (s *Server) PrintBalance(ctx context.Context, req *common.PrintBalanceRequest) (*common.PrintBalanceResponse, error) {
	fmt.Printf("Server %d: received PrintBalance request\n", s.Config.ServerNumber)
	resp, err := logic.PrintBalance(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("PrintBalanceError: %v\n", err)
	}
	return resp, nil
}

func (s *Server) PrintDB(ctx context.Context, req *common.PrintDBRequest) (*common.PrintDBResponse, error) {
	fmt.Printf("Server %d: received PrintDB request\n", s.Config.ServerNumber)
	resp, err := logic.PrintDB(ctx, s.Config, req)
	if err != nil {
		fmt.Printf("PrintDBError: %v\n", err)
	}
	return resp, nil
}

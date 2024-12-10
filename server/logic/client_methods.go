package logic

import (
	"context"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
)

func PrintBalance(ctx context.Context, conf *config.Config, req *common.PrintBalanceRequest) (*common.PrintBalanceResponse, error) {
	balance, err := datastore.GetBalance(conf.DataStore, req.User)
	if err != nil {
		return nil, err
	}
	return &common.PrintBalanceResponse{Balance: map[int32]float32{conf.ServerNumber: balance}}, nil
}

func PrintDB(ctx context.Context, conf *config.Config, req *common.PrintDBRequest) (*common.PrintDBResponse, error) {
	executedTxns, err := datastore.GetExecutedTxns(conf.DataStore)
	if err != nil {
		return nil, err
	}
	return &common.PrintDBResponse{Txns: executedTxns}, err
}

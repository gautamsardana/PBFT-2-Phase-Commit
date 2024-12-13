package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
)

func ReceiveTwoPCResponse(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) error {
	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(req.TxnRequest, txnReq)
	if err != nil {
		return err
	}

	fmt.Printf("received response from participant cluster for txn: %s\n", txnReq.TxnID)

	err = AddTwoPCMessages(conf, req, MessageTypeTwoPCPrepareFromParticipant)
	if err != nil {
		return err
	}

	if GetLeaderNumber(conf, conf.ClusterNumber) != conf.ServerNumber {
		return nil
	}

	err = StartConsensus(conf, txnReq, OutcomeCommit)
	if err != nil {
		return err
	}

	go func() {
		err = TwoPCCommit(ctx, conf, txnReq)
		if err != nil {
			fmt.Printf("TwoPCCommit failed: %v\n", err)
		}
		SendReplyToClient(conf, txnReq)
	}()

	reqBytes, err := json.Marshal(txnReq)
	if err != nil {
		return err
	}

	sign, err := SignMessage(conf.PrivateKey, reqBytes)
	if err != nil {
		return err
	}

	twoPCCommitReq := &common.PBFTRequestResponse{
		SignedMessage: reqBytes,
		Sign:          sign,
		Outcome:       OutcomeCommit,
		ServerNo:      conf.ServerNumber,
	}

	receiverCluster := math.Ceil(float64(txnReq.Receiver) / float64(conf.DataItemsPerShard))
	var wg sync.WaitGroup
	for _, serverNo := range conf.MapClusterToServers[int32(receiverCluster)] {
		if serverNo == conf.ServerNumber {
			continue
		}
		wg.Add(1)
		go func(serverAddress string) {
			defer wg.Done()
			server, err := conf.Pool.GetServer(serverAddress)
			if err != nil {
				fmt.Println(err)
			}
			_, err = server.TwoPCCommitRequest(context.Background(), twoPCCommitReq)
			if err != nil {
				fmt.Println(err)
			}

		}(config.MapServerNumberToAddress[serverNo])
	}
	wg.Wait()
	return nil
}

package logic

import (
	"context"
	"encoding/json"
	"fmt"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
)

func ReceiveTwoPCCommit(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) (*common.PBFTRequestResponse, error) {
	serverAddr := config.MapServerNumberToAddress[req.ServerNo]
	publicKey, err := conf.PublicKeys.GetPublicKey(serverAddr)
	if err != nil {
		return nil, err
	}

	err = VerifySignature(publicKey, req.SignedMessage, req.Sign)
	if err != nil {
		return nil, err
	}

	txnReq := &common.TxnRequest{}
	err = json.Unmarshal(req.SignedMessage, txnReq)
	if err != nil {
		return nil, err
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, txnReq.TxnID)
	if err != nil {
		return nil, err
	}

	fmt.Printf("received TwoPCCommit from coordinator cluster with request: %v\n", dbTxn)

	txnBytes, err := json.Marshal(dbTxn)
	if err != nil {
		return nil, err
	}
	sign, err := SignMessage(conf.PrivateKey, txnBytes)
	if err != nil {
		return nil, err
	}

	resp := &common.PBFTRequestResponse{
		SignedMessage: txnBytes,
		Sign:          sign,
		ServerNo:      conf.ServerNumber,
	}

	if GetLeaderNumber(conf, conf.ClusterNumber) == conf.ServerNumber {
		conf.TwoPCChan[txnReq.TxnID] <- req
	}

	return resp, nil
}

func WaitForCoordinatorResponse(conf *config.Config, txnReq *common.TxnRequest) {
	select {
	case <-conf.TwoPCTimer[txnReq.TxnID].C:
		fmt.Printf("no response from coordinator cluster, outcome = abort\n")
		ProcessTwoPCCommit(context.Background(), conf, txnReq, OutcomeAbort)
		return
	case resp := <-conf.TwoPCChan[txnReq.TxnID]:
		conf.TwoPCTimer[txnReq.TxnID].Stop()
		if resp.Outcome == OutcomeCommit {
			fmt.Printf("got response from participant cluster, outcome = commit\n")
			ProcessTwoPCCommit(context.Background(), conf, txnReq, OutcomeCommit)
		} else if resp.Outcome == OutcomeAbort {
			fmt.Printf("got response from participant cluster,  outcome = abort\n")
			ProcessTwoPCCommit(context.Background(), conf, txnReq, OutcomeAbort)
		}
		return
	}
}

func ProcessTwoPCCommit(ctx context.Context, conf *config.Config, txnReq *common.TxnRequest, outcome string) {
	err := StartConsensus(conf, txnReq, outcome)
	if err != nil {
		fmt.Printf("failed to start consensus cluster: %v\n", err)
	}

	if outcome == OutcomeCommit {
		err = TwoPCCommit(ctx, conf, txnReq)
		if err != nil {
			fmt.Printf("TwoPC commit failed: %v\n", err)
		}
	} else if outcome == OutcomeAbort {
		err = TwoPCAbort(ctx, conf, txnReq)
		if err != nil {
			fmt.Printf("TwoPC abort failed: %v\n", err)
		}
	}

	SendReplyToClient(conf, txnReq)
}

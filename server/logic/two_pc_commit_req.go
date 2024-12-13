package logic

import (
	"context"
	"encoding/json"
	"fmt"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
)

func ReceiveTwoPCCommit(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) error {
	serverAddr := config.MapServerNumberToAddress[req.ServerNo]
	publicKey, err := conf.PublicKeys.GetPublicKey(serverAddr)
	if err != nil {
		return err
	}

	err = VerifySignature(publicKey, req.SignedMessage, req.Sign)
	if err != nil {
		return err
	}

	txnReq := &common.TxnRequest{}
	err = json.Unmarshal(req.SignedMessage, txnReq)
	if err != nil {
		return err
	}

	fmt.Printf("received TwoPCCommit from coordinator cluster with request: %v\n", txnReq)

	if GetLeaderNumber(conf, conf.ClusterNumber) != conf.ServerNumber {
		return nil
	}

	err = StartConsensus(conf, txnReq, req.Outcome)
	if err != nil {
		return err
	}

	if req.Outcome == OutcomeCommit {
		err = TwoPCCommit(ctx, conf, txnReq)
		if err != nil {
			return err
		}
	} else if req.Outcome == OutcomeAbort {
		err = TwoPCAbort(ctx, conf, txnReq)
		if err != nil {
			return err
		}
	}

	SendReplyToClient(conf, txnReq)

	return nil
}

package logic

import (
	"context"
	"fmt"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
	"encoding/json"
)

func SyncIfServerSlow(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) error {
	signedMessage := &common.SignedMessage{}
	err := json.Unmarshal(req.SignedMessage, signedMessage)
	if err != nil {
		return err
	}

	lastExecutedSeq := conf.PBFT.GetLastExecutedSequenceNumber()
	if signedMessage.LastExecutedSequence <= lastExecutedSeq {
		return nil
	}

	fmt.Printf("server is slow, asking for new txns...")

	signedReq := &common.SignedMessage{
		LastExecutedSequence: conf.PBFT.GetLastExecutedSequenceNumber(),
	}

	signedReqBytes, err := json.Marshal(signedReq)
	if err != nil {
		return err
	}
	sign, err := SignMessage(conf.PrivateKey, signedReqBytes)
	if err != nil {
		return err
	}

	syncReq := &common.PBFTRequestResponse{
		SignedMessage: signedReqBytes,
		Sign:          sign,
		ServerNo:      conf.ServerNumber,
	}

	server, err := conf.Pool.GetServer(config.MapServerNumberToAddress[req.ServerNo])
	if err != nil {
		return err
	}
	resp, err := server.Sync(ctx, syncReq)
	if err != nil {
		return err
	}

	err = AddNewTxns(ctx, conf, resp)
	if err != nil {
		return err
	}

	return nil
}

func AddNewTxns(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) error {
	fmt.Printf("i was slow, adding these new txns: %v\n", req)

	serverAddr := config.MapServerNumberToAddress[req.ServerNo]
	publicKey, err := conf.PublicKeys.GetPublicKey(serverAddr)
	if err != nil {
		return err
	}
	err = VerifySignature(publicKey, req.SignedMessage, req.Sign)
	if err != nil {
		return err
	}

	var newTxns []*common.TxnRequest
	err = json.Unmarshal(req.SignedMessage, &newTxns)
	if err != nil {
		return err
	}

	for _, txn := range newTxns {
		_ = AcquireLock(conf, txn)
		err = ExecuteTxn(conf, txn, true)
		if err != nil {
			ReleaseLock(conf, txn)
			return err
		}
		conf.PBFT.IncrementLastExecutedSequenceNumber()
		ReleaseLock(conf, txn)
	}

	return nil
}

func ReceiveSyncRequest(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) (*common.PBFTRequestResponse, error) {
	serverAddr := config.MapServerNumberToAddress[req.ServerNo]
	publicKey, err := conf.PublicKeys.GetPublicKey(serverAddr)
	if err != nil {
		return nil, err
	}
	err = VerifySignature(publicKey, req.SignedMessage, req.Sign)
	if err != nil {
		return nil, err
	}

	signedMessage := &common.SignedMessage{}
	err = json.Unmarshal(req.SignedMessage, signedMessage)
	if err != nil {
		return nil, err
	}

	txns, err := datastore.GetExecutedTransactionsAfterSequence(conf.DataStore, signedMessage.LastExecutedSequence)
	if err != nil {
		return nil, err
	}

	signedMsgBytes, err := json.Marshal(txns)
	if err != nil {
		return nil, err
	}
	sign, err := SignMessage(conf.PrivateKey, signedMsgBytes)
	if err != nil {
		return nil, err
	}

	syncResp := &common.PBFTRequestResponse{
		SignedMessage: signedMsgBytes,
		Sign:          sign,
		ServerNo:      conf.ServerNumber,
	}

	return syncResp, nil
}

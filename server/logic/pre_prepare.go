package logic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
)

func SendPrePrepare(conf *config.Config, req *common.TxnRequest) error {
	fmt.Printf("Sending Prepare with request: %v\n", req)

	signedReq := &common.SignedMessage{
		ViewNumber:     req.ViewNo,
		SequenceNumber: req.SeqNo,
		Digest:         req.Digest,
	}
	signedReqBytes, err := json.Marshal(signedReq)
	if err != nil {
		return err
	}
	sign, err := SignMessage(conf.PrivateKey, signedReqBytes)
	if err != nil {
		return err
	}

	requestBytes, err := json.Marshal(req)
	if err != nil {
		fmt.Println(err)
		return err
	}

	prePrepareReq := &common.PBFTRequestResponse{
		SignedMessage: signedReqBytes,
		Sign:          sign,
		TxnRequest:    requestBytes,
		ServerNo:      conf.ServerNumber,
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, req.TxnID)
	if err != nil {
		return err
	}

	dbTxn.Status = StatusPrePrepared
	err = datastore.UpdateTransactionStatus(conf.DataStore, dbTxn)
	if err != nil {
		return err
	}

	//todo: send context with a timeout? Handle timeouts in some way (if majority not reached)

	var wg sync.WaitGroup
	for _, serverNo := range conf.MapClusterToServers[conf.ClusterNumber] {
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
			resp, err := server.PrePrepare(context.Background(), prePrepareReq)
			if err != nil {
				fmt.Println(err)
			}

			HandlePBFTResponse(conf, resp, MessageTypePrePrepare)

		}(config.MapServerNumberToAddress[serverNo])
	}
	wg.Wait()

	return nil
}

func ReceivePrePrepare(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) (*common.PBFTRequestResponse, error) {
	//todo: check if txn already exists? any scenario where i
	// already have in my db but i get another prepare?

	if !conf.IsAlive {
		return nil, errors.New("server dead")
	}

	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(req.TxnRequest, txnReq)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Received PrePrepare for request: %v\n", txnReq)

	txnReq.Status = StatusPrePrepared
	err = datastore.InsertTransaction(conf.DataStore, txnReq)
	if err != nil {
		return nil, err
	}

	err = VerifyPBFTMessage(ctx, conf, req, txnReq, MessageTypePrePrepare)
	if err != nil {
		UpdateTxnFailed(conf, txnReq, err)
		return nil, err
	}

	lockErr := AcquireLockWithAbort(conf, txnReq)
	if lockErr != nil {
		UpdateTxnFailed(conf, txnReq, lockErr)
		return nil, lockErr
	}

	defer func() {
		if err != nil {
			UpdateTxnFailed(conf, txnReq, err)
			ReleaseLock(conf, txnReq)
		}
	}()

	err = ValidateBalance(conf, txnReq)
	if err != nil {
		return nil, err
	}

	return SendPrePrepareResponse(conf, req)
}

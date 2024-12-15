package logic

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
)

func SendPrePrepare(conf *config.Config, req *common.TxnRequest, outcome string) error {
	fmt.Printf("Sending pre-prepare with request: %v\n", req)

	signedReq := &common.SignedMessage{
		ViewNumber:           req.ViewNo,
		SequenceNumber:       req.SeqNo,
		Digest:               req.Digest,
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
		Outcome:       outcome,
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, req.TxnID)
	if err != nil {
		return err
	}

	GetTxnUpdatedStatusLeader(dbTxn, MessageTypePrePrepare)
	req.Status = dbTxn.Status
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
				return
			}
			resp, err := server.PrePrepare(context.Background(), prePrepareReq)
			if err != nil || resp == nil {
				return
			}
			if resp.Outcome == EmptyString {
				HandlePBFTResponse(conf, resp, MessageTypePrepare)
			} else {
				HandlePBFTResponse(conf, resp, MessageTypeTwoPCPrepare)
			}
		}(config.MapServerNumberToAddress[serverNo])
	}
	wg.Wait()

	return nil
}

func ReceivePrePrepare(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) (*common.PBFTRequestResponse, error) {
	if !conf.IsAlive {
		return nil, errors.New("server dead")
	}
	if conf.IsByzantine {
		return nil, errors.New("server byzantine")
	}

	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(req.TxnRequest, txnReq)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			UpdateTxnFailed(conf, txnReq, err)
		}
	}()

	fmt.Printf("Received PrePrepare for request: %v\n", txnReq)

	/*
		err = SyncIfServerSlow(ctx, conf, req)
		if err != nil {
			return nil, err
		}
	*/

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, txnReq.TxnID)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	if dbTxn == nil {
		txnReq.Status = StatusPrepared
		err = datastore.InsertTransaction(conf.DataStore, txnReq)
		if err != nil {
			return nil, err
		}
		AcquireLock(conf, txnReq)

		err = ValidateBalance(conf, txnReq)
		if err != nil {
			return nil, err
		}
	} else {
		GetTxnUpdatedStatusFollower(dbTxn, MessageTypePrePrepare)
		txnReq.Status = dbTxn.Status
		err = datastore.UpdateTransactionStatus(conf.DataStore, dbTxn)
		if err != nil {
			return nil, err
		}
	}

	err = VerifyPBFTMessage(ctx, conf, req, txnReq, MessageTypePrePrepare)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Sending pre-prepare response for txn:%s\n", txnReq.TxnID)
	return SendPrePrepareResponse(conf, req)
}

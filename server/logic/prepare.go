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

func SendPrepare(conf *config.Config, req *common.TxnRequest) error {
	fmt.Printf("Sending prepare for request: %v\n", req)

	prepareMessages, err := datastore.GetPBFTMessages(conf.DataStore, req.TxnID, MessageTypePrepare)
	if err != nil {
		return err
	}
	if len(prepareMessages) < int(conf.Majority)-1 {
		return errors.New("not enough prepare messages")
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, req.TxnID)
	if err != nil {
		return err
	}

	dbTxn.Status = StatusPrepared
	err = datastore.UpdateTransactionStatus(conf.DataStore, dbTxn)
	if err != nil {
		return err
	}

	cert := &common.Certificate{
		ViewNumber:     dbTxn.ViewNo,
		SequenceNumber: dbTxn.SeqNo,
		Messages:       prepareMessages,
	}

	certBytes, err := json.Marshal(cert)
	if err != nil {
		return err
	}

	sign, err := SignMessage(conf.PrivateKey, certBytes)
	if err != nil {
		return err
	}

	txnBytes, err := json.Marshal(dbTxn)
	if err != nil {
		return err
	}

	prepareReq := &common.PBFTRequestResponse{
		SignedMessage: certBytes,
		Sign:          sign,
		TxnRequest:    txnBytes,
		ServerNo:      conf.ServerNumber,
	}

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
			resp, err := server.Prepare(context.Background(), prepareReq)
			if err != nil {
				fmt.Println(err)
			}

			HandlePBFTResponse(conf, resp, MessageTypeCommit)

		}(config.MapServerNumberToAddress[serverNo])
	}
	wg.Wait()

	return nil
}

func ReceivePrepare(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) (*common.PBFTRequestResponse, error) {
	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(req.TxnRequest, txnReq)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			UpdateTxnFailed(conf, txnReq, err)
			ReleaseLock(conf, txnReq)
		}
	}()

	fmt.Printf("Received Prepare for request: %v\n", txnReq)
	err = VerifyPrepare(ctx, conf, req, txnReq)
	if err != nil {
		return nil, err
	}

	err = AddPrepareMessages(conf, req)
	if err != nil {
		return nil, err
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, txnReq.TxnID)
	if err != nil {
		return nil, err
	}
	dbTxn.Status = StatusCommitted
	err = datastore.UpdateTransactionStatus(conf.DataStore, dbTxn)
	if err != nil {
		return nil, err
	}
	return SendPrepareResponse(conf, req, txnReq)
}

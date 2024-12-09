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

	prePrepareMessages, err := datastore.GetPBFTMessages(conf.DataStore, req.TxnID, MessageTypePrePrepare)
	if err != nil {
		return err
	}
	if len(prePrepareMessages) < int(conf.Majority) {
		return errors.New("not enough pre-prepare messages")
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
		Messages:       prePrepareMessages,
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

			HandlePBFTResponse(conf, resp, MessageTypePrepare)

		}(MapServerNumberToAddress[serverNo])
	}
	wg.Wait()

	return nil
}

func ReceivePrepare(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) (*common.PBFTRequestResponse, error) {
	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(req.TxnRequest, txnReq)
	if err != nil {
		UpdateTxnFailed(conf, txnReq, err)
		ReleaseLock(conf, txnReq)
		return nil, err
	}

	defer func() {
		if err != nil {
			UpdateTxnFailed(conf, txnReq, err)
			ReleaseLock(conf, txnReq)
		}
	}()

	fmt.Printf("Received Prepared for request: %v\n", txnReq)
	err = VerifyPrepare(ctx, conf, req, txnReq)
	if err != nil {
		return nil, err
	}

	err = AddPrePrepareMessages(conf, req)
	if err != nil {
		return nil, err
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, txnReq.TxnID)
	if err != nil {
		return nil, err
	}
	dbTxn.Status = StatusPrepared
	err = datastore.UpdateTransactionStatus(conf.DataStore, dbTxn)
	if err != nil {
		return nil, err
	}

	return SendPrepareResponse(conf, req, txnReq)
}

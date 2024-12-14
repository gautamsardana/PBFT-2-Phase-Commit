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

func SendPrepare(conf *config.Config, req *common.TxnRequest, outcome string) error {
	fmt.Printf("Sending prepare for request: %v\n", req)

	messageType := EmptyString
	if outcome == EmptyString {
		messageType = MessageTypePrepare
	} else {
		messageType = MessageTypeTwoPCPrepare
	}
	prepareMessages, err := datastore.GetPBFTMessages(conf.DataStore, req.TxnID, messageType)
	if err != nil {
		return err
	}
	if len(prepareMessages) < int(conf.Majority)-1 {
		return errors.New("not enough prepare messages")
	} else if conf.IsByzantine {
		return errors.New("server byzantine")
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, req.TxnID)
	if err != nil {
		return err
	}

	GetTxnUpdatedStatusLeader(dbTxn, MessageTypePrepare)
	req.Status = dbTxn.Status
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
		Outcome:       outcome,
	}

	var wg sync.WaitGroup
	for _, prepareMessage := range prepareMessages {
		serverNo := prepareMessage.Sender
		wg.Add(1)
		go func(serverAddress string) {
			defer wg.Done()
			server, err := conf.Pool.GetServer(serverAddress)
			if err != nil {
				return
			}
			resp, err := server.Prepare(context.Background(), prepareReq)
			if err != nil || resp == nil {
				return
			}
			if resp.Outcome == EmptyString {
				HandlePBFTResponse(conf, resp, MessageTypeCommit)
			} else {
				HandlePBFTResponse(conf, resp, MessageTypeTwoPCCommit)
			}
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
	GetTxnUpdatedStatusFollower(dbTxn, MessageTypePrepare)
	txnReq.Status = dbTxn.Status
	err = datastore.UpdateTransactionStatus(conf.DataStore, dbTxn)
	if err != nil {
		return nil, err
	}
	return SendPrepareResponse(conf, req, txnReq)
}

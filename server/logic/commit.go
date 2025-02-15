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

func SendCommit(conf *config.Config, req *common.TxnRequest, outcome string) error {
	fmt.Printf("Sending commit for request: %v\n", req)

	messageType := EmptyString
	if outcome == EmptyString {
		messageType = MessageTypeCommit
	} else {
		messageType = MessageTypeTwoPCCommit
	}

	commitMessages, err := datastore.GetPBFTMessages(conf.DataStore, req.TxnID, messageType)
	if err != nil {
		return err
	}
	if len(commitMessages) < int(conf.Majority)-1 {
		return errors.New("not enough commit messages")
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, req.TxnID)
	if err != nil {
		return err
	}

	GetTxnUpdatedStatusLeader(dbTxn, MessageTypeCommit)
	req.Status = dbTxn.Status
	err = datastore.UpdateTransactionStatus(conf.DataStore, dbTxn)
	if err != nil {
		return err
	}

	txnBytes, err := json.Marshal(dbTxn)
	if err != nil {
		return err
	}

	cert := &common.Certificate{
		ViewNumber:     dbTxn.ViewNo,
		SequenceNumber: dbTxn.SeqNo,
		Messages:       commitMessages,
	}

	certBytes, err := json.Marshal(cert)
	if err != nil {
		return err
	}

	sign, err := SignMessage(conf.PrivateKey, certBytes)
	if err != nil {
		return err
	}

	commitReq := &common.PBFTRequestResponse{
		SignedMessage: certBytes,
		Sign:          sign,
		TxnRequest:    txnBytes,
		ServerNo:      conf.ServerNumber,
		Outcome:       outcome,
	}

	var wg sync.WaitGroup
	for _, commitMessage := range commitMessages {
		serverNo := commitMessage.Sender
		wg.Add(1)
		go func(serverAddress string) {
			defer wg.Done()
			server, err := conf.Pool.GetServer(serverAddress)
			if err != nil {
				fmt.Println(err)
			}
			_, err = server.Commit(context.Background(), commitReq)
			if err != nil {
				fmt.Println(err)
			}

		}(config.MapServerNumberToAddress[serverNo])
	}
	wg.Wait()

	return nil
}

func ReceiveCommit(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) error {
	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(req.TxnRequest, txnReq)
	if err != nil {
		UpdateTxnFailed(conf, txnReq, err)
		return err
	}

	defer func() {
		if err != nil {
			UpdateTxnFailed(conf, txnReq, err)
		}
	}()

	fmt.Printf("Received commit for request: %v\n", txnReq)
	err = VerifyCommit(ctx, conf, req, txnReq)
	if err != nil {
		return err
	}

	err = AddCommitMessages(conf, req)
	if err != nil {
		return err
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, txnReq.TxnID)
	if err != nil {
		return err
	}

	if dbTxn.Type == TypeIntraShard {
		SendExecuteSignal(conf, txnReq)
	} else {
		if dbTxn.Status == StatusCommitted {
			SendExecuteSignal(conf, txnReq)
		} else if dbTxn.Status == Status2PCCommitted {
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
		}
	}

	return nil
}

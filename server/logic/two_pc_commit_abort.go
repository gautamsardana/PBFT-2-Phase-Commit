package logic

import (
	"context"
	"encoding/json"
	"fmt"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
)

func Commit(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) error {
	//todo: run consensus

	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(req.TxnRequest, txnReq)
	if err != nil {
		return err
	}

	fmt.Printf("Received commit from coordinator cluster for txn: %v\n", txnReq)

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, txnReq.TxnID)
	if err != nil {
		return err
	}

	dbTxn.Status = StatusExecuted
	err = datastore.UpdateTransactionStatus(conf.DataStore, dbTxn)
	if err != nil {
		fmt.Printf("failed to update transaction status: %v\n", err)
	}
	conf.PBFT.IncrementLastExecutedSequenceNumber()
	ReleaseLock(conf, txnReq)

	return nil
}

func Abort(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) error {
	//todo: run consensus

	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(req.TxnRequest, txnReq)
	if err != nil {
		return err
	}

	fmt.Printf("Received abort from coordinator cluster for txn: %v\n", txnReq)

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, txnReq.TxnID)
	if err != nil {
		return err
	}

	dbTxnStatus := dbTxn.Status
	err = RollbackTxn(conf, dbTxn)
	if err != nil {
		return err
	}
	if dbTxnStatus == StatusPreparedToExecute {
		ReleaseLock(conf, dbTxn)
	}

	return nil
}

func RollbackTxn(conf *config.Config, req *common.TxnRequest) error {
	if req.Status == StatusPreparedToExecute {
		if req.Type == TypeCrossShardSender {
			senderBalance, err := datastore.GetBalance(conf.DataStore, req.Sender)
			if err != nil {
				return err
			}
			err = datastore.UpdateBalance(conf.DataStore, datastore.User{User: req.Sender, Balance: senderBalance + req.Amount})
			if err != nil {
				return err
			}
		} else if req.Type == TypeCrossShardReceiver {
			receiverBalance, err := datastore.GetBalance(conf.DataStore, req.Receiver)
			if err != nil {
				return err
			}
			err = datastore.UpdateBalance(conf.DataStore, datastore.User{User: req.Receiver, Balance: receiverBalance - req.Amount})
			if err != nil {
				return err
			}
		}
	}

	req.Status = StatusAborted
	err := datastore.UpdateTransactionStatus(conf.DataStore, req)
	if err != nil {
		fmt.Printf("failed to update transaction status: %v\n", err)
	}

	return nil
}

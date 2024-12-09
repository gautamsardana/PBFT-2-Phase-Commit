package logic

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
)

func ProcessTxn(ctx context.Context, conf *config.Config, req *common.TxnRequest) error {
	fmt.Printf("Received ProcessTxn request: %v\n", req)

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, req.TxnID)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if dbTxn != nil {
		return errors.New("txn id already exists, invalid commit")
	}

	req.Type = GetTxnType(conf, req)
	req.Digest = GetTxnDigest(req)
	req.SeqNo = conf.PBFT.IncrementSequenceNumber()
	req.ViewNo = conf.PBFT.GetViewNumber()

	req.Status = StatusInit
	err = datastore.InsertTransaction(conf.DataStore, req)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			UpdateTxnFailed(conf, req, err)
			ReleaseLock(conf, req)
			SendReplyToClient(conf, req)
		}
	}()

	lockErr := AcquireLockWithAbort(conf, req)
	if lockErr != nil {
		UpdateTxnFailed(conf, req, lockErr)
		return lockErr
	}

	err = ValidateBalance(conf, req)
	if err != nil {
		return err
	}

	err = StartConsensus(conf, req)
	if err != nil {
		return err
	}

	err = ExecuteTxn(conf, req)
	if err != nil {
		return err
	}
	ReleaseLock(conf, req)

	if req.Type == TypeIntraShard {
		go SendReplyToClient(conf, req)
	}

	return nil
}

func StartConsensus(conf *config.Config, req *common.TxnRequest) error {
	err := SendPrePrepare(conf, req)
	if err != nil {
		return err
	}

	err = SendPrepare(conf, req)
	if err != nil {
		return err
	}

	err = SendCommit(conf, req)
	if err != nil {
		return err
	}
	return nil
}

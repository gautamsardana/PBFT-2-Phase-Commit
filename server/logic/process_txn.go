package logic

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"

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
		return errors.New("txn id already exists, invalid txn")
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

	lockErr := AcquireLock(conf, req)
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

	err = ExecuteTxn(conf, req, false)
	if err != nil {
		return err
	}

	if req.Type == TypeIntraShard {
		conf.PBFT.IncrementLastExecutedSequenceNumber()
		ReleaseLock(conf, req)
		go SendReplyToClient(conf, req)
	} else if req.Type == TypeCrossShardSender {
		err = StartTwoPC(conf, req)
		if err != nil {
			_ = RollbackTxn(conf, req)
			return err
		}
	} else {
		// send response back to coordinator cluster
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

func StartTwoPC(conf *config.Config, req *common.TxnRequest) error {
	fmt.Printf("sending request to participant cluster with request: %v\n", req)

	reqBytes, err := json.Marshal(req)
	if err != nil {
		return err
	}

	commitMessages, err := datastore.GetPBFTMessages(conf.DataStore, req.TxnID, MessageTypeCommit)
	if err != nil {
		return err
	}

	cert := &common.Certificate{
		Messages: commitMessages,
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
		TxnRequest:    reqBytes,
		ServerNo:      conf.ServerNumber,
	}

	receiverCluster := math.Ceil(float64(req.Receiver) / float64(conf.DataItemsPerShard))

	var wg sync.WaitGroup
	for _, serverNo := range conf.MapClusterToServers[int32(receiverCluster)] {
		wg.Add(1)
		go func(serverAddress string) {
			defer wg.Done()
			server, err := conf.Pool.GetServer(serverAddress)
			if err != nil {
				fmt.Println(err)
			}
			_, err = server.TwoPCPrepareRequest(context.Background(), commitReq)
			if err != nil {
				fmt.Println(err)
			}

		}(config.MapServerNumberToAddress[serverNo])
	}
	wg.Wait()

	return nil
}

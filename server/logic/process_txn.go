package logic

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
)

func ProcessTxn(ctx context.Context, conf *config.Config, req *common.TxnRequest) error {
	fmt.Printf("Received ProcessTxn request: %v\n", req)

	req.Type = GetTxnType(conf, req)

	AcquireLock(conf, req)
	fmt.Printf("acquired lock for %d and %d\n", req.Sender, req.Receiver)

	err := ValidateBalance(conf, req)
	if err != nil {
		ReleaseLock(conf, req)
		return err
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, req.TxnID)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	defer func() {
		if err != nil {
			UpdateTxnFailed(conf, req, err)
		}
	}()

	if dbTxn == nil {
		req.Digest = GetTxnDigest(req)
		req.SeqNo = conf.PBFT.IncrementSequenceNumber()
		req.ViewNo = conf.PBFT.GetViewNumber()

		req.Status = StatusInit
		err = datastore.InsertTransaction(conf.DataStore, req)
		if err != nil {
			return err
		}
	} else {
		req = dbTxn
		req.Status = StatusInit
		err = datastore.UpdateTransactionStatus(conf.DataStore, req)
		if err != nil {
			return err
		}
	}

	err = StartConsensus(conf, req, "")
	if err != nil {
		return err
	}

	SendExecuteSignal(conf, req)

	return nil
}

func StartConsensus(conf *config.Config, req *common.TxnRequest, outcome string) error {
	err := SendPrePrepare(conf, req, outcome)
	if err != nil {
		return err
	}

	err = SendPrepare(conf, req, outcome)
	if err != nil {
		return err
	}

	err = SendCommit(conf, req, outcome)
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

	conf.TwoPCLock.Lock()
	conf.TwoPCTimer[req.TxnID] = time.NewTimer(5 * time.Second)
	conf.TwoPCChan[req.TxnID] = make(chan struct{})
	conf.TwoPCLock.Unlock()
	go WaitForParticipantResponse(conf, req)

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

func SendExecuteSignal(conf *config.Config, txnReq *common.TxnRequest) {
	conf.PendingTransactionsMutex.Lock()
	conf.PendingTransactions[txnReq.SeqNo] = txnReq
	conf.PendingTransactionsMutex.Unlock()

	select {
	case conf.ExecuteSignal <- struct{}{}:
		fmt.Printf("signalled for execution for txn:%s %d\n", txnReq.TxnID, txnReq.SeqNo)
	default:
		fmt.Println("worker already signaled or signal channel is full")
	}
}

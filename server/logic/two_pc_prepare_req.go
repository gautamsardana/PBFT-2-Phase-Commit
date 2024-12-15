package logic

import (
	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
)

func ReceiveTwoPCPrepareRequest(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) error {
	serverAddr := config.MapServerNumberToAddress[req.ServerNo]
	publicKey, err := conf.PublicKeys.GetPublicKey(serverAddr)
	if err != nil {
		return err
	}

	err = VerifySignature(publicKey, req.SignedMessage, req.Sign)
	if err != nil {
		return err
	}

	txnReq := &common.TxnRequest{}
	err = json.Unmarshal(req.TxnRequest, txnReq)
	if err != nil {
		return err
	}
	fmt.Printf("received TwoPCPrepare from coordinator cluster with request: %v\n", txnReq)

	err = AddTwoPCMessages(conf, req, MessageTypeTwoPCPrepareFromCoordinator)
	if err != nil {
		return err
	}

	if GetLeaderNumber(conf, conf.ClusterNumber) != conf.ServerNumber {
		return nil
	}

	err = ProcessTxn(ctx, conf, txnReq, false)
	if err != nil {
		return err
	}

	commitMessages, err := datastore.GetPBFTMessages(conf.DataStore, txnReq.TxnID, MessageTypeCommit)
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

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, txnReq.TxnID)
	if err != nil {
		return err
	}

	txnBytes, err := json.Marshal(dbTxn)
	if err != nil {
		return err
	}

	commitReq := &common.PBFTRequestResponse{
		SignedMessage: certBytes,
		Sign:          sign,
		TxnRequest:    txnBytes,
		ServerNo:      conf.ServerNumber,
	}

	fmt.Printf("sending response to coordinator cluster for txn: %s\n", dbTxn.TxnID)

	senderCluster := math.Ceil(float64(txnReq.Sender) / float64(conf.DataItemsPerShard))

	var wg sync.WaitGroup
	for _, serverNo := range conf.MapClusterToServers[int32(senderCluster)] {
		wg.Add(1)
		go func(serverAddress string) {
			defer wg.Done()
			server, err := conf.Pool.GetServer(serverAddress)
			if err != nil {
				fmt.Println(err)
			}
			_, err = server.TwoPCPrepareResponse(context.Background(), commitReq)
			if err != nil {
				fmt.Println(err)
			}

		}(config.MapServerNumberToAddress[serverNo])
	}
	wg.Wait()

	return nil
}

func AddTwoPCMessages(conf *config.Config, req *common.PBFTRequestResponse, messageType string) error {
	cert := &common.Certificate{}
	err := json.Unmarshal(req.SignedMessage, cert)
	if err != nil {
		return err
	}

	if len(cert.Messages) < int(conf.Majority-1) {
		return errors.New("not enough messages")
	}

	for _, twoPCCommitRequest := range cert.Messages {
		twoPCCommitRequest.MessageType = messageType
		err = datastore.InsertPBFTMessage(conf.DataStore, twoPCCommitRequest)
		if err != nil {
			return err
		}
	}
	return nil
}

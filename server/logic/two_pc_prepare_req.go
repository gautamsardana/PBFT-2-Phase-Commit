package logic

import (
	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// participant nodes get 2pc request from leader

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

	err = VerifyTwoPCMessages(conf, req, MessageTypeTwoPCPrepareFromCoordinator)
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
	return nil
}

// participant leader sends 2pc prepare response to coordinator nodes

func SendTwoPCPrepareResponse(conf *config.Config, req *common.TxnRequest) error {
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

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, req.TxnID)
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
		Outcome:       OutcomeCommit,
	}

	fmt.Printf("sending response to coordinator cluster for txn: %s\n", dbTxn.TxnID)

	conf.TwoPCLock.Lock()
	conf.TwoPCTimer[req.TxnID] = time.NewTimer(5 * time.Second)
	conf.TwoPCChan[req.TxnID] = make(chan *common.PBFTRequestResponse)
	conf.TwoPCLock.Unlock()
	go WaitForCoordinatorResponse(conf, req)

	senderCluster := math.Ceil(float64(req.Sender) / float64(conf.DataItemsPerShard))

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

func VerifyTwoPCMessages(conf *config.Config, req *common.PBFTRequestResponse, messageType string) error {
	cert := &common.Certificate{}
	err := json.Unmarshal(req.SignedMessage, cert)
	if err != nil {
		return err
	}

	if len(cert.Messages) < int(conf.Majority-1) {
		return errors.New("not enough messages")
	}

	for _, commitMessage := range cert.Messages {
		serverAddr := config.MapServerNumberToAddress[commitMessage.Sender]
		publicKey, err := conf.PublicKeys.GetPublicKey(serverAddr)
		if err != nil {
			return err
		}

		payload, _ := base64.StdEncoding.DecodeString(commitMessage.Payload)
		sign, _ := base64.StdEncoding.DecodeString(commitMessage.Sign)

		err = VerifySignature(publicKey, payload, sign)
		if err != nil {
			return err
		}

		commitMessage.MessageType = messageType
		err = datastore.InsertPBFTMessage(conf.DataStore, commitMessage)
		if err != nil {
			return err
		}
	}
	return nil
}

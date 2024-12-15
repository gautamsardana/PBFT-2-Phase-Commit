package logic

import (
	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
)

func ReceiveTwoPCPrepareResponse(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) error {
	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(req.TxnRequest, txnReq)
	if err != nil {
		return err
	}

	fmt.Printf("received response from participant cluster for txn: %s\n", txnReq.TxnID)

	err = VerifyTwoPCMessages(conf, req, MessageTypeTwoPCPrepareFromParticipant)
	if err != nil {
		return err
	}

	if GetLeaderNumber(conf, conf.ClusterNumber) != conf.ServerNumber {
		return nil
	}

	conf.TwoPCLock.Lock()
	conf.TwoPCChan[txnReq.TxnID] <- struct{}{}
	conf.TwoPCLock.Unlock()

	return nil
}

func WaitForParticipantResponse(conf *config.Config, req *common.TxnRequest) {
	select {
	case <-conf.TwoPCTimer[req.TxnID].C:
		fmt.Printf("outcome = abort\n")
		ProcessTwoPCPrepareResponse(context.Background(), conf, req, OutcomeAbort)
		return
	case <-conf.TwoPCChan[req.TxnID]:
		conf.TwoPCTimer[req.TxnID].Stop()
		fmt.Printf("outcome = commit\n")
		ProcessTwoPCPrepareResponse(context.Background(), conf, req, OutcomeCommit)
		return
	}
}

func ProcessTwoPCPrepareResponse(ctx context.Context, conf *config.Config, txnReq *common.TxnRequest, outcome string) {
	err := StartConsensus(conf, txnReq, outcome)
	if err != nil {
		fmt.Printf("failed to start consensus, err: %v\n", err)
	}

	go func() {
		if outcome == OutcomeCommit {
			err = TwoPCCommit(ctx, conf, txnReq)
			if err != nil {
				fmt.Printf("TwoPCCommit failed: %v\n", err)
			}
		} else if outcome == OutcomeAbort {
			err = TwoPCAbort(ctx, conf, txnReq)
			if err != nil {
				fmt.Printf("TwoPCCommit failed: %v\n", err)
			}
		}
		SendReplyToClient(conf, txnReq)
	}()

	reqBytes, err := json.Marshal(txnReq)
	if err != nil {
		fmt.Printf("Failed to marshal req: %v\n", err)
	}

	sign, err := SignMessage(conf.PrivateKey, reqBytes)
	if err != nil {
		fmt.Printf("Failed to sign req: %v\n", err)
	}

	twoPCCommitReq := &common.PBFTRequestResponse{
		SignedMessage: reqBytes,
		Sign:          sign,
		Outcome:       outcome,
		ServerNo:      conf.ServerNumber,
	}

	receiverCluster := math.Ceil(float64(txnReq.Receiver) / float64(conf.DataItemsPerShard))
	var wg sync.WaitGroup
	for _, serverNo := range conf.MapClusterToServers[int32(receiverCluster)] {
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
			_, err = server.TwoPCCommitRequest(context.Background(), twoPCCommitReq)
			if err != nil {
				fmt.Println(err)
			}

		}(config.MapServerNumberToAddress[serverNo])
	}
	wg.Wait()
}

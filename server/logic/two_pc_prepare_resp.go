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

func ReceiveTwoPCPrepareResponse(ctx context.Context, conf *config.Config, resp *common.PBFTRequestResponse) error {
	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(resp.TxnRequest, txnReq)
	if err != nil {
		return err
	}

	fmt.Printf("received response from participant cluster for txn: %s\n", txnReq.TxnID)

	err = VerifyTwoPCMessages(conf, resp, MessageTypeTwoPCPrepareFromParticipant)
	if err != nil {
		return err
	}

	if GetLeaderNumber(conf, conf.ClusterNumber) != conf.ServerNumber {
		return nil
	}

	conf.TwoPCChan[txnReq.TxnID] <- resp

	return nil
}

func WaitForParticipantResponse(conf *config.Config, req *common.TxnRequest) {
	select {
	case <-conf.TwoPCTimer[req.TxnID].C:
		fmt.Printf("no response from participant cluster, outcome = abort\n")
		ProcessTwoPCPrepareResponse(context.Background(), conf, req, OutcomeAbort)
		return
	case resp := <-conf.TwoPCChan[req.TxnID]:
		conf.TwoPCTimer[req.TxnID].Stop()

		if resp.Outcome == OutcomeCommit {
			fmt.Printf("got response from participant cluster, outcome = commit\n")
			ProcessTwoPCPrepareResponse(context.Background(), conf, req, OutcomeCommit)
		} else if resp.Outcome == OutcomeAbort {
			fmt.Printf("got response from participant cluster,  outcome = abort\n")
			ProcessTwoPCPrepareResponse(context.Background(), conf, req, OutcomeAbort)
		}
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
				fmt.Printf("TwoPC commit failed: %v\n", err)
			}
		} else if outcome == OutcomeAbort {
			err = TwoPCAbort(ctx, conf, txnReq)
			if err != nil {
				fmt.Printf("TwoPC abort failed: %v\n", err)
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
			resp, err := server.TwoPCCommitRequest(context.Background(), twoPCCommitReq)
			if err != nil {
				fmt.Println(err)
			}
			if resp != nil {
				serverAddr := config.MapServerNumberToAddress[resp.ServerNo]
				publicKey, err := conf.PublicKeys.GetPublicKey(serverAddr)
				if err != nil {
					fmt.Println(err)
				}

				err = VerifySignature(publicKey, resp.SignedMessage, resp.Sign)
				if err != nil {
					fmt.Println(err)
				}
			}
		}(config.MapServerNumberToAddress[serverNo])
	}
	wg.Wait()
}

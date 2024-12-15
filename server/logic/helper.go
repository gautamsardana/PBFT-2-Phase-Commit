package logic

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math"
	"time"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
)

const (
	TypeIntraShard         = "IntraShard"
	TypeCrossShardSender   = "CrossShard-Sender"
	TypeCrossShardReceiver = "CrossShard-Receiver"

	StatusInit           = "Init"
	StatusPrePrepared    = "Pre-Prepared"
	StatusPrepared       = "Prepared"
	StatusCommitted      = "Committed"
	Status2PCPending     = "2PC-Pending"
	Status2PCPrePrepared = "2PC-Pre-Prepared"
	Status2PCPrepared    = "2PC-Prepared"
	Status2PCCommitted   = "2PC-Committed"
	StatusAborted        = "Aborted"
	StatusFailed         = "Failed"
	StatusExecuted       = "Executed"

	OutcomeCommit = "Commit"
	OutcomeAbort  = "Abort"

	EmptyString = ""
)

const (
	MessageTypePrePrepare = "Pre-Prepare"
	MessageTypePrepare    = "Prepare"
	MessageTypeCommit     = "Commit"

	MessageTypeTwoPCPrePrepare = "TwoPC-Pre-Prepare"
	MessageTypeTwoPCPrepare    = "TwoPC-Prepare"
	MessageTypeTwoPCCommit     = "TwoPC-Commit"

	MessageTypeTwoPCPrepareFromCoordinator = "TwoPC-Prepare-Coordinator"
	MessageTypeTwoPCPrepareFromParticipant = "TwoPC-Prepare-Participant"
	MessageTypeTwoPCCommitFromCoordinator  = "TwoPC-Commit-Coordinator"
	MessageTypeTwoPCCommitFromParticipant  = "TwoPC-Commit-Participant"
)

func SignMessage(privateKey *rsa.PrivateKey, message []byte) ([]byte, error) {
	hash := sha256.Sum256(message)
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hash[:])
	if err != nil {
		return nil, err
	}
	return signature, nil
}

func VerifySignature(publicKey *rsa.PublicKey, message, signature []byte) error {
	hash := sha256.Sum256(message)
	err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, hash[:], signature)
	if err != nil {
		return fmt.Errorf("signature verification failed: %v", err)
	}
	return nil
}

func GetTxnType(conf *config.Config, req *common.TxnRequest) string {
	senderCluster := math.Ceil(float64(req.Sender) / float64(conf.DataItemsPerShard))
	receiverCluster := math.Ceil(float64(req.Receiver) / float64(conf.DataItemsPerShard))

	if conf.ClusterNumber == int32(senderCluster) && conf.ClusterNumber == int32(receiverCluster) {
		return TypeIntraShard
	} else if conf.ClusterNumber == int32(senderCluster) {
		return TypeCrossShardSender
	} else if conf.ClusterNumber == int32(receiverCluster) {
		return TypeCrossShardReceiver
	}
	fmt.Printf("error: invalid txn type\n")
	return ""
}

func ValidateBalance(conf *config.Config, req *common.TxnRequest) error {
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardSender {
		balance, err := datastore.GetBalance(conf.DataStore, req.Sender)
		if err != nil {
			return err
		}
		if balance < req.Amount {
			return errors.New("insufficient balance")
		}
	}
	return nil
}

func GetTxnDigest(req *common.TxnRequest) string {
	txn := &datastore.Txn{
		Sender:   req.Sender,
		Receiver: req.Receiver,
		Amount:   req.Amount,
	}
	requestBytes, _ := json.Marshal(txn)

	digest := sha256.Sum256(requestBytes)
	dHex := fmt.Sprintf("%x", digest[:])

	return dHex
}

func HandlePBFTResponse(conf *config.Config, resp *common.PBFTRequestResponse, messageType string) {
	if resp == nil {
		fmt.Printf("HandlePBFTResponse error: resp nil\n")
		return
	}

	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(resp.TxnRequest, txnReq)
	if err != nil {
		fmt.Printf("HandlePBFTResponse: error %v\n", err)
		return
	}

	fmt.Printf("received %s response from server %d for txn request: %v\n", messageType, resp.ServerNo, txnReq.TxnID)

	pbftMessage := &common.PBFTMessage{
		TxnID:       txnReq.TxnID,
		MessageType: messageType,
		Sender:      resp.ServerNo,
		Sign:        base64.StdEncoding.EncodeToString(resp.Sign),
		Payload:     base64.StdEncoding.EncodeToString(resp.SignedMessage),
		CreatedAt:   timestamppb.New(time.Now()),
	}

	err = datastore.InsertPBFTMessage(conf.DataStore, pbftMessage)
	if err != nil {
		fmt.Printf("HandlePBFTResponse: error %v\n", err)
	}
}

func AcquireLockWithAbort(conf *config.Config, req *common.TxnRequest) error {
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardSender {
		if !conf.UserLocks[req.Sender%conf.DataItemsPerShard].TryLock() {
			return errors.New("lock not available for sender")
		}
	}
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardReceiver {
		if !conf.UserLocks[req.Receiver%conf.DataItemsPerShard].TryLock() {
			return errors.New("lock not available for receiver")
		}
	}
	return nil
}

func AcquireLock(conf *config.Config, req *common.TxnRequest) {
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardSender {
		conf.UserLocks[req.Sender%conf.DataItemsPerShard].Lock()
		fmt.Printf("acquired lock for sender %d\n", req.Sender)
	}

	if req.Type == TypeIntraShard || req.Type == TypeCrossShardReceiver {
		conf.UserLocks[req.Receiver%conf.DataItemsPerShard].Lock()
		fmt.Printf("acquired lock for receiver %d\n", req.Receiver)
	}
}

func ReleaseLock(conf *config.Config, req *common.TxnRequest) {
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardSender {
		conf.UserLocks[req.Sender%conf.DataItemsPerShard].Unlock()
		fmt.Printf("released lock for sender %d\n", req.Sender)
	}

	if req.Type == TypeIntraShard || req.Type == TypeCrossShardReceiver {
		conf.UserLocks[req.Receiver%conf.DataItemsPerShard].Unlock()
		fmt.Printf("released lock for receiver %d\n", req.Receiver)
	}
}

func UpdateTxnFailed(conf *config.Config, req *common.TxnRequest, err error) {
	req.Status = StatusFailed
	req.Error = err.Error()
	err = datastore.UpdateTransactionStatus(conf.DataStore, req)
	if err != nil {
		fmt.Println("Update transaction error:", err)
	}
}

func InsertFailedTxn(conf *config.Config, req *common.TxnRequest, err error) {
	req.Status = StatusFailed
	req.Error = err.Error()
	err = datastore.InsertTransaction(conf.DataStore, req)
	if err != nil {
		fmt.Println("Update transaction error:", err)
	}
}

func SendReplyToClient(conf *config.Config, txn *common.TxnRequest) {
	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, txn.TxnID)
	if err != nil {
		fmt.Println("SendReplyToClient error:", err)
	}

	response := &common.ProcessTxnResponse{
		Txn:    dbTxn,
		Status: dbTxn.Status,
		Error:  dbTxn.Error,
	}
	fmt.Printf("sending reply back to client txn with resp: %v\n", response)

	client, err := conf.Pool.GetServer(GetClientAddress())
	if err != nil {
		fmt.Println(err)
	}
	_, err = client.Callback(context.Background(), response)
	if err != nil {
		fmt.Println("SendReplyToClient error:", err)
	}
	return
}

func GetClientAddress() string {
	return "localhost:8000"
}

func GetLeaderNumber(conf *config.Config, clusterNumber int32) int32 {
	servers := conf.MapClusterToServers[clusterNumber]
	leaderIndex := (conf.PBFT.GetViewNumber() - 1) % int32(len(servers))
	return servers[leaderIndex]
}

func GetTxnUpdatedStatusLeader(txn *common.TxnRequest, messageType string) {
	switch messageType {
	case MessageTypePrePrepare:
		if txn.Status == Status2PCPending {
			txn.Status = Status2PCPrePrepared
		} else {
			txn.Status = StatusPrePrepared
		}
	case MessageTypePrepare:
		if txn.Status == Status2PCPrePrepared {
			txn.Status = Status2PCPrepared
		} else {
			txn.Status = StatusPrepared
		}
	case MessageTypeCommit:
		if txn.Status == Status2PCPrepared {
			txn.Status = Status2PCCommitted
		} else {
			txn.Status = StatusCommitted
		}
	}
}

func GetTxnUpdatedStatusFollower(txn *common.TxnRequest, messageType string) {
	switch messageType {
	case MessageTypePrePrepare:
		if txn.Status == Status2PCPending {
			txn.Status = Status2PCPrepared
		} else {
			txn.Status = StatusPrepared
		}
	case MessageTypePrepare:
		if txn.Status == Status2PCPrepared {
			txn.Status = Status2PCCommitted
		} else {
			txn.Status = StatusCommitted
		}
	}
}

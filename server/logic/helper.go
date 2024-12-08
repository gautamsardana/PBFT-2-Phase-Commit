package logic

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
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

	StatusInit              = "Init"
	StatusPrepared          = "Prepared"
	StatusCommitted         = "Committed"
	StatusSuccess           = "Success"
	StatusPreparedForCommit = "PreparedForCommit"
	StatusRolledBack        = "RolledBack"
	StatusFailed            = "Failed"
)

const (
	MessageTypePrePrepare = "PrePrepare"
	MessageTypePrepare    = "Prepare"
)

var MapServerNumberToAddress = map[int32]string{
	1:  "localhost:8081",
	2:  "localhost:8082",
	3:  "localhost:8083",
	4:  "localhost:8084",
	5:  "localhost:8085",
	6:  "localhost:8086",
	7:  "localhost:8087",
	8:  "localhost:8088",
	9:  "localhost:8089",
	10: "localhost:8090",
	11: "localhost:8091",
	12: "localhost:8092",
}

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

func GetTxnDigest(conf *config.Config, req *common.TxnRequest) string {
	txn := &Txn{
		Sender:   req.Sender,
		Receiver: req.Receiver,
		Amount:   req.Amount,
	}
	requestBytes, _ := json.Marshal(txn)

	digest := sha256.Sum256(requestBytes)
	dHex := fmt.Sprintf("%x", digest[:])

	return dHex
}

func AcquireLockWithAbort(conf *config.Config, req *common.TxnRequest) error {
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardSender {
		if !conf.UserLocks[req.Sender%conf.DataItemsPerShard].TryLock() {
			return errors.New("lock not available for sender")
		}
	}
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardReceiver {
		if !conf.UserLocks[req.Receiver%conf.DataItemsPerShard].TryLock() {
			return errors.New("lock not available receiver")
		}
	}
	return nil
}

func HandlePBFTResponse(conf *config.Config, resp *common.PBFTRequestResponse, messageType string) {
	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(resp.TxnRequest, txnReq)
	if err != nil {
		fmt.Printf("HandlePrePrepareResponse: error %v\n", err)
		return
	}

	pbftMessage := &common.PBFTMessage{
		TxnID:       txnReq.TxnID,
		MessageType: messageType,
		Sender:      resp.ServerNo,
		Sign:        string(resp.Sign),
		Payload:     string(resp.SignedMessage),
		CreatedAt:   timestamppb.New(time.Now()),
	}

	err = datastore.InsertPBFTMessage(conf.DataStore, pbftMessage)
	if err != nil {
		fmt.Printf("HandlePrePrepareResponse: error %v\n", err)
	}
}

func AcquireLock(conf *config.Config, req *common.TxnRequest) error {
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardSender {
		conf.UserLocks[req.Sender%conf.DataItemsPerShard].Lock()
	}
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardReceiver {
		conf.UserLocks[req.Receiver%conf.DataItemsPerShard].Lock()
	}
	return nil
}

func ReleaseLock(conf *config.Config, req *common.TxnRequest) {
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardSender {
		conf.UserLocks[req.Sender%conf.DataItemsPerShard].Unlock()
	}
	if req.Type == TypeIntraShard || req.Type == TypeCrossShardReceiver {
		conf.UserLocks[req.Receiver%conf.DataItemsPerShard].Unlock()
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

func FailureResponse(txn *common.TxnRequest, err error) *common.ProcessTxnResponse {
	return &common.ProcessTxnResponse{
		Txn:    txn,
		Status: StatusFailed,
		Error:  err.Error(),
	}
}

func RolledBackResponse(txn *common.TxnRequest) *common.ProcessTxnResponse {
	return &common.ProcessTxnResponse{
		Txn:    txn,
		Status: StatusRolledBack,
	}
}

func SuccessResponse(txn *common.TxnRequest) *common.ProcessTxnResponse {
	return &common.ProcessTxnResponse{
		Txn:    txn,
		Status: StatusSuccess,
	}
}

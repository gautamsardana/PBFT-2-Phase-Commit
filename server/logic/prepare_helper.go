package logic

import (
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
	"context"
	"fmt"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"encoding/json"
	"errors"
)

func SendPrepareResponse(conf *config.Config, req *common.PBFTRequestResponse, txnRequest *common.TxnRequest) (*common.PBFTRequestResponse, error) {
	fmt.Printf("Sending prepare response for txn: %v\n", txnRequest)

	signedMessage := &common.SignedMessage{
		ViewNumber:     txnRequest.ViewNo,
		SequenceNumber: txnRequest.SeqNo,
		Digest:         txnRequest.Digest,
	}
	signedMsgBytes, err := json.Marshal(signedMessage)
	if err != nil {
		return nil, err
	}

	sign, err := SignMessage(conf.PrivateKey, signedMsgBytes)
	if err != nil {
		return nil, err
	}

	prepareReq := &common.PBFTRequestResponse{
		SignedMessage: req.SignedMessage,
		Sign:          sign,
		ServerNo:      conf.ServerNumber,
		TxnRequest:    req.TxnRequest,
	}

	return prepareReq, nil
}

func VerifyPrepare(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse, txnReq *common.TxnRequest) error {
	serverAddr := MapServerNumberToAddress[req.ServerNo]
	publicKey, err := conf.PublicKeys.GetPublicKey(serverAddr)
	if err != nil {
		return err
	}

	err = VerifySignature(publicKey, req.SignedMessage, req.Sign)
	if err != nil {
		return err
	}

	cert := &common.Certificate{}
	err = json.Unmarshal(req.SignedMessage, cert)
	if err != nil {
		return err
	}

	validPrepareCount := int32(0)
	for _, prepareRequest := range cert.Messages {
		verifyReq := &common.PBFTRequestResponse{
			SignedMessage: []byte(prepareRequest.Payload),
			Sign:          []byte(prepareRequest.Sign),
			ServerNo:      prepareRequest.Sender,
		}

		err = VerifyPrePrepare(ctx, conf, verifyReq, txnReq)
		if err != nil {
			fmt.Println(err)
			continue
		}
		validPrepareCount++
	}

	if validPrepareCount < conf.Majority {
		return errors.New("not enough valid prepares")
	}

	return nil
}

func AddPrepareMessages(conf *config.Config, req *common.PBFTRequestResponse) error {
	cert := &common.Certificate{}
	err := json.Unmarshal(req.SignedMessage, cert)
	if err != nil {
		return err
	}

	for _, prepareRequest := range cert.Messages {
		err = datastore.InsertPBFTMessage(conf.DataStore, prepareRequest)
		if err != nil {
			return err
		}
	}
	return nil
}

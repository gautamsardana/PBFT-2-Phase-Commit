package logic

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
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
	serverAddr := config.MapServerNumberToAddress[req.ServerNo]
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

	validPrePrepareCount := int32(0)
	for _, prePrepareMessage := range cert.Messages {
		payload, _ := base64.StdEncoding.DecodeString(prePrepareMessage.Payload)
		sign, _ := base64.StdEncoding.DecodeString(prePrepareMessage.Sign)

		verifyReq := &common.PBFTRequestResponse{
			SignedMessage: payload,
			Sign:          sign,
			ServerNo:      prePrepareMessage.Sender,
		}

		err = VerifyPBFTMessage(ctx, conf, verifyReq, txnReq, MessageTypePrepare)
		if err != nil {
			fmt.Println(err)
			continue
		}
		validPrePrepareCount++
	}

	if validPrePrepareCount < conf.Majority {
		return errors.New("not enough valid pre-prepares")
	}

	return nil
}

func AddPrePrepareMessages(conf *config.Config, req *common.PBFTRequestResponse) error {
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

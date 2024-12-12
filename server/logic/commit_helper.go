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

func VerifyCommit(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse, txnReq *common.TxnRequest) error {
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

	validPrepareCount := int32(0)
	for _, prepareMessage := range cert.Messages {
		payload, _ := base64.StdEncoding.DecodeString(prepareMessage.Payload)
		sign, _ := base64.StdEncoding.DecodeString(prepareMessage.Sign)

		verifyReq := &common.PBFTRequestResponse{
			SignedMessage: payload,
			Sign:          sign,
			ServerNo:      prepareMessage.Sender,
		}

		err = VerifyPBFTMessage(ctx, conf, verifyReq, txnReq, MessageTypeCommit)
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

func AddCommitMessages(conf *config.Config, req *common.PBFTRequestResponse) error {
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

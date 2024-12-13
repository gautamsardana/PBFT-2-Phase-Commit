package logic

import (
	"context"
	"encoding/json"
	"errors"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
)

func VerifyPBFTMessage(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse, txnReq *common.TxnRequest, messageType string) error {
	serverAddr := config.MapServerNumberToAddress[req.ServerNo]
	publicKey, err := conf.PublicKeys.GetPublicKey(serverAddr)
	if err != nil {
		return err
	}
	err = VerifySignature(publicKey, req.SignedMessage, req.Sign)
	if err != nil {
		return err
	}

	signedMessage := &common.SignedMessage{}
	err = json.Unmarshal(req.SignedMessage, signedMessage)
	if err != nil {
		return err
	}
	if signedMessage.ViewNumber != conf.PBFT.GetViewNumber() {
		return errors.New("invalid view number")
	}

	//if signedMessage.SequenceNumber <= conf.LowWatermark || signedMessage.SequenceNumber > conf.HighWatermark {
	//	return errors.New("invalid sequence")
	//}

	digest := GetTxnDigest(txnReq)
	if digest != signedMessage.Digest {
		return errors.New("invalid digest")
	}

	if messageType == MessageTypePrePrepare && signedMessage.SequenceNumber > conf.PBFT.GetSequenceNumber() {
		conf.PBFT.SetSequenceNumber(signedMessage.SequenceNumber)
	} else {
		if signedMessage.SequenceNumber != txnReq.SeqNo {
			return errors.New("invalid sequence number")
		}
	}

	return nil
}

func SendPrePrepareResponse(conf *config.Config, req *common.PBFTRequestResponse) (*common.PBFTRequestResponse, error) {
	sign, err := SignMessage(conf.PrivateKey, req.SignedMessage)
	if err != nil {
		return nil, err
	}

	prepareReq := &common.PBFTRequestResponse{
		SignedMessage: req.SignedMessage,
		Sign:          sign,
		ServerNo:      conf.ServerNumber,
		TxnRequest:    req.TxnRequest,
		Outcome:       req.Outcome,
	}
	return prepareReq, nil
}

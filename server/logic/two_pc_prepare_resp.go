package logic

import (
	"context"
	"encoding/json"
	"fmt"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
)

func ReceiveTwoPCResponse(ctx context.Context, conf *config.Config, req *common.PBFTRequestResponse) error {
	txnReq := &common.TxnRequest{}
	err := json.Unmarshal(req.TxnRequest, txnReq)
	if err != nil {
		return err
	}

	fmt.Printf("received response from participant cluster for txn: %s\n", txnReq.TxnID)

	err = AddTwoPCMessages(conf, req, MessageTypeTwoPCPrepareFromParticipant)
	if err != nil {
		return err
	}

	//todo: another consensus
	if txnReq.Status == StatusPreparedToExecute {
		//send commit
		//commit
		//respond to client
		//release lock
	} else if txnReq.Status == StatusFailed {
		// send abort - participant changes from failed to aborted & releases lock
		// abort and rollback
		//respond to client
		// release lock
	}
	return nil
}

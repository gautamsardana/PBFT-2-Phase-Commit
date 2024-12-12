package logic

import (
	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
	"fmt"
)

func WorkerProcess(conf *config.Config) {
	for {
		<-conf.ExecuteSignal
		ProcessReadyTransactions(conf)
	}
}

func ProcessReadyTransactions(conf *config.Config) {
	for {
		currentSeqNum := conf.PBFT.GetNextSequenceNumber()

		conf.PendingTransactionsMutex.Lock()
		txnRequest, exists := conf.PendingTransactions[currentSeqNum]
		conf.PendingTransactionsMutex.Unlock()

		if !exists {
			break
		}

		err := ExecuteTxn(conf, txnRequest, false)
		if err != nil {
			fmt.Println(err)
		}

		conf.PBFT.IncrementNextSequenceNumber()

		conf.PendingTransactionsMutex.Lock()
		delete(conf.PendingTransactions, currentSeqNum)
		conf.PendingTransactionsMutex.Unlock()

		if txnRequest.Type == TypeIntraShard {
			conf.PBFT.IncrementLastExecutedSequenceNumber()
			ReleaseLock(conf, txnRequest)
			go SendReplyToClient(conf, txnRequest)
		} else if txnRequest.Type == TypeCrossShardSender {
			err = StartTwoPC(conf, txnRequest)
			if err != nil {
				_ = RollbackTxn(conf, txnRequest)
				fmt.Println(err)
			}
		} else {
			// send response back to coordinator cluster
		}
	}
}

func ExecuteTxn(conf *config.Config, txnReq *common.TxnRequest, isSync bool) error {
	fmt.Printf("executing txn for request: %v\n", txnReq)

	if txnReq.Type == TypeIntraShard || txnReq.Type == TypeCrossShardSender {
		senderBalance, err := datastore.GetBalance(conf.DataStore, txnReq.Sender)
		if err != nil {
			return err
		}
		updatedSenderBalance := senderBalance - txnReq.Amount
		err = datastore.UpdateBalance(conf.DataStore, datastore.User{User: txnReq.Sender, Balance: updatedSenderBalance})
		if err != nil {
			return err
		}
	}

	if txnReq.Type == TypeIntraShard || txnReq.Type == TypeCrossShardReceiver {
		receiverBalance, err := datastore.GetBalance(conf.DataStore, txnReq.Receiver)
		if err != nil {
			return err
		}
		updatedReceiverBalance := receiverBalance + txnReq.Amount

		err = datastore.UpdateBalance(conf.DataStore, datastore.User{User: txnReq.Receiver, Balance: updatedReceiverBalance})
		if err != nil {
			return err
		}
	}

	dbTxn, err := datastore.GetTransactionByTxnID(conf.DataStore, txnReq.TxnID)
	if err != nil {
		return err
	}

	if txnReq.Type == TypeIntraShard {
		dbTxn.Status = StatusExecuted
	} else {
		if isSync {
			dbTxn.Status = StatusExecuted
		} else {
			dbTxn.Status = StatusPreparedToExecute
		}
	}

	err = datastore.UpdateTransactionStatus(conf.DataStore, dbTxn)
	if err != nil {
		return err
	}

	return nil
}

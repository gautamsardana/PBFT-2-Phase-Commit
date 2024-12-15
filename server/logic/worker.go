package logic

import (
	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/storage/datastore"
	"context"
	"fmt"
	"time"
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
		} else if txnRequest.Type == TypeCrossShardSender &&
			GetLeaderNumber(conf, conf.ClusterNumber) == conf.ServerNumber {
			err = StartTwoPC(conf, txnRequest)
			if err != nil {
				_ = RollbackTxn(conf, txnRequest)
				fmt.Println(err)
			}
		} else if txnRequest.Type == TypeCrossShardReceiver &&
			GetLeaderNumber(conf, conf.ClusterNumber) == conf.ServerNumber {
			err = SendTwoPCPrepareResponse(conf, txnRequest)
			if err != nil {
				fmt.Println(err)
			}
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
			dbTxn.Status = Status2PCPending
		}
	}

	err = datastore.UpdateTransactionStatus(conf.DataStore, dbTxn)
	if err != nil {
		return err
	}

	return nil
}

func RetryCron(conf *config.Config) {
	ticker := time.NewTicker(7 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				if GetLeaderNumber(conf, conf.ClusterNumber) != conf.ServerNumber {
					return
				}
				RetryPendingTransactions(conf)
			}
		}
	}()
}

func RetryPendingTransactions(conf *config.Config) {
	pendingTxns, err := datastore.GetPendingTransactions(conf.DataStore)
	if err != nil {
		return
	}

	for _, txn := range pendingTxns {
		if txn.Type == TypeCrossShardReceiver {
			continue
		}
		messagesDeleted, err := datastore.DeletePBFTMessagesByByTxnID(conf.DataStore, txn.TxnID) // not a good way of doing this, ideally have a retry count
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("retrying pending transaction %v\n", txn)
		fmt.Printf("messages deleted: %d\n", messagesDeleted)

		err = ProcessTxn(context.Background(), conf, txn, true)
		if err != nil {
			fmt.Println(err)
		}
	}
}

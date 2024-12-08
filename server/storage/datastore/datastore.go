package datastore

import (
	"GolandProjects/2pcbyz-gautamsardana/server/logic"
	"database/sql"
	"errors"
	"time"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
)

var ErrNoRowsUpdated = errors.New("no rows updated for user")

func GetBalance(db *sql.DB, user int32) (float32, error) {
	var balance float32
	query := `SELECT balance FROM user WHERE user = ?`
	err := db.QueryRow(query, user).Scan(&balance)
	if err != nil {
		return 0, err
	}
	return balance, nil
}

func UpdateBalance(tx *sql.DB, user logic.User) error {
	query := `UPDATE user SET balance = ? WHERE user = ?`
	res, err := tx.Exec(query, user.Balance, user.User)
	if err != nil {
		return err
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return ErrNoRowsUpdated
	}
	return nil
}

func GetTransactionByTxnID(db *sql.DB, txnID string) (*common.TxnRequest, error) {
	transaction := &common.TxnRequest{}
	query := `SELECT txn_id, sender, receiver, amount, seq_no, view_no, type, status FROM transaction WHERE txn_id = ?`
	err := db.QueryRow(query, txnID).Scan(
		&transaction.TxnID,
		&transaction.Sender,
		&transaction.Receiver,
		&transaction.Amount,
		&transaction.SeqNo,
		&transaction.ViewNo,
		&transaction.Type,
		&transaction.Status,
	)
	if err != nil {
		return nil, err
	}
	return transaction, nil
}

func InsertTransaction(db *sql.DB, transaction *common.TxnRequest) error {
	query := `INSERT INTO transaction (txn_id, sender, receiver, amount, seq_no, view_no, type, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := db.Exec(query, transaction.TxnID, transaction.Sender, transaction.Receiver,
		transaction.Amount, transaction.SeqNo, transaction.ViewNo, transaction.Type, transaction.Status, time.Now())
	if err != nil {
		return err
	}
	return nil
}

func UpdateTransactionStatus(tx *sql.DB, transaction *common.TxnRequest) error {
	query := `UPDATE transaction SET status = ?, error = ? WHERE txn_id = ?`
	_, err := tx.Exec(query, transaction.Status, transaction.Error, transaction.TxnID)
	if err != nil {
		return err
	}
	return nil
}

func GetTransactionsAfterTerm(db *sql.DB, term int32) ([]*common.TxnRequest, error) {
	var transactions []*common.TxnRequest

	query := `SELECT txn_id, sender, receiver, amount, term, type, status FROM transaction WHERE term > ? AND status = 'committed' ORDER BY term`
	rows, err := db.Query(query, term)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var txn common.TxnRequest
		if err = rows.Scan(&txn.TxnID, &txn.Sender, &txn.Receiver, &txn.Amount, &txn.Term, &txn.Type, &txn.Status); err != nil {
			return nil, err
		}
		transactions = append(transactions, &txn)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return transactions, nil
}

func GetCommittedTxns(db *sql.DB) ([]*common.TxnRequest, error) {
	var transactions []*common.TxnRequest

	query := `SELECT txn_id, sender, receiver, amount, term, type, status FROM transaction WHERE status = 'committed' ORDER BY term`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var txn common.TxnRequest
		if err = rows.Scan(&txn.TxnID, &txn.Sender, &txn.Receiver, &txn.Amount, &txn.Term, &txn.Type, &txn.Status); err != nil {
			return nil, err
		}
		transactions = append(transactions, &txn)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return transactions, nil
}

func InsertPBFTMessage(db *sql.DB, pbftMessage *common.PBFTMessage) error {
	query := `INSERT INTO PBFT_Messages (txn_id, message_type, sender, sign, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)`
	_, err := db.Exec(query, pbftMessage.TxnID, pbftMessage.MessageType, pbftMessage.Sender,
		pbftMessage.Sign, pbftMessage.Payload, pbftMessage.CreatedAt.AsTime())
	if err != nil {
		return err
	}
	return nil
}

func GetPrepareMessages(db *sql.DB, txnID, messagesType string) ([]*common.PBFTMessage, error) {
	query := `SELECT txn_id, message_type, sender, sign, payload, created_at from PBFT_Messages where txn_id = ? and message_type = ?`
	rows, err := db.Query(query, txnID, messagesType)
	if err != nil {
		return nil, err
	}

	var messages []*common.PBFTMessage
	for rows.Next() {
		var message common.PBFTMessage
		if err = rows.Scan(&message.TxnID, &message.MessageType, &message.Sender, &message.Sign, &message.Payload, &message.CreatedAt); err != nil {
			return nil, err
		}
		messages = append(messages, &message)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}

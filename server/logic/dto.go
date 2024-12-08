package logic

type User struct {
	User    int32
	Balance float32
}

type Txn struct {
	Sender   int32
	Receiver int32
	Amount   float32
}

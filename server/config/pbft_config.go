package config

import "sync"

type PBFTConfig struct {
	Lock               sync.Mutex
	ViewNumber         int32
	SequenceNumber     int32
	NextSequenceNumber int32
	LastExecutedSeq    int32
}

func (c *PBFTConfig) GetSequenceNumber() int32 {
	return c.SequenceNumber
}

func (c *PBFTConfig) GetViewNumber() int32 {
	return c.ViewNumber
}

func (c *PBFTConfig) GetLastExecutedSequenceNumber() int32 {
	return c.LastExecutedSeq
}

func (c *PBFTConfig) IncrementSequenceNumber() int32 {
	c.Lock.Lock()
	c.SequenceNumber++
	c.Lock.Unlock()
	return c.SequenceNumber
}

func (c *PBFTConfig) IncrementViewNumber() int32 {
	c.Lock.Lock()
	c.ViewNumber++
	c.Lock.Unlock()
	return c.ViewNumber
}

func (c *PBFTConfig) SetSequenceNumber(updatedSequenceNumber int32) {
	c.Lock.Lock()
	c.SequenceNumber = updatedSequenceNumber
	c.Lock.Unlock()
}

func (c *PBFTConfig) IncrementLastExecutedSequenceNumber() {
	c.Lock.Lock()
	c.LastExecutedSeq++
	c.Lock.Unlock()
}

func (c *PBFTConfig) GetNextSequenceNumber() int32 {
	return c.NextSequenceNumber
}

func (c *PBFTConfig) IncrementNextSequenceNumber() {
	c.Lock.Lock()
	c.NextSequenceNumber++
	c.Lock.Unlock()
}

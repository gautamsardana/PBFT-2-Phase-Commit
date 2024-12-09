package logic

import (
	"GolandProjects/2pcbyz-gautamsardana/client/config"
)

const (
	EmptyString            = ""
	StatusSuccess          = "Success"
	StatusFailed           = "Failed"
	TypeIntraShard         = "IntraShard"
	TypeCrossShardSender   = "CrossShard-Sender"
	TypeCrossShardReceiver = "CrossShard-Receiver"
)

var mapServerToServerNo = map[string]int32{
	"S1":  1,
	"S2":  2,
	"S3":  3,
	"S4":  4,
	"S5":  5,
	"S6":  6,
	"S7":  7,
	"S8":  8,
	"S9":  9,
	"S10": 10,
	"S11": 11,
	"S12": 12,
}

var mapServerNoToServerAddr = map[int32]string{
	1:  "localhost:8081",
	2:  "localhost:8082",
	3:  "localhost:8083",
	4:  "localhost:8084",
	5:  "localhost:8085",
	6:  "localhost:8086",
	7:  "localhost:8087",
	8:  "localhost:8088",
	9:  "localhost:8089",
	10: "localhost:8090",
	11: "localhost:8091",
	12: "localhost:8092",
}

func GetContactServerForCluster(conf *config.Config, cluster int32, contactServers []string) string {
	for _, serverNo := range conf.MapClusterToServers[cluster] {
		for _, contactServer := range contactServers {
			if mapServerToServerNo[contactServer] == serverNo {
				return mapServerNoToServerAddr[serverNo]
			}
		}
	}
	return ""
}

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

var mapServerToServerAddr = map[string]string{
	"S1": "localhost:8081",
	"S2": "localhost:8082",
	"S3": "localhost:8083",
	"S4": "localhost:8084",
	"S5": "localhost:8085",
	"S6": "localhost:8086",
	"S7": "localhost:8087",
	"S8": "localhost:8088",
	"S9": "localhost:8089",
}

var mapServerToServerNo = map[string]int32{
	"S1": 1,
	"S2": 2,
	"S3": 3,
	"S4": 4,
	"S5": 5,
	"S6": 6,
	"S7": 7,
	"S8": 8,
	"S9": 9,
}

var mapServerNoToServerAddr = map[int32]string{
	1: "localhost:8081",
	2: "localhost:8082",
	3: "localhost:8083",
	4: "localhost:8084",
	5: "localhost:8085",
	6: "localhost:8086",
	7: "localhost:8087",
	8: "localhost:8088",
	9: "localhost:8089",
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

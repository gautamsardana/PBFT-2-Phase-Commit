package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	serverPool "GolandProjects/2pcbyz-gautamsardana/server_pool"
)

const configPath = "/go/src/GolandProjects/2pcbyz-gautamsardana/client/config/config.json"

type Config struct {
	Port                string   `json:"port"`
	ServerAddresses     []string `json:"server_addresses"`
	Clusters            int32    `json:"clusters"`
	ClusterSize         int32    `json:"cluster_size"`
	TotalUsers          int32    `json:"total_users"`
	DataItemsPerShard   int32
	Pool                *serverPool.ServerPool
	DBDSN               string `json:"db_dsn"`
	MapClusterToServers map[int32][]int32
	ViewNumber          int32 `json:"view_number"`

	Lock         sync.Mutex
	TxnResponses map[string][]*common.ProcessTxnResponse

	TxnQueueLock sync.Mutex
	TxnStartTime map[string]time.Time
	LatencyQueue []time.Duration
}

func GetConfig() *Config {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	jsonConfig, err := os.ReadFile(homeDir + configPath)
	if err != nil {
		log.Fatal(err)
	}
	conf := &Config{}
	if err = json.Unmarshal(jsonConfig, conf); err != nil {
		log.Fatal(err)
	}
	return conf
}

func InitiateServerPool(conf *Config) {
	pool, err := serverPool.NewServerPool(conf.ServerAddresses)
	if err != nil {
		log.Fatal(err)
	}
	conf.Pool = pool
}

func InitiateConfig(conf *Config) {
	conf.TxnResponses = make(map[string][]*common.ProcessTxnResponse)
	conf.TxnStartTime = make(map[string]time.Time)
	conf.LatencyQueue = make([]time.Duration, 0)
}

func InitiateClusters(conf *Config) {
	conf.DataItemsPerShard = conf.TotalUsers / conf.Clusters
	conf.MapClusterToServers = make(map[int32][]int32)
	totalClusters := int(conf.Clusters)
	clusterSize := int(conf.ClusterSize)

	for i := 0; i < totalClusters; i++ {
		for j := 0; j < clusterSize; j++ {
			conf.MapClusterToServers[int32(i+1)] = append(conf.MapClusterToServers[int32(i+1)], int32(j+1+(i*clusterSize)))
		}
	}
	fmt.Println(conf.MapClusterToServers)
}

func InitiateDB(conf *Config) {
	for cluster, servers := range conf.MapClusterToServers {
		userStart := (cluster-1)*conf.DataItemsPerShard + 1
		userEnd := cluster * conf.DataItemsPerShard
		for _, server := range servers {
			go func() {
				PopulateDB(conf.DBDSN, server, userStart, userEnd)
			}()

		}
	}
}

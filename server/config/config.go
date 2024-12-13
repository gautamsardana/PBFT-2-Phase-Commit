package config

import (
	"crypto/rsa"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"sync"
	"time"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	KeyPool "GolandProjects/2pcbyz-gautamsardana/key_pool"
	serverPool "GolandProjects/2pcbyz-gautamsardana/server_pool"
)

const configPath = "/Users/gautamsardana/go/src/GolandProjects/2pcbyz-gautamsardana/server/config/config.json"

type Config struct {
	BasePort            int32 `json:"base_port"`
	Port                string
	ServerNumber        int32  `json:"server_number"`
	ServerTotal         int32  `json:"server_total"`
	Majority            int32  `json:"majority"`
	DBDSN               string `json:"db_dsn"`
	DataStore           *sql.DB
	ServerAddresses     []string `json:"server_addresses"`
	Pool                *serverPool.ServerPool
	ClusterNumber       int32
	MapClusterToServers map[int32][]int32
	DataItemsPerShard   int32
	IsAlive             bool
	IsByzantine         bool

	PendingTransactions      map[int32]*common.TxnRequest
	PendingTransactionsMutex sync.Mutex
	ExecuteSignal            chan struct{}

	PublicKeys *KeyPool.KeyPool
	PrivateKey *rsa.PrivateKey

	PBFT *PBFTConfig

	TwoPCLock sync.Mutex
	UserLocks []sync.Mutex
}

func InitiateConfig(conf *Config) {
	InitiateServerPool(conf)
	InitiatePublicKeys(conf)
	InitiatePrivateKey(conf)
	conf.MapClusterToServers = make(map[int32][]int32)
	conf.PBFT = &PBFTConfig{ViewNumber: 1, NextSequenceNumber: 1}
	conf.PendingTransactions = make(map[int32]*common.TxnRequest)
	conf.ExecuteSignal = make(chan struct{}, 1000)
}

func InitiateServerPool(conf *Config) {
	pool, err := serverPool.NewServerPool(conf.ServerAddresses)
	if err != nil {
		fmt.Println(err)
	}
	conf.Pool = pool
}

func InitiatePublicKeys(conf *Config) {
	pool, err := KeyPool.NewPublicKeyPool()
	if err != nil {
		log.Fatal(err)
	}
	conf.PublicKeys = pool
}

func InitiatePrivateKey(conf *Config) {
	pool, err := KeyPool.NewPrivateKeyPool()
	if err != nil {
		fmt.Println(err)
	}

	serverAddr := MapServerNumberToAddress[conf.ServerNumber]

	conf.PrivateKey, err = pool.GetPrivateKey(serverAddr)
	if err != nil {
		log.Fatal(err)
	}
}

func GetConfig() *Config {
	jsonConfig, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatal(err)
	}
	conf := &Config{}
	if err = json.Unmarshal(jsonConfig, conf); err != nil {
		log.Fatal(err)
	}

	serverNumber := flag.Int("server", 1, "Server number")
	flag.Parse()
	conf.ServerNumber = int32(*serverNumber)
	conf.Port = fmt.Sprintf("%d", int(conf.BasePort)+*serverNumber)

	conf.DBDSN = fmt.Sprintf(conf.DBDSN, conf.ServerNumber)
	return conf
}

func SetupDB(config *Config) {
	db, err := sql.Open("mysql", config.DBDSN)
	if err != nil {
		log.Fatal(err)
	}
	config.DataStore = db
	fmt.Println("MySQL Connected!!")
	db.SetMaxOpenConns(1001)
	db.SetMaxIdleConns(50)
	db.SetConnMaxLifetime(5 * time.Minute)
}

var MapServerNumberToAddress = map[int32]string{
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

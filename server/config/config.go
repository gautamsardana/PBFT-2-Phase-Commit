package config

import (
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	publicKeyPool "GolandProjects/2pcbyz-gautamsardana/public_key_pool"
	serverPool "GolandProjects/2pcbyz-gautamsardana/server_pool"
)

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

	PublicKeys       *publicKeyPool.PublicKeyPool
	PrivateKeyString string `json:"private_key"`
	PrivateKey       *rsa.PrivateKey

	PBFT *PBFTConfig

	TwoPCLock sync.Mutex
	UserLocks []sync.Mutex
}

func InitiateConfig(conf *Config) {
	InitiateServerPool(conf)
	InitiatePublicKeys(conf)
	InitiatePrivateKey(conf)
	conf.MapClusterToServers = make(map[int32][]int32)
}

func InitiateServerPool(conf *Config) {
	pool, err := serverPool.NewServerPool(conf.ServerAddresses)
	if err != nil {
		fmt.Println(err)
	}
	conf.Pool = pool
}

func InitiatePublicKeys(conf *Config) {
	pool, err := publicKeyPool.NewPublicKeyPool()
	if err != nil {
		fmt.Println(err)
	}
	conf.PublicKeys = pool
}

func InitiatePrivateKey(conf *Config) {
	pemData, err := base64.StdEncoding.DecodeString(conf.PrivateKeyString)
	if err != nil {
		return
	}

	block, _ := pem.Decode(pemData)
	if block == nil {
		return
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return
	}

	conf.PrivateKey = privateKey
}

func GetConfig() *Config {
	configPath := flag.String("config", "config.json", "Path to the configuration file")
	serverNumber := flag.Int("server", 1, "Server number")
	flag.Parse()
	jsonConfig, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	conf := &Config{}
	if err = json.Unmarshal(jsonConfig, conf); err != nil {
		log.Fatal(err)
	}

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
}

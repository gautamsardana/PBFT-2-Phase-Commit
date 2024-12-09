package key_pool

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"os"
)

const configPath = "/go/src/GolandProjects/pbft-gautamsardana/public_key_pool/config.json"

type Config struct {
	PublicKeys  map[string]string `json:"public_keys"`
	PrivateKeys map[string]string `json:"private_keys"`
}

type KeyPool struct {
	PublicKeys map[string]*rsa.PublicKey
	PrivateKey map[string]*rsa.PrivateKey
}

func NewPublicKeyPool() (*KeyPool, error) {
	conf := GetConfig()
	publicKeyPool := &KeyPool{
		PublicKeys: make(map[string]*rsa.PublicKey),
	}

	for addr, pubKeyStr := range conf.PublicKeys {
		pubKeyBytes, err := base64.StdEncoding.DecodeString(pubKeyStr)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64 public key for %s: %v", addr, err)
		}

		block, _ := pem.Decode(pubKeyBytes)
		if block == nil {
			return nil, fmt.Errorf("failed to parse PEM block containing the public key for %s", addr)
		}

		pubInterface, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse public key for %s: %v", addr, err)
		}

		pubKey, ok := pubInterface.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("not an RSA public key for %s", addr)
		}

		publicKeyPool.PublicKeys[addr] = pubKey
	}

	return publicKeyPool, nil
}

func (pkp *KeyPool) GetPublicKey(addr string) (*rsa.PublicKey, error) {
	pubKey, exists := pkp.PublicKeys[addr]
	if !exists {
		return nil, fmt.Errorf("public key not found for address %s", addr)
	}
	return pubKey, nil
}

func NewPrivateKeyPool() (*KeyPool, error) {
	conf := GetConfig()
	privateKeyPool := &KeyPool{
		PrivateKey: make(map[string]*rsa.PrivateKey),
	}

	for addr, privateKeyStr := range conf.PrivateKeys {
		privateKeyBytes, err := base64.StdEncoding.DecodeString(privateKeyStr)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64 private key for %s: %v", addr, err)
		}

		block, _ := pem.Decode(privateKeyBytes)
		if block == nil {
			return nil, fmt.Errorf("failed to parse PEM block containing the private key for %s", addr)
		}

		privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key for %s: %v", addr, err)
		}

		privateKeyPool.PrivateKey[addr] = privateKey
	}

	return privateKeyPool, nil
}

func (pkp *KeyPool) GetPrivateKey(addr string) (*rsa.PrivateKey, error) {
	privateKey, exists := pkp.PrivateKey[addr]
	if !exists {
		return nil, fmt.Errorf("private key not found for address %s", addr)
	}
	return privateKey, nil
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

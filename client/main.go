package main

import (
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/client/api"
	"GolandProjects/2pcbyz-gautamsardana/client/config"
)

func main() {
	conf := config.GetConfig()
	config.InitiateServerPool(conf)
	config.InitiateConfig(conf)
	config.InitiateClusters(conf)
	config.InitiateDB(conf)
	ListenAndServe(conf)
}

func ListenAndServe(conf *config.Config) {
	lis, err := net.Listen("tcp", ":"+conf.Port)
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	common.RegisterByz2PCServer(s, &api.Client{Config: conf})
	fmt.Printf("gRPC server running on port %v...\n", conf.Port)
	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

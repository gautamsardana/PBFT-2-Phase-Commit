package main

import (
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"

	"GolandProjects/2pcbyz-gautamsardana/server/api"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
)

func main() {
	conf := config.GetConfig()

	config.SetupDB(conf)
	config.InitiateConfig(conf)

	ListenAndServe(conf)
}

func ListenAndServe(conf *config.Config) {
	lis, err := net.Listen("tcp", ":"+conf.Port)
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	common.RegisterPaxos2PCServer(s, &api.Server{Config: conf})
	fmt.Printf("gRPC server running on port %v...\n", conf.Port)
	if err = s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

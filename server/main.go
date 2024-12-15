package main

import (
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/api"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"GolandProjects/2pcbyz-gautamsardana/server/logic"
)

func main() {
	conf := config.GetConfig()

	config.SetupDB(conf)
	config.InitiateConfig(conf)

	go logic.WorkerProcess(conf)
	go logic.RetryCron(conf)

	ListenAndServe(conf)
}

func ListenAndServe(conf *config.Config) {
	lis, err := net.Listen("tcp", ":"+conf.Port)
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	common.RegisterByz2PCServer(s, &api.Server{Config: conf})
	fmt.Printf("gRPC server running on port %v...\n", conf.Port)
	if err = s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

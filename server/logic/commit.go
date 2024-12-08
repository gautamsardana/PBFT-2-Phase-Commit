package logic

import (
	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/server/config"
	"fmt"
)

func SendCommit(conf *config.Config, req *common.TxnRequest) error {
	fmt.Printf("Sending Commit for request: %v\n", req)

	return nil
}

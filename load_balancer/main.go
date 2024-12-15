package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"strconv"
	"strings"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
)

const inputFilePath = "Lab4_Testset_2.csv"

var sets map[int32]*common.TxnSet
var totalSets int32

func loadCSV(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ','

	sets = make(map[int32]*common.TxnSet)

	var currentSetNo int32
	var liveServers []string
	var contactServers []string
	var byzantineServers []string

	for {
		setRow, err := reader.Read()
		if err != nil {
			break
		}

		if setRow[0] != "" {
			setNumber, _ := strconv.Atoi(setRow[0])
			currentSetNo = int32(setNumber)
			totalSets = currentSetNo

			liveServers = strings.Split(strings.Trim(setRow[2], "[] "), ",")
			for i := range liveServers {
				liveServers[i] = strings.TrimSpace(liveServers[i])
			}

			contactServers = strings.Split(strings.Trim(setRow[3], "[] "), ",")
			for i := range contactServers {
				contactServers[i] = strings.TrimSpace(contactServers[i])
			}

			byzantineServers = strings.Split(strings.Trim(setRow[4], "[] "), ",")
			for i := range byzantineServers {
				byzantineServers[i] = strings.TrimSpace(byzantineServers[i])
			}

			if _, exists := sets[currentSetNo]; !exists {
				sets[currentSetNo] = &common.TxnSet{
					SetNo:            currentSetNo,
					Txns:             []*common.TxnRequest{},
					LiveServers:      liveServers,
					ContactServers:   contactServers,
					ByzantineServers: byzantineServers,
				}
			}
		}

		txnStrings := strings.Split(setRow[1], ";")
		for _, txn := range txnStrings {
			txn = strings.Trim(txn, "() ")
			parts := strings.Split(txn, ",")
			if len(parts) != 3 {
				continue
			}
			sender, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
			receiver, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
			amount, _ := strconv.ParseFloat(strings.TrimSpace(parts[2]), 32)

			if sets[currentSetNo] == nil {
				sets[currentSetNo] = &common.TxnSet{
					SetNo: currentSetNo,
					Txns:  []*common.TxnRequest{},
				}
			}

			sets[currentSetNo].Txns = append(sets[currentSetNo].Txns, &common.TxnRequest{
				Sender:   int32(sender),
				Receiver: int32(receiver),
				Amount:   float32(amount),
			})
		}
	}

	return nil
}

func main() {
	client := InitiateClient()

	err := loadCSV(inputFilePath)
	if err != nil {
		fmt.Println("Error loading CSV:", err)
		return
	}

	scanner := bufio.NewScanner(os.Stdin)

	var i int32
	for i = 1; i <= totalSets; i++ {
		fmt.Printf("Processing Set %d: Txns: %v LiveServers: %v ContactServers: %v ByzantineServers: %v \n", i, sets[i].Txns, sets[i].LiveServers, sets[i].ContactServers, sets[i].ByzantineServers)

		scanner.Scan()
		ProcessSet(sets[i], client)

		for {
			fmt.Println("\nType 'next' to process the next set, " +
				"'balance' to get balance, " +
				"'db' to print database, " +
				" 'perf' to print performance" +
				" or 'bench' to print benchmark metrics")
			scanner.Scan()
			input := scanner.Text()
			if input == "next" {
				break

			} else if input == "db" {
				fmt.Println("Which server? (eg. '1' without quotes)")
				scanner.Scan()
				serverNoString := scanner.Text()
				serverNo, _ := strconv.Atoi(serverNoString)
				PrintDB(client, int32(serverNo))

			} else if input == "balance" {
				fmt.Println("Which user? (eg. '100' without quotes)")
				scanner.Scan()
				userString := scanner.Text()
				user, _ := strconv.Atoi(userString)
				PrintBalance(client, int32(user))

			} else if input == "perf" {
				Performance(client)
			} else if input == "bench" {
				fmt.Println("How many transactions? (eg. 1000)")
				scanner.Scan()
				txnNumberString := scanner.Text()
				txnNumber, _ := strconv.Atoi(txnNumberString)
				Benchmark(client, txnNumber, sets[i].ContactServers)
			} else {
				fmt.Println("Unknown command")
			}
		}
	}
	fmt.Println("All sets processed.")
}

func InitiateClient() common.Byz2PCClient {
	conn, err := grpc.NewClient("localhost:8000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	client := common.NewByz2PCClient(conn)
	return client
}

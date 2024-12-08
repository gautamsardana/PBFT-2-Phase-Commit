package config

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strings"
)

func PopulateDB(dsn string, server, userStart, userEnd int32) {
	dsn = fmt.Sprintf(dsn, server)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	queries := []string{
		"DELETE FROM transaction",
		"DELETE FROM user",
	}
	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			log.Printf("Error executing query '%s': %v", query, err)
		}
	}

	var placeholders []string
	var values []interface{}
	for i := userStart; i <= userEnd; i++ {
		placeholders = append(placeholders, "(?, ?)")
		values = append(values, i, 10)

		query := fmt.Sprintf("INSERT INTO user (user, balance) VALUES %s", strings.Join(placeholders, ","))
		if _, err := db.Exec(query, values...); err != nil {
			log.Printf("Error inserting users: %v", err)
		}
		placeholders = nil
		values = nil
	}
	fmt.Printf("Done populating database for server %d at DSN: %s\n", server, dsn)
}

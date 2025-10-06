package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

const (
	_port     = "port"
	_host     = "host"
	_user     = "user"
	_dbname   = "dbname"
	_password = "password"
	_sslmode  = "sslmode"
	_env_file = "env_file"
	_sql_file = "sql_file"
)

type config struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
	EnvFile  string
	SqlFile  string
}

type LogEntry struct {
	JsonPayload struct {
		Message string `json:"message"`
	} `json:"jsonPayload"`
}

func ExtractQuery(filePath string) ([]*LogEntry, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var logs []*LogEntry
	err = json.Unmarshal(data, &logs)
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`(?i)\b(SELECT|INSERT|UPDATE|DELETE|WITH RECURSIVE)\b[\s\S]*`)
	for _, l := range logs {
		sql := re.FindString(l.JsonPayload.Message)
		l.JsonPayload.Message = sql
	}
	return logs, nil
}

func Connection(dsn string) (*sql.DB, error) {
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	conn.SetMaxIdleConns(0.1 * 80)
	conn.SetConnMaxLifetime(30 * time.Second)
	conn.SetConnMaxIdleTime(15 * time.Second)
	conn.SetMaxOpenConns(70)
	return conn, nil
}

func QueryConcurrency(logs []*LogEntry, dbConfig *config) (int, error) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=%s", dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.DBName, dbConfig.Password, dbConfig.SSLMode)
	conn, err := Connection(dsn)
	if err != nil {
		return 0, err
	}

	var m sync.Mutex
	var numberSuccess int
	var wg sync.WaitGroup
	errChan := make(chan error, len(logs))
	for _, log := range logs {
		wg.Add(1)
		go func(sqlString string, conn *sql.DB, wg *sync.WaitGroup) {
			timeStart := time.Now()
			defer wg.Done()
			rows, err := conn.Query(sqlString)
			if err != nil {
				fmt.Println("--------------------------------------------------------")
				fmt.Println("Query error: ", sqlString)
				fmt.Println(err.Error())
				fmt.Printf("The total time taken before timeout is %d milisecons.\n", time.Since(timeStart).Milliseconds())
				fmt.Println("--------------------------------------------------------")
				errChan <- err
				return
			}
			m.Lock()
			numberSuccess = numberSuccess + 1
			m.Unlock()
			defer rows.Close()
		}(log.JsonPayload.Message, conn, &wg)
	}
	wg.Wait()
	close(errChan)
	if len(errChan) > 0 {
		return 0, <-errChan
	}
	return numberSuccess, nil
}

func main() {
	var (
		host     = flag.String(_host, "localhost", "database host name")
		port     = flag.String(_port, "5432", "database port")
		user     = flag.String(_user, "", "database user name")
		password = flag.String(_password, "", "database password")
		sslmode  = flag.String(_sslmode, "disable", "database sslmode")
		dbname   = flag.String(_dbname, "", "database name")
		sqlFile  = flag.String(_sql_file, "", "sql query file")
	)

	flag.Parse()

	config := config{
		Host:     *host,
		Port:     *port,
		User:     *user,
		Password: *password,
		DBName:   *dbname,
		SSLMode:  *sslmode,
		SqlFile:  *sqlFile,
	}

	flag.Parse()

	if config.User == "" {
		log.Fatalf("Missing user name")
	}
	if config.Password == "" {
		log.Fatalf("Missing password")
	}
	if config.DBName == "" {
		log.Fatalf("Missing database name")
	}
	if config.SqlFile == "" {
		log.Fatalf("Missing sql file")
	}

	// logs, err := ExtractQuery("/Users/genkidev/Desktop/stagging_sql_query.json")
	logs, err := ExtractQuery(config.SqlFile)
	if err != nil {
		panic(err.Error())
	}
	timeStart := time.Now()
	numberSuccess, err := QueryConcurrency(logs, &config)

	fmt.Println("****************************************")
	fmt.Printf("%d successful requests out of %d requests.\n ", numberSuccess, len(logs))
	fmt.Println("****************************************")
	fmt.Printf("timeconsuming: %d milisecon\n", time.Since(timeStart).Milliseconds())

	if err != nil {
		panic(err.Error())
	}
}

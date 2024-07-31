// Package postgres provides everything to serve functionality that needed from postgres to solve
// given task.
package postgre

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"log"
	"os"
	"time"
)

type DatabaseDriver interface {
	InitDB() error
	AddMessage(message string) (int64, error)
	DropTable()
	GetOverallRows() (int, error)
	GetProcessedRows() (int, error)
	//SetMsgAsProcessed(id int64) error
	//CloseConnection() error
}

var _ DatabaseDriver = (*PostgresDriver)(nil)

type PostgresDriver struct {
	Pool *sql.DB
}

// NewPostgresDriver is the constructor that  pointer to PostgresDriver instance.
func NewPostgresDriver(pool *sql.DB) *PostgresDriver {
	return &PostgresDriver{Pool: pool}
}

// CreateConnection returns *sql.DB connection pool to postgres from given credentials.
func CreateConnection(
	host string,
	port int,
	username string,
	password string,
	database string,
	sslmode string,
) *sql.DB {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s",
		host, port, username, password, database, sslmode)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	db.SetMaxOpenConns(15)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5)

	err = db.Ping()
	if err != nil {
		panic(err)
	} else {
		log.Println("Connected to postgres!")
	}
	return db
}

// InitDB method provides migration to postgres from given sql scripts.
// It creates "clients" and "algorithm_status" tables.
func (d *PostgresDriver) InitDB() error {
	sqlFile, err := os.Open("/usr/local/src/db/sql/MessageTable.sql")
	if err != nil {
		rErr := fmt.Errorf("err while opening file %v", err)
		log.Println(rErr)
		return rErr
	}

	defer sqlFile.Close()

	sqlBytes, err := io.ReadAll(sqlFile)
	if err != nil {
		rErr := fmt.Errorf("err while reading file %v", err)
		log.Println(rErr)
		return rErr
	}

	createTableQuery := string(sqlBytes)

	_, err = d.Pool.Exec(createTableQuery)
	if err != nil {
		rErr := fmt.Errorf("err while creating messages table %v", err)
		log.Println(rErr)
		return rErr
	}

	log.Println("No problems found during initializing db")
	return nil
}

func (d *PostgresDriver) DropTable() {
	query := `
	DROP TABLE IF EXISTS messages;
	`
	d.Pool.Exec(query)
}

// AddMessage metod creates new row into messages table.
func (d *PostgresDriver) AddMessage(message string) (int64, error) {
	var msgId int64
	tx, err := d.Pool.Begin()
	if err != nil {
		log.Printf("err during starting transaction: %v\n", err)
		return 0, err
	}

	query := `
	INSERT INTO messages(content)
	VALUES ($1)
	RETURNING ID;
	`

	rows := tx.QueryRow(query, message)
	err = rows.Err()
	if err != nil {
		tx.Rollback()
		log.Printf("err during executing query: %v\n", err)
		return 0, err
	}

	err = rows.Scan(&msgId)
	if err != nil {
		tx.Rollback()
		log.Printf("err during scanning rows: %v\n", err)
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		log.Printf("cant commit transaction: %v\n", err)
		return 0, err
	}

	return msgId, nil
}

// GetOverallRows return overall amount of rows in messages table.
func (d *PostgresDriver) GetOverallRows() (int, error) {
	var overallRows int

	tx, err := d.Pool.Begin()
	if err != nil {
		log.Printf("err during starting transaction: %v\n", err)
		return 0, err
	}

	query := `
	SELECT COUNT(*) FROM messages;
	`

	rows := tx.QueryRow(query)
	err = rows.Err()
	if err != nil {
		tx.Rollback()
		log.Printf("err during executing query: %v\n", err)
		return 0, err
	}

	err = rows.Scan(&overallRows)
	if err != nil {
		tx.Rollback()
		log.Printf("err during scanning rows: %v\n", err)
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		log.Printf("cant commit transaction: %v\n", err)
		return 0, err
	}

	return overallRows, nil
}

// GetProcessedRows return overall amount of rows where Processed field is true.
func (d *PostgresDriver) GetProcessedRows() (int, error) {
	var overallRows int

	tx, err := d.Pool.Begin()
	if err != nil {
		log.Printf("err during starting transaction: %v\n", err)
		return 0, err
	}

	query := `
	SELECT COUNT(*) FROM messages WHERE processed = true;
	`

	rows := tx.QueryRow(query)
	err = rows.Err()
	if err != nil {
		tx.Rollback()
		log.Printf("err during executing query: %v\n", err)
		return 0, err
	}

	err = rows.Scan(&overallRows)
	if err != nil {
		tx.Rollback()
		log.Printf("err during scanning rows: %v\n", err)
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		log.Printf("cant commit transaction: %v\n", err)
		return 0, err
	}

	return overallRows, nil
}

// SetMsgAsProcessed func sets Processed field to true.
func SetMsgAsProcessed(pool *sql.DB, id int64) error {
	tx, err := pool.Begin()
	if err != nil {
		log.Printf("err during starting transaction: %v\n", err)
		return err
	}

	query := `
	UPDATE messages SET processed = TRUE WHERE id = $1
	`
	log.Println("SetMsgAsProcessed: ID:", id)
	_, err = tx.Exec(query, id)
	if err != nil {
		log.Printf("error while updating processed status: %v", err)
		tx.Rollback()
		return fmt.Errorf("error while updating processed status: %v", err)
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		log.Printf("cant commit transaction: %v\n", err)
		return fmt.Errorf("cant commit transaction: %v\n", err)
	}
	return nil
}

// CloseConnection method closes connection pool.
func CloseConnection(db *sql.DB) error {
	err := db.Close()
	if err != nil {
		return fmt.Errorf("error while closing conection: %v", err)
	}
	log.Println("connection closed")
	return nil
}

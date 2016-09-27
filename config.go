package documentGeom

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	"github.com/IMQS/log"
)

type Server struct {
	Config     Configuration
	Log        *log.Logger
	PostgresDB postgresDB
}

type postgresDB struct {
	db *sql.DB
}

type Configuration struct {
	Logfile    string
	PostgresDB PostgresDBConnection
	MongoDB    mongoDB
}

type PostgresDBConnection struct {
	Driver   string
	Host     string
	Port     uint16
	Database string
	User     string
	Password string
	SSL      bool
}

type mongoDB struct {
	Host string
}

// Initialize opens a log file and connects to the DB.
func (s *Server) Initialize() error {
	var err error

	s.Log = log.New(s.Config.Logfile)
	s.Log.Level = 0
	s.PostgresDB.db, err = s.Config.PostgresDB.open()
	if err != nil {
		s.Log.Errorf("Error connecting to Messaging DB: %v", err)
		return err
	}

	if err = s.PostgresDB.db.Ping(); err != nil {
		s.Log.Infof("Could not connect to the database")
		return err
	}
	return nil
}

// NewConfig reads the config file and populates the config object.
func (c *Configuration) NewConfig(filename string) error {

	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)

	if err = decoder.Decode(&c); err != nil {
		fmt.Println("Error parsing config file:", err)
		return err
	}

	return nil
}

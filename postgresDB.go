package documentGeom

import (
	"database/sql"
	"errors"
	"fmt"

	"strings"

	_ "github.com/lib/pq"
)

// Connect to the DB as defined in the dbConnection configuration.
func (x *PostgresDBConnection) open() (*sql.DB, error) {
	return sql.Open(x.Driver, x.connectionString(true))
}

func (x *PostgresDBConnection) connectionString(addDB bool) string {
	sslmode := "disable"
	if x.SSL {
		sslmode = "require"
	}
	conStr := fmt.Sprintf("host=%v user=%v password=%v sslmode=%v", x.Host, x.User, x.Password, sslmode)
	if addDB {
		conStr += fmt.Sprintf(" dbname=%v", x.Database)
	}
	if x.Port != 0 {
		conStr += fmt.Sprintf(" port=%v", x.Port)
	}
	return conStr
}

func (x *postgresDB) selectGeometry(tableName string, fieldName string, fieldValue string) (geom string, err error) {
	r := strings.NewReplacer("<TABLE>", tableName, "<FIELD>", fieldName, "<VAL>", fieldValue)
	sqlString := r.Replace(`SELECT "Geometry" FROM "<TABLE>" WHERE "<FIELD>" = '<VAL>' LIMIT 1`)
	err = x.db.QueryRow(sqlString).Scan(&geom)
	if err != nil {
		return "", errors.New("selectGeometry: Could not find element for field and value -> " + err.Error())
	}
	return geom, nil
}

func (x *postgresDB) truncateDocumentGeometry() error {
	_, err := x.db.Exec(`TRUNCATE "DocumentGeometry"`)
	return err
}

func (x *postgresDB) createDocumentGeometry(documentGeometry documentGeometry) error {
	_, err := x.db.Exec(`INSERT INTO "DocumentGeometry"
		("ID", "TableName", "FieldName", "RecordID", "DocumentCount", "Geometry")
		VALUES ($1, $2, $3, $4, $5, $6)`,
		documentGeometry.ID, documentGeometry.TableName, documentGeometry.FieldName, documentGeometry.RecordID, documentGeometry.DocumentCount, documentGeometry.Geometry)
	return err
}

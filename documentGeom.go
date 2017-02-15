package documentGeom

import (
	"fmt"
)

type documentGeometry struct {
	ID            string
	TableName     string
	FieldName     string
	RecordID      string
	DocumentCount int
	Geometry      string
}

// TranslateDocumentGeometry starts the process of reading from MongoDB, processing the
// data and writing into Postgres.
func (s *Server) TranslateDocumentGeometry() error {

	// Retrieves the document data from MongoDB and converts to the correct structure
	documentGeometry, err := s.readMongo()
	if err != nil {
		return err
	}

	// Clears the DocumentGeometry table in Postgres before importing
	err = s.PostgresDB.truncateDocumentGeometry()
	if err != nil {
		fmt.Println(err)
		return err
	}

	// Insert records into Postgres.
	err = s.updatePostgres(&documentGeometry)
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println("Process completed successfully")
	return nil
}

func (s *Server) readMongo() ([]documentGeometry, error) {
	fmt.Println("Reading documents from MongoDB")
	session, err := s.mongoDBConnect()
	if err != nil {
		panic(err)
	}
	return s.fetchMongoDBRecords(session)
}

func (s *Server) updatePostgres(documentGeometry *[]documentGeometry) error {
	fmt.Println("Updating Postgres with document geometry")
	for _, dg := range *documentGeometry {
		err := s.PostgresDB.createDocumentGeometry(dg)
		if err != nil {
			return err
		}
	}
	return nil
}

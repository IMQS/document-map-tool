package documentGeom

import (
	"encoding/binary"
	"fmt"

	"github.com/nu7hatch/gouuid"
	geom "github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkbhex"
	"github.com/twpayne/go-geom/encoding/wkbcommon"
	"github.com/twpayne/go-geom/xy"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	fetchTableCount  = 100
	fetchRecordCount = 10000000
)

type TableMeta struct {
	ID       int    `bson:"_id"`
	DB       string `bson:"db"`
	Table    string `bson:"table"`
	KeyField string `bson:"keyField"`
}

type RecordLink struct {
	ID       bson.ObjectId `bson:"_id"`
	TableID  int           `bson:"tableID"`
	KeyValue string        `bson:"keyValue"`
	Lat      float64       `bson:"lat"`
	Lon      float64       `bson:"lon"`
}

type aggregatedRecord struct {
	documentCount int
	recordID      int
}

type MongoData interface {
	readData(session *mgo.Session, data interface{}, table string)
}

func (s *Server) mongoDBConnect() (*mgo.Session, error) {
	return mgo.Dial(s.Config.MongoDB.Host)
}

func (s *Server) fetchMongoDBRecords(session *mgo.Session) ([]documentGeometry, error) {
	var docGeoms []documentGeometry
	var tableMeta []TableMeta
	var records []RecordLink
	defer session.Close()

	// Read table_meta data from MongoDB
	if err := readData(session, &tableMeta, "table_meta", fetchTableCount); err != nil {
		return docGeoms, err
	}

	// Read document records data from MongoDB
	if err := readData(session, &records, "record_link", fetchRecordCount); err != nil {
		return docGeoms, err
	}

	tableMap := mapTableMeta(tableMeta)
	uniqueRecordsMap := mapUniqueRecords(&records)

	for _, aggRecord := range uniqueRecordsMap {
		findElementPoints(s, session, &records[aggRecord.recordID], tableMap)
		dg := populateGeometryRecord(records[aggRecord.recordID], tableMap, aggRecord.documentCount)
		docGeoms = append(docGeoms, dg)
	}

	return docGeoms, nil
}

func readData(session *mgo.Session, data interface{}, table string, limit int) error {
	s := session.DB("docs").C(table)
	iter := s.Find(nil).Limit(limit).Iter()
	err := iter.All(data)
	if err != nil {
		return err
	}
	return nil
}

// Converts TableMeta records to a map structure.
func mapTableMeta(tableMeta []TableMeta) map[int]TableMeta {
	tableMap := make(map[int]TableMeta)
	for _, table := range tableMeta {
		tableMap[table.ID] = table
	}
	return tableMap
}

func mapUniqueRecords(records *[]RecordLink) map[string]aggregatedRecord {
	recordMap := make(map[string]aggregatedRecord)

	for i := 0; i < len(*records); i++ {
		if (*records)[i].KeyValue != "undefined" {
			key := fmt.Sprintf("%v-%v", (*records)[i].TableID, (*records)[i].KeyValue)
			existingRecordMap, seenBefore := recordMap[key]
			if !seenBefore {
				recordMap[key] = aggregatedRecord{
					documentCount: 1,
					recordID:      i,
				}
			} else { // Increase document count for duplicates.
				recordMap[key] = aggregatedRecord{
					documentCount: existingRecordMap.documentCount + 1,
					recordID:      existingRecordMap.recordID,
				}
			}
		}
	}

	return recordMap
}

// Attempts to read the geometry from the Postgres table and update to Mongo
func findElementPoints(s *Server, session *mgo.Session, record *RecordLink, tableMap map[int]TableMeta) {
	// Only update if no lat or lon information stored.
	if record.Lat < 0.1 && record.Lat > -0.1 && record.Lon < 0.1 && record.Lon > -0.1 {
		var newLat, newLon float64
		tableName := tableMap[record.TableID].Table
		fieldName := tableMap[record.TableID].KeyField
		// Get geometry string from record in Postgres
		geomStr, err := s.PostgresDB.selectGeometry(tableName, fieldName, record.KeyValue)
		if err != nil {
			// Cannot determine geometry (i.e. record not found)
			newLat = 0.1
			newLon = 0.1
		} else {
			geomT, err := ewkbhex.Decode(geomStr)
			if err == nil {
				// Find centroid of geometry
				centroid, err := xy.Centroid(geomT)
				if err == nil {
					newLon = centroid[0]
					newLat = centroid[1]
				} else {
					// Use first point if centroid calculations failed
					points := geomT.FlatCoords()
					newLon = points[0]
					newLat = points[1]
				}
			}
		}

		record.Lat = newLat
		record.Lon = newLon

		// Update mongo record to prevent future calculations
		rs := session.DB("docs").C("record_link")
		colQ := bson.M{"_id": record.ID}
		change := bson.M{"$set": bson.M{"lat": newLat, "lon": newLon}}
		err = rs.Update(colQ, change)
	}
}

func populateGeometryRecord(record RecordLink, tableMap map[int]TableMeta, documentCount int) documentGeometry {
	// Create a new ID
	uuid, _ := uuid.NewV4()

	// Get and translate coordinates
	var byteOrder binary.ByteOrder
	coords := []float64{record.Lon, record.Lat, 0}
	point := geom.NewPoint(geom.XYZ)
	point.SetSRID(4326)
	point.SetCoords(coords)
	byteOrder = wkbcommon.NDR
	geometry, _ := ewkbhex.Encode(point, byteOrder)

	dg := documentGeometry{
		ID:            uuid.String(),
		TableName:     tableMap[record.TableID].Table,
		FieldName:     tableMap[record.TableID].KeyField,
		RecordID:      record.KeyValue,
		Geometry:      geometry,
		DocumentCount: documentCount,
	}
	return dg
}

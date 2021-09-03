package PoSql

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

type DatabaseCreator struct {
	tableStr string
	queryStr string
	queries  []string
	DB *sql.DB
}
func (dc DatabaseCreator) Table(table string) DatabaseCreator {
	if dc.queryStr != "" {
		if last := len(dc.queryStr) - 1; last >= 0 && dc.queryStr[last] == ',' {
			dc.queryStr = dc.queryStr[:last]
		}
		dc.queries = append(dc.queries, dc.queryStr+")")
		dc.queryStr = ""
	}
	dc.queryStr += "CREATE TABLE IF NOT EXISTS " + table + " ("
	return dc
}

func (dc DatabaseCreator) Column(name string, dataType string) DatabaseCreator {
	dc.queryStr += name + " " + dataType + ","
	return dc
}

func (dc DatabaseCreator) ID() DatabaseCreator {
	dc.queryStr += "id bigserial, PRIMARY KEY (id), "
	return dc
}
func (dc DatabaseCreator) Integer(name string) DatabaseCreator {
	return dc.Column(name, "INT")
}

func (dc DatabaseCreator) String(name string) DatabaseCreator {
	return dc.Column(name, "VARCHAR")
}

func (dc DatabaseCreator) Timestamp(name string) DatabaseCreator {
	return dc.Column(name, "timestamp")
}
func (dc DatabaseCreator) Text(name string) DatabaseCreator {
	return dc.Column(name, "Text")
}
func (dc DatabaseCreator) Init() {
	if last := len(dc.queryStr) - 1; last >= 0 && dc.queryStr[last] == ',' {
		dc.queryStr = dc.queryStr[:last]
	}
	dc.queries = append(dc.queries, dc.queryStr+")")
	for i := range dc.queries {
		_, err := dc.DB.Query(dc.queries[i])
		if err != nil {
			log.Print(err.Error())
		}
	}
}

package PoSql

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

type DatabaseCreator struct {
	table_str  string
	query_str  string
	queries    []string
	connection *sql.DB
}

func (dc DatabaseCreator) DatabaseCreator(db *sql.DB) DatabaseCreator {
	dc.connection = db
	return dc
}
func (dc DatabaseCreator) Table(table string) DatabaseCreator {
	if dc.query_str != "" {
		if last := len(dc.query_str) - 1; last >= 0 && dc.query_str[last] == ',' {
			dc.query_str = dc.query_str[:last]
		}
		dc.queries = append(dc.queries, dc.query_str+")")
		dc.query_str = ""
	}
	dc.query_str += "CREATE TABLE [IF NOT EXISTS] " + table + " ("
	return dc
}

func (dc DatabaseCreator) Column(name string, dataType string) DatabaseCreator {
	dc.query_str += name + " " + dataType + ","
	return dc
}

func (dc DatabaseCreator) ID() DatabaseCreator {
	dc.query_str += "id serial PRIMARY KEY,"
	return dc
}
func (dc DatabaseCreator) Integer(name string) DatabaseCreator {
	return dc.Column(name, "INT (255)")
}

func (dc DatabaseCreator) String(name string) DatabaseCreator {
	return dc.Column(name, "VARCHAR (255)")
}

func (dc DatabaseCreator) DateTime(name string) DatabaseCreator {
	return dc.Column(name, "DATETIME")
}
func (dc DatabaseCreator) Text(name string) DatabaseCreator {
	return dc.Column(name, "Text")
}
func (dc DatabaseCreator) Init() {
	if last := len(dc.query_str) - 1; last >= 0 && dc.query_str[last] == ',' {
		dc.query_str = dc.query_str[:last]
	}
	dc.queries = append(dc.queries, dc.query_str+")")
	for i := range dc.queries {
		_, err := dc.connection.Query(dc.queries[i])
		if err != nil {
			log.Print(err.Error())
		}
	}
}

package PoSql

import "database/sql"

type PaginateModel struct {
	TotalPages int
	CurrentPage int
	ResultsPerPage int
	Rows *sql.Rows
}

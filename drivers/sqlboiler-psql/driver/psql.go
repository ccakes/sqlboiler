// Package driver implements an sqlboiler driver.
// It can be used by either building the main.go in the same project
// and using as a binary or using the side effect import.
package driver

import (
	"database/sql"
	"embed"
	"encoding/base64"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/volatiletech/sqlboiler/v4/importers"

	"github.com/friendsofgo/errors"
	"github.com/volatiletech/sqlboiler/v4/drivers"
	"github.com/volatiletech/strmangle"

	// Side-effect import sql driver
	_ "github.com/lib/pq"
)

//go:embed override
var templates embed.FS

func init() {
	drivers.RegisterFromInit("psql", &PostgresDriver{})
}

// Assemble is more useful for calling into the library so you don't
// have to instantiate an empty type.
func Assemble(config drivers.Config) (dbinfo *drivers.DBInfo, err error) {
	driver := PostgresDriver{}
	return driver.Assemble(config)
}

// PostgresDriver holds the database connection string and a handle
// to the database connection.
type PostgresDriver struct {
	connStr        string
	conn           *sql.DB
	version        int
	addEnumTypes   bool
	enumNullPrefix string
}

// Templates that should be added/overridden
func (p PostgresDriver) Templates() (map[string]string, error) {
	tpls := make(map[string]string)
	fs.WalkDir(templates, "override", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		b, err := fs.ReadFile(templates, path)
		if err != nil {
			return err
		}
		tpls[strings.Replace(path, "override/", "", 1)] = base64.StdEncoding.EncodeToString(b)

		return nil
	})

	return tpls, nil
}

// Assemble all the information we need to provide back to the driver
func (p *PostgresDriver) Assemble(config drivers.Config) (dbinfo *drivers.DBInfo, err error) {
	defer func() {
		if r := recover(); r != nil && err == nil {
			dbinfo = nil
			err = r.(error)
		}
	}()

	user := config.MustString(drivers.ConfigUser)
	pass, _ := config.String(drivers.ConfigPass)
	dbname := config.MustString(drivers.ConfigDBName)
	host := config.MustString(drivers.ConfigHost)
	port := config.DefaultInt(drivers.ConfigPort, 5432)
	sslmode := config.DefaultString(drivers.ConfigSSLMode, "require")
	schema := config.DefaultString(drivers.ConfigSchema, "public")
	whitelist, _ := config.StringSlice(drivers.ConfigWhitelist)
	blacklist, _ := config.StringSlice(drivers.ConfigBlacklist)

	useSchema := schema != "public"

	p.addEnumTypes, _ = config[drivers.ConfigAddEnumTypes].(bool)
	p.enumNullPrefix = strmangle.TitleCase(config.DefaultString(drivers.ConfigEnumNullPrefix, "Null"))
	p.connStr = PSQLBuildQueryString(user, pass, dbname, host, port, sslmode)
	p.conn, err = sql.Open("postgres", p.connStr)
	if err != nil {
		return nil, errors.Wrap(err, "sqlboiler-psql failed to connect to database")
	}

	defer func() {
		if e := p.conn.Close(); e != nil {
			dbinfo = nil
			err = e
		}
	}()

	p.version, err = p.getVersion()
	if err != nil {
		return nil, errors.Wrap(err, "sqlboiler-psql failed to get database version")
	}

	dbinfo = &drivers.DBInfo{
		Schema: schema,
		Dialect: drivers.Dialect{
			LQ: '"',
			RQ: '"',

			UseIndexPlaceholders: true,
			UseSchema:            useSchema,
			UseDefaultKeyword:    true,
		},
	}
	dbinfo.Tables, err = drivers.Tables(p, schema, whitelist, blacklist)
	if err != nil {
		return nil, err
	}

	return dbinfo, err
}

// PSQLBuildQueryString builds a query string.
func PSQLBuildQueryString(user, pass, dbname, host string, port int, sslmode string) string {
	parts := []string{}
	if len(user) != 0 {
		parts = append(parts, fmt.Sprintf("user=%s", user))
	}
	if len(pass) != 0 {
		parts = append(parts, fmt.Sprintf("password=%s", pass))
	}
	if len(dbname) != 0 {
		parts = append(parts, fmt.Sprintf("dbname=%s", dbname))
	}
	if len(host) != 0 {
		parts = append(parts, fmt.Sprintf("host=%s", host))
	}
	if port != 0 {
		parts = append(parts, fmt.Sprintf("port=%d", port))
	}
	if len(sslmode) != 0 {
		parts = append(parts, fmt.Sprintf("sslmode=%s", sslmode))
	}

	return strings.Join(parts, " ")
}

// TableNames connects to the postgres database and
// retrieves all table names from the information_schema where the
// table schema is schema. It uses a whitelist and blacklist.
func (p *PostgresDriver) TableNames(schema string, whitelist, blacklist []string) ([]string, error) {
	var names []string

	query := fmt.Sprintf(`select table_name from information_schema.tables where table_schema = $1 and table_type = 'BASE TABLE'`)
	args := []interface{}{schema}
	if len(whitelist) > 0 {
		tables := drivers.TablesFromList(whitelist)
		if len(tables) > 0 {
			query += fmt.Sprintf(" and table_name in (%s)", strmangle.Placeholders(true, len(tables), 2, 1))
			for _, w := range tables {
				args = append(args, w)
			}
		}
	} else if len(blacklist) > 0 {
		tables := drivers.TablesFromList(blacklist)
		if len(tables) > 0 {
			query += fmt.Sprintf(" and table_name not in (%s)", strmangle.Placeholders(true, len(tables), 2, 1))
			for _, b := range tables {
				args = append(args, b)
			}
		}
	}

	query += ` order by table_name;`

	rows, err := p.conn.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}

	return names, nil
}

// ViewNames connects to the postgres database and
// retrieves all view names from the information_schema where the
// view schema is schema. It uses a whitelist and blacklist.
func (p *PostgresDriver) ViewNames(schema string, whitelist, blacklist []string) ([]string, error) {
	var names []string

	query := `select table_name from information_schema.views where table_schema = $1`
	args := []interface{}{schema}
	if len(whitelist) > 0 {
		views := drivers.TablesFromList(whitelist)
		if len(views) > 0 {
			query += fmt.Sprintf(" and table_name in (%s)", strmangle.Placeholders(true, len(views), 2, 1))
			for _, w := range views {
				args = append(args, w)
			}
		}
	} else if len(blacklist) > 0 {
		views := drivers.TablesFromList(blacklist)
		if len(views) > 0 {
			query += fmt.Sprintf(" and table_name not in (%s)", strmangle.Placeholders(true, len(views), 2, 1))
			for _, b := range views {
				args = append(args, b)
			}
		}
	}

	query += ` order by table_name;`

	rows, err := p.conn.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		names = append(names, name)
	}

	return names, nil
}

// ViewCapabilities return what actions are allowed for a view.
func (p *PostgresDriver) ViewCapabilities(schema, name string) (drivers.ViewCapabilities, error) {
	capabilities := drivers.ViewCapabilities{}

	query := `select
	is_insertable_into = 'YES',
	is_updatable = 'YES',
	is_trigger_insertable_into = 'YES',
	is_trigger_updatable = 'YES',
	is_trigger_deletable = 'YES'
	from information_schema.views where table_schema = $1 and table_name = $2
	order by table_name;`

	row := p.conn.QueryRow(query, schema, name)

	var insertable, updatable, trInsert, trUpdate, trDelete bool
	if err := row.Scan(&insertable, &updatable, &trInsert, &trUpdate, &trDelete); err != nil {
		return capabilities, err
	}

	capabilities.CanInsert = insertable || trInsert
	capabilities.CanUpsert = insertable && updatable

	return capabilities, nil
}

func (p *PostgresDriver) ViewColumns(schema, tableName string, whitelist, blacklist []string) ([]drivers.Column, error) {
	return p.Columns(schema, tableName, whitelist, blacklist)
}

// Columns takes a table name and attempts to retrieve the table information
// from the database information_schema.columns. It retrieves the column names
// and column types and returns those as a []Column after TranslateColumnType()
// converts the SQL types to Go types, for example: "varchar" to "string"
func (p *PostgresDriver) Columns(schema, tableName string, whitelist, blacklist []string) ([]drivers.Column, error) {
	var columns []drivers.Column
	args := []interface{}{fmt.Sprintf(`"%s"."%s"`, schema, tableName)}

	query := `
		WITH resolved_view AS (
			WITH re AS (
				SELECT
					ev_class,
					ev_action as view_definition
				FROM pg_rewrite
			),
			target_lists AS (
				SELECT
					ev_class,
					regexp_split_to_array(view_definition, 'targetList') AS x
				FROM re
			),
			last_target_list_wo_tail AS (
				SELECT
					ev_class,
					(regexp_split_to_array(x[array_upper(x, 1)], ':onConflict'))[1] AS x
				FROM target_lists
			),
			target_entries AS (
				SELECT
					ev_class,
					unnest((regexp_split_to_array(x, 'TARGETENTRY'))[2:]) AS entry
				FROM last_target_list_wo_tail
			)

			SELECT
				ev_class,
				substring(entry from ':resname (.*?) :') AS col_name,
				substring(entry from ':resorigtbl (.*?) :')::oid AS resorigtbl,
				substring(entry from ':resorigcol (.*?) :')::oid AS resorigcol
			FROM target_entries
		),
		resolved_attribute AS (
			SELECT
				a.attrelid attorigrelid,
				a.attnum attorignum,
				a.attrelid,
				a.attname,
				a.attnum,
				a.atttypid,
				a.attlen,
				a.atttypmod,
				a.attnotnull,
				a.attgenerated,
				a.attidentity
			FROM pg_attribute a
			JOIN pg_class c ON c.oid = a.attrelid
			WHERE
				c.relkind = 'r'

			UNION

			SELECT
				oa.attrelid attorigrelid,
				oa.attnum attorignum,
				ra.attrelid,
				oa.attname,
				ra.attnum,
				ra.atttypid,
				ra.attlen,
				ra.atttypmod,
				ra.attnotnull,
				ra.attgenerated,
				ra.attidentity
			FROM pg_attribute oa
			JOIN pg_class c ON c.oid = oa.attrelid
			JOIN resolved_view rv ON rv.ev_class = oa.attrelid AND rv.col_name = oa.attname
			JOIN pg_attribute ra ON ra.attrelid = rv.resorigtbl AND ra.attnum = resorigcol
			WHERE
				c.relkind = 'v'
		)

		SELECT
			a.attname column_name,
			ct.column_type,
			CASE
				WHEN t.typlen = -1 AND a.atttypmod >= 0 THEN format('%s(%s)', ct.column_type, information_schema._pg_char_max_length(a.atttypid, a.atttypmod))
				ELSE COALESCE(bt.typname, t.typname)
			END column_full_type,
			COALESCE(bt.typname, t.typname) udt_name,
			format_type(at.oid, NULL::integer) array_type,
			CASE
				WHEN t.typtype = 'd' THEN t.typname
				ELSE NULL::text
			END domain_name,
			CASE
				WHEN a.attgenerated = '' THEN pg_get_expr(ad.adbin, ad.adrelid)
				ELSE NULL::text
			END column_default,
			COALESCE(col_description(a.attrelid, a.attnum), '') column_comment,
			(a.attnotnull = false) is_nullable,
			(a.attgenerated != '') is_generated,
			(a.attidentity != '') is_identity,
			(
				SELECT EXISTS (
					SELECT 1
					FROM pg_index i
					WHERE
						i.indrelid = a.attrelid
						AND a.attnum = ALL(i.indkey)
						AND i.indisunique = true
				)
			) is_unique
		FROM resolved_attribute a
		JOIN (pg_type t JOIN pg_namespace nt ON t.typnamespace = nt.oid) ON a.atttypid = t.oid
		LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
		LEFT JOIN pg_type at ON at.oid = t.typelem
		LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON bt.typnamespace = nbt.oid) ON t.typtype = 'd'::"char" AND t.typbasetype = bt.oid,
		LATERAL (
			SELECT
				CASE
					WHEN t.typtype = 'd' THEN
					CASE
						WHEN bt.typelem != 0 AND bt.typlen = -1 THEN 'ARRAY'::text
						WHEN nbt.nspname = 'pg_catalog' THEN format_type(t.typbasetype, NULL::integer)
						ELSE 'USER-DEFINED'::text
					END
					ELSE
					CASE
						WHEN t.typelem != 0 AND t.typlen = -1 THEN 'ARRAY'::text
						WHEN nt.nspname = 'pg_catalog' THEN format_type(a.atttypid, NULL::integer)
						ELSE 'USER-DEFINED'::text
					END
				END column_type
		) ct
		WHERE
			a.attorigrelid = $1::regclass
			AND a.attnum > 0`

	if len(whitelist) > 0 {
		cols := drivers.ColumnsFromList(whitelist, tableName)
		if len(cols) > 0 {
			query += fmt.Sprintf(" and a.attname in (%s)", strmangle.Placeholders(true, len(cols), 3, 1))
			for _, w := range cols {
				args = append(args, w)
			}
		}
	} else if len(blacklist) > 0 {
		cols := drivers.ColumnsFromList(blacklist, tableName)
		if len(cols) > 0 {
			query += fmt.Sprintf(" and a.attname not in (%s)", strmangle.Placeholders(true, len(cols), 3, 1))
			for _, w := range cols {
				args = append(args, w)
			}
		}
	}

	query += ` order by a.attorignum;`

	rows, err := p.conn.Query(query, args...)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var colName, colType, colFullType, udtName, comment string
		var defaultValue, arrayType, domainName *string
		var nullable, generated, identity, unique bool
		if err := rows.Scan(&colName, &colType, &colFullType, &udtName, &arrayType, &domainName, &defaultValue, &comment, &nullable, &generated, &identity, &unique); err != nil {
			return nil, errors.Wrapf(err, "unable to scan for table %s", tableName)
		}

		column := drivers.Column{
			Name:          colName,
			DBType:        colType,
			FullDBType:    colFullType,
			ArrType:       arrayType,
			DomainName:    domainName,
			UDTName:       udtName,
			Comment:       comment,
			Nullable:      nullable,
			AutoGenerated: generated,
			Unique:        unique,
		}
		if defaultValue != nil {
			column.Default = *defaultValue
		}

		if identity != false {
			column.Default = "IDENTITY"
		}

		// A generated column technically has a default value
		if generated && column.Default == "" {
			column.Default = "GENERATED"
		}

		// A nullable column can always default to NULL
		if nullable && column.Default == "" {
			column.Default = "NULL"
		}

		columns = append(columns, column)
	}

	return columns, nil
}

// PrimaryKeyInfo looks up the primary key for a table.
func (p *PostgresDriver) PrimaryKeyInfo(schema, tableName string) (*drivers.PrimaryKey, error) {
	pkey := &drivers.PrimaryKey{}
	var err error

	query := `
	select tc.constraint_name
	from information_schema.table_constraints as tc
	where tc.table_name = $1 and tc.constraint_type = 'PRIMARY KEY' and tc.table_schema = $2;`

	row := p.conn.QueryRow(query, tableName, schema)
	if err = row.Scan(&pkey.Name); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	queryColumns := `
	select kcu.column_name
	from   information_schema.key_column_usage as kcu
	where  constraint_name = $1 and table_name = $2 and table_schema = $3
	order by kcu.ordinal_position;`

	var rows *sql.Rows
	if rows, err = p.conn.Query(queryColumns, pkey.Name, tableName, schema); err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string

		err = rows.Scan(&column)
		if err != nil {
			return nil, err
		}

		columns = append(columns, column)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	pkey.Columns = columns

	return pkey, nil
}

// ForeignKeyInfo retrieves the foreign keys for a given table name.
func (p *PostgresDriver) ForeignKeyInfo(schema, tableName string) ([]drivers.ForeignKey, error) {
	var fkeys []drivers.ForeignKey

	whereConditions := []string{"oid = $1::regclass"}
	if p.version >= 120000 {
		whereConditions = append(whereConditions, "srcattgenerated = ''", "dstattgenerated = ''")
	}

	query := fmt.Sprintf(`
	WITH resolved_view AS (
		WITH re AS (
			SELECT
				ev_class,
				ev_action as view_definition
			FROM pg_rewrite
		),
		target_lists AS (
			SELECT
				ev_class,
				regexp_split_to_array(view_definition, 'targetList') AS x
			FROM re
		),
		last_target_list_wo_tail AS (
			SELECT
				ev_class,
				(regexp_split_to_array(x[array_upper(x, 1)], ':onConflict'))[1] AS x
			FROM target_lists
		),
		target_entries AS (
			SELECT
				ev_class,
				unnest((regexp_split_to_array(x, 'TARGETENTRY'))[2:]) AS entry
			FROM last_target_list_wo_tail
		)

		SELECT
			ev_class,
			substring(entry from ':resname (.*?) :') AS col_name,
			substring(entry from ':resorigtbl (.*?) :')::oid AS resorigtbl,
			substring(entry from ':resorigcol (.*?) :')::oid AS resorigcol
		FROM target_entries
	),
	rels AS (
		SELECT
			pgc.oid,
			pgasrc.attgenerated srcattgenerated,
			pgadst.attgenerated dstattgenerated,
			pgcon.conname,
			pgc.relname as source_table,
			pgasrc.attname as source_column,
			dstlookupname.relname as dest_table,
			pgadst.attname as dest_column
		FROM pg_namespace pgn
		JOIN pg_class pgc on pgn.oid = pgc.relnamespace and pgc.relkind = 'r'
		JOIN pg_constraint pgcon on pgn.oid = pgcon.connamespace and pgc.oid = pgcon.conrelid
		JOIN pg_class dstlookupname on pgcon.confrelid = dstlookupname.oid
		JOIN pg_namespace dstnsp on dstnsp.oid = dstlookupname.relnamespace and dstlookupname.relkind = 'r'
		JOIN pg_attribute pgasrc on pgc.oid = pgasrc.attrelid and pgasrc.attnum = ANY(pgcon.conkey)
		JOIN pg_attribute pgadst on pgcon.confrelid = pgadst.attrelid and pgadst.attnum = ANY(pgcon.confkey)
		WHERE
			pgcon.contype = 'f'
			AND pgc.relnamespace = dstlookupname.relnamespace

		UNION

		SELECT
			pgc.oid,
			pgasrc.attgenerated srcattgenerated,
			pgadst.attgenerated dstattgenerated,
			pgcon.conname,
			pgc.relname AS source_table,
			pgasrc.attname AS source_column,
			dstlookupname.relname AS dest_table,
			pgadst.attname AS dest_column
		FROM pg_class pgc
		JOIN resolved_view rv ON rv.ev_class = pgc.oid AND pgc.relkind = 'v'
		JOIN pg_class tc ON tc.oid = rv.resorigtbl AND tc.relkind = 'r'
		JOIN pg_constraint pgcon ON pgcon.conrelid = rv.resorigtbl AND rv.resorigcol = ANY(pgcon.conkey)
		JOIN pg_class dstlookupname on pgcon.confrelid = dstlookupname.oid
		JOIN pg_attribute pgasrc ON pgasrc.attrelid = rv.resorigtbl AND pgasrc.attnum = rv.resorigcol
		JOIN pg_attribute pgadst ON pgadst.attrelid = pgcon.confrelid AND pgadst.attnum = ANY(pgcon.confkey)
		WHERE
			pgcon.contype = 'f'
			AND tc.relnamespace = dstlookupname.relnamespace
	)

	SELECT
		conname,
		source_table,
		source_column,
		dest_table,
		dest_column
	FROM rels
	WHERE %s
	ORDER BY
		conname,
		source_table,
		source_column,
		dest_table,
		dest_column`,
		strings.Join(whereConditions, " and "),
	)

	var rows *sql.Rows
	var err error
	if rows, err = p.conn.Query(query, fmt.Sprintf(`"%s"."%s"`, schema, tableName)); err != nil {
		return nil, err
	}

	for rows.Next() {
		var fkey drivers.ForeignKey
		var sourceTable string

		fkey.Table = tableName
		err = rows.Scan(&fkey.Name, &sourceTable, &fkey.Column, &fkey.ForeignTable, &fkey.ForeignColumn)
		if err != nil {
			return nil, err
		}

		fkeys = append(fkeys, fkey)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return fkeys, nil
}

// TranslateColumnType converts postgres database types to Go types, for example
// "varchar" to "string" and "bigint" to "int64". It returns this parsed data
// as a Column object.
func (p *PostgresDriver) TranslateColumnType(c drivers.Column) drivers.Column {
	if c.Nullable {
		switch c.DBType {
		case "bigint", "bigserial":
			c.Type = "null.Int64"
		case "integer", "serial":
			c.Type = "null.Int"
		case "oid":
			c.Type = "null.Uint32"
		case "smallint", "smallserial":
			c.Type = "null.Int16"
		case "decimal", "numeric":
			c.Type = "types.NullDecimal"
		case "double precision":
			c.Type = "null.Float64"
		case "real":
			c.Type = "null.Float32"
		case "bit", "interval", "bit varying", "character", "money", "character varying", "cidr", "inet", "macaddr", "text", "uuid", "xml":
			c.Type = "null.String"
		case `"char"`:
			c.Type = "null.Byte"
		case "bytea":
			c.Type = "null.Bytes"
		case "json", "jsonb":
			c.Type = "null.JSON"
		case "boolean":
			c.Type = "null.Bool"
		case "date", "time", "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone":
			c.Type = "null.Time"
		case "point":
			c.Type = "pgeo.NullPoint"
		case "line":
			c.Type = "pgeo.NullLine"
		case "lseg":
			c.Type = "pgeo.NullLseg"
		case "box":
			c.Type = "pgeo.NullBox"
		case "path":
			c.Type = "pgeo.NullPath"
		case "polygon":
			c.Type = "pgeo.NullPolygon"
		case "circle":
			c.Type = "pgeo.NullCircle"
		case "ARRAY":
			var dbType string
			c.Type, dbType = getArrayType(c)
			// Make DBType something like ARRAYinteger for parsing with randomize.Struct
			c.DBType += dbType
		case "USER-DEFINED":
			switch c.UDTName {
			case "hstore":
				c.Type = "types.HStore"
				c.DBType = "hstore"
			case "citext":
				c.Type = "null.String"
			default:
				c.Type = "string"
				fmt.Fprintf(os.Stderr, "warning: incompatible data type detected: %s\n", c.UDTName)
			}
		default:
			if enumName := strmangle.ParseEnumName(c.DBType); enumName != "" && p.addEnumTypes {
				c.Type = p.enumNullPrefix + strmangle.TitleCase(enumName)
			} else {
				c.Type = "null.String"
			}
		}
	} else {
		switch c.DBType {
		case "bigint", "bigserial":
			c.Type = "int64"
		case "integer", "serial":
			c.Type = "int"
		case "oid":
			c.Type = "uint32"
		case "smallint", "smallserial":
			c.Type = "int16"
		case "decimal", "numeric":
			c.Type = "types.Decimal"
		case "double precision":
			c.Type = "float64"
		case "real":
			c.Type = "float32"
		case "bit", "interval", "uuint", "bit varying", "character", "money", "character varying", "cidr", "inet", "macaddr", "text", "uuid", "xml":
			c.Type = "string"
		case `"char"`:
			c.Type = "types.Byte"
		case "json", "jsonb":
			c.Type = "types.JSON"
		case "bytea":
			c.Type = "[]byte"
		case "boolean":
			c.Type = "bool"
		case "date", "time", "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone":
			c.Type = "time.Time"
		case "point":
			c.Type = "pgeo.Point"
		case "line":
			c.Type = "pgeo.Line"
		case "lseg":
			c.Type = "pgeo.Lseg"
		case "box":
			c.Type = "pgeo.Box"
		case "path":
			c.Type = "pgeo.Path"
		case "polygon":
			c.Type = "pgeo.Polygon"
		case "circle":
			c.Type = "pgeo.Circle"
		case "ARRAY":
			var dbType string
			c.Type, dbType = getArrayType(c)
			// Make DBType something like ARRAYinteger for parsing with randomize.Struct
			c.DBType += dbType
		case "USER-DEFINED":
			switch c.UDTName {
			case "hstore":
				c.Type = "types.HStore"
				c.DBType = "hstore"
			case "citext":
				c.Type = "string"
			default:
				c.Type = "string"
				fmt.Fprintf(os.Stderr, "warning: incompatible data type detected: %s\n", c.UDTName)
			}
		default:
			if enumName := strmangle.ParseEnumName(c.DBType); enumName != "" && p.addEnumTypes {
				c.Type = strmangle.TitleCase(enumName)
			} else {
				c.Type = "string"
			}
		}
	}

	return c
}

// getArrayType returns the correct boil.Array type for each database type
func getArrayType(c drivers.Column) (string, string) {
	// If a domain is created with a statement like this: "CREATE DOMAIN
	// text_array AS TEXT[] CHECK ( ... )" then the array type will be null,
	// but the udt name will be whatever the underlying type is with a leading
	// underscore. Note that this code handles some types, but not nearly all
	// the possibities. Notably, an array of a user-defined type ("CREATE
	// DOMAIN my_array AS my_type[]") will be treated as an array of strings,
	// which is not guaranteed to be correct.
	if c.ArrType != nil {
		switch *c.ArrType {
		case "bigint", "bigserial", "integer", "serial", "smallint", "smallserial", "oid":
			return "types.Int64Array", *c.ArrType
		case "bytea":
			return "types.BytesArray", *c.ArrType
		case "bit", "interval", "uuint", "bit varying", "character", "money", "character varying", "cidr", "inet", "macaddr", "text", "uuid", "xml":
			return "types.StringArray", *c.ArrType
		case "boolean":
			return "types.BoolArray", *c.ArrType
		case "decimal", "numeric":
			return "types.DecimalArray", *c.ArrType
		case "double precision", "real":
			return "types.Float64Array", *c.ArrType
		default:
			return "types.StringArray", *c.ArrType
		}
	} else {
		switch c.UDTName {
		case "_int4", "_int8":
			return "types.Int64Array", c.UDTName
		case "_bytea":
			return "types.BytesArray", c.UDTName
		case "_bit", "_interval", "_varbit", "_char", "_money", "_varchar", "_cidr", "_inet", "_macaddr", "_citext", "_text", "_uuid", "_xml":
			return "types.StringArray", c.UDTName
		case "_bool":
			return "types.BoolArray", c.UDTName
		case "_numeric":
			return "types.DecimalArray", c.UDTName
		case "_float4", "_float8":
			return "types.Float64Array", c.UDTName
		default:
			return "types.StringArray", c.UDTName
		}
	}
}

// Imports for the postgres driver
func (p PostgresDriver) Imports() (importers.Collection, error) {
	var col importers.Collection

	col.All = importers.Set{
		Standard: importers.List{
			`"strconv"`,
		},
	}
	col.Singleton = importers.Map{
		"psql_upsert": {
			Standard: importers.List{
				`"fmt"`,
				`"strings"`,
			},
			ThirdParty: importers.List{
				`"github.com/volatiletech/strmangle"`,
				`"github.com/volatiletech/sqlboiler/v4/drivers"`,
			},
		},
	}
	col.TestSingleton = importers.Map{
		"psql_suites_test": {
			Standard: importers.List{
				`"testing"`,
			},
		},
		"psql_main_test": {
			Standard: importers.List{
				`"bytes"`,
				`"database/sql"`,
				`"fmt"`,
				`"io"`,
				`"io/ioutil"`,
				`"os"`,
				`"os/exec"`,
				`"regexp"`,
				`"strings"`,
			},
			ThirdParty: importers.List{
				`"github.com/kat-co/vala"`,
				`"github.com/friendsofgo/errors"`,
				`"github.com/spf13/viper"`,
				`"github.com/volatiletech/sqlboiler/v4/drivers/sqlboiler-psql/driver"`,
				`"github.com/volatiletech/randomize"`,
				`_ "github.com/lib/pq"`,
			},
		},
	}
	col.BasedOnType = importers.Map{
		"null.Float32": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Float64": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Int": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Int8": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Int16": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Int32": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Int64": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Uint": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Uint8": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Uint16": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Uint32": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Uint64": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.String": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Bool": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Time": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.JSON": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Bytes": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"time.Time": {
			Standard: importers.List{`"time"`},
		},
		"types.JSON": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.Decimal": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.BytesArray": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.Int64Array": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.Float64Array": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.BoolArray": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.StringArray": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.DecimalArray": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.HStore": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"pgeo.Point": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.Line": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.Lseg": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.Box": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.Path": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.Polygon": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"types.NullDecimal": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"pgeo.Circle": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullPoint": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullLine": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullLseg": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullBox": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullPath": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullPolygon": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullCircle": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types/pgeo"`},
		},
	}

	return col, nil
}

// getVersion gets the version of underlying database
func (p *PostgresDriver) getVersion() (int, error) {
	type versionInfoType struct {
		ServerVersionNum int `json:"server_version_num"`
	}
	versionInfo := &versionInfoType{}

	row := p.conn.QueryRow("SHOW server_version_num")
	if err := row.Scan(&versionInfo.ServerVersionNum); err != nil {
		return 0, err
	}

	return versionInfo.ServerVersionNum, nil
}

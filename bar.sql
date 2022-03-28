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
  -- FKs: table-table
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

  UNION

  -- FKs: view-table
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

  UNION

  -- FKs: table-view
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
  JOIN pg_constraint pgcon ON pgcon.conrelid = rv.resorigtbl AND rv.resorigcol = ANY(pgcon.confkey)
  JOIN pg_class dstlookupname on pgcon.conrelid = dstlookupname.oid
  JOIN pg_attribute pgasrc ON pgasrc.attrelid = pgcon.confrelid AND pgasrc.attnum = ANY(pgcon.confkey)
  JOIN pg_attribute pgadst ON pgadst.attrelid = rv.resorigtbl AND pgadst.attnum = rv.resorigcol
  WHERE
    pgcon.contype = 'f'
)

SELECT
  conname,
  source_table,
  source_column,
  dest_table,
  dest_column
FROM rels
WHERE
  oid = 'network.device'::regclass::oid
  AND srcattgenerated = ''
  AND dstattgenerated = ''
ORDER BY
  conname,
  source_table,
  source_column,
  dest_table,
  dest_column;

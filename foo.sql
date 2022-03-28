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
  a.attrelid,
  a.attnum,
  t.oid,
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
        AND a.attnum = ANY(i.indkey)
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
  a.attorigrelid = 'network.device_vrf_membership'::regclass
  AND a.attnum > 0
ORDER BY a.attnum;

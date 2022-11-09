import { IBaseQueries, ContextValue } from '@sqltools/types';
import queryFactory from '@sqltools/base-driver/dist/lib/factory';
import { ISnowflakeQueries } from '../extension';

/** write your queries here go fetch desired data. This queries are just examples copied from SQLite driver */

const fetchDatabases: IBaseQueries['fetchDatabases'] = queryFactory`
SELECT database_name as "label"
     , database_name as "database"
     , '${ContextValue.DATABASE}' as "type"
     , 'database' as "detail"
FROM information_schema.databases
ORDER BY database_name
`;

const describeTable: IBaseQueries['describeTable'] = queryFactory`
  SELECT C.*
  FROM ${p => p.database}.information_schema.columns c
  WHERE table_catalog = UPPER('${p => p.database}')
        AND table_schema = UPPER('${p => p.schema}')
        AND table_name = UPPER('${p => p.label}')
  ORDER BY ordinal_position
`;

const fetchColumns: IBaseQueries['fetchColumns'] = queryFactory`
SELECT
  column_name as "label",
  '${ContextValue.COLUMN}' as type,
  'table_name' as "table",
  data_type as "dataType",
  UPPER(data_type || (
    CASE WHEN character_maximum_length > 0 THEN (
      '(' || character_maximum_length || ')'
    ) ELSE '' END
  )) AS "detail",
  character_maximum_length AS size,
  table_catalog as "database",
  table_schema as "schema",
  column_default as "defaultValue",
  is_nullable as "isNullable",
  false as "isPk",
  false as "isFk"
FROM "${p => p.database}".information_schema.columns c
WHERE table_schema = '${p => p.schema}'
      AND table_name = '${p => p.label}'
ORDER BY ordinal_position
`;

const fetchRecords: IBaseQueries['fetchRecords'] = queryFactory`
SELECT *
FROM "${p => p.table.database}"."${p => p.table.schema}"."${p => p.table.label}"
LIMIT ${p => p.limit || 50}
OFFSET ${p => p.offset || 0};
`;

const countRecords: IBaseQueries['countRecords'] = queryFactory`
SELECT COUNT(1) AS "total"
FROM "${p => p.table.database}"."${p => p.table.schema}"."${p => p.table.label}"
`;

const fetchTablesAndViews = (type: ContextValue, tableType = 'BASE TABLE'): IBaseQueries['fetchTables'] => queryFactory`
SELECT table_name as "label",
  '${type}' as "type",
  table_schema as "schema",
  table_catalog as "database",
  ${type === ContextValue.VIEW ? 'TRUE' : 'FALSE'} as "isView"
FROM "${p => p.database}".information_schema.tables
WHERE table_schema = '${p => p.schema}'
      AND table_type = '${tableType}'
ORDER BY table_name
`;

const fetchSchemas: IBaseQueries['fetchSchemas'] = queryFactory`
SELECT
  schema_name as "label",
  schema_name as "schema",
  '${ContextValue.SCHEMA}' as "type",
  'group-by-ref-type' as "iconId",
  catalog_name as "database"
FROM ${p => p.database}.information_schema.schemata
WHERE
  schema_name != 'INFORMATION_SCHEMA'
ORDER BY 2
`;

const fetchTables: IBaseQueries['fetchTables'] = fetchTablesAndViews(ContextValue.TABLE);

const fetchMaterializedViews: ISnowflakeQueries['fetchMaterializedViews'] = queryFactory`
SHOW MATERIALIZED VIEWS IN ${p => p.database}.${p => p.schema}
`;

const fetchViews: IBaseQueries['fetchTables'] = fetchTablesAndViews(ContextValue.VIEW , 'VIEW');

const fetchStages: ISnowflakeQueries['fetchStages'] = queryFactory`
SHOW STAGES IN ${p => p.database}.${p => p.schema}
`;

const fetchPipes: ISnowflakeQueries['fetchPipes'] = queryFactory`
SHOW PIPES IN ${p => p.database}.${p => p.schema}
`;

const fetchStreams: ISnowflakeQueries['fetchStreams'] = queryFactory`
SHOW STREAMS IN ${p => p.database}.${p => p.schema}
`;

const fetchTasks: ISnowflakeQueries['fetchTasks'] = queryFactory`
SHOW TASKS IN ${p => p.database}.${p => p.schema}
`;

const fetchSFFunctions: ISnowflakeQueries['fetchSFFunctions'] = queryFactory`
SHOW USER FUNCTIONS IN ${p => p.database}.${p => p.schema}
`;

const fetchProcedures: ISnowflakeQueries['fetchProcedures'] = queryFactory`
SHOW USER PROCEDURES IN ${p => p.database}.${p => p.schema}
`;

const fetchFileFormats: ISnowflakeQueries['fetchFileFormats'] = queryFactory`
SHOW FILE FORMATS IN ${p => p.database}.${p => p.schema}
`;

const fetchSequences: ISnowflakeQueries['fetchSequences'] = queryFactory`
SHOW SEQUENCES IN ${p => p.database}.${p => p.schema}
`;

/**
 * Intellisense code completion is not implemented
 * 
 * Auto completion should use SHOW TABLES <object> LIKE <pattern> IN <object_type>
 * because selecting from INFORMATION SCHEMA directly is too slow.
 * 
 * Parsing the output of SHOW commands requires RESULT_SCAN(last_query_id())
 * in two SQL commands, but queryFactory not supporting to run multiple queries.
 */ 
const searchTables: IBaseQueries['searchTables'] = queryFactory`
SELECT 'no-table' as "label",
  '${ContextValue.TABLE}' as "type",
  'no-schema' as "schema",
  'no-db' as "database",
  false AS "isView",
  'table' AS "description",
  'no-schema.no-db.table' as "detail"
WHERE 1 = 0
`;

/**
 * Intellisense code completion is not implemented
 * 
 * Auto completion should use SHOW TABLES <object> LIKE <pattern> IN <object_type>
 * because selecting from INFORMATION SCHEMA directly is too slow.
 * 
 * Parsing the output of SHOW commands requires RESULT_SCAN(last_query_id())
 * in two SQL commands, but queryFactory not supporting to run multiple queries.
 */ 
const searchColumns: IBaseQueries['searchColumns'] = queryFactory`
SELECT 'no-column' as "label",
  'no-table' as "table",
  'no-type' as "dataType",
  'no-isnull' as "isNullable",
  'column' AS "description",
  '${ContextValue.COLUMN}' as "type"
WHERE 1 = 0
`;


export default {
  countRecords,
  describeTable,
  fetchColumns,
  fetchDatabases,
  fetchFileFormats,
  fetchSFFunctions,
  fetchMaterializedViews,
  fetchPipes,
  fetchProcedures,
  fetchRecords,
  fetchSchemas,
  fetchSequences,
  fetchStages,
  fetchStreams,
  fetchTables,
  fetchTasks,
  fetchViews,
  searchColumns,
  searchTables
}

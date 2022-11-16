import { IBaseQueries, ContextValue, NSDatabase } from '@sqltools/types';
import queryFactory from '@sqltools/base-driver/dist/lib/factory';
import { SnowflakeDatabase } from '../extension';

const fetchDatabases: IBaseQueries['fetchDatabases'] = queryFactory`
SHOW DATABASES
`;

const fetchSchemas: IBaseQueries['fetchSchemas'] = queryFactory`
SHOW SCHEMAS IN "${p => p.database}"
`;

const describeTable: IBaseQueries['describeTable'] = queryFactory`
  SELECT 1 AS NOT_IMPLEMENTED
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

const fetchTables: IBaseQueries['fetchTables'] = queryFactory<NSDatabase.ISchema>`
SHOW TABLES IN "${p => p.database}".${p => p.label}
`;

const fetchViews: IBaseQueries['fetchViews'] = queryFactory<NSDatabase.ISchema>`
SHOW VIEWS IN "${p => p.database}".${p => p.label}
`;

const fetchMaterializedViews: IBaseQueries['fetchMaterializedViews'] = queryFactory<SnowflakeDatabase.IMaterializedView>`
SHOW MATERIALIZED VIEWS IN ${p => p.database}.${p => p.name}
`;

const fetchStages: IBaseQueries['fetchStages'] = queryFactory<SnowflakeDatabase.IStage>`
SHOW STAGES IN ${p => p.database}.${p => p.name}
`;

const fetchPipes: IBaseQueries['fetchPipes'] = queryFactory<SnowflakeDatabase.IPipe>`
SHOW PIPES IN ${p => p.database}.${p => p.name}
`;

const fetchStreams: IBaseQueries['fetchStreams'] = queryFactory<SnowflakeDatabase.IStream>`
SHOW STREAMS IN ${p => p.database}.${p => p.name}
`;

const fetchTasks: IBaseQueries['fetchTasks'] = queryFactory<SnowflakeDatabase.ITask>`
SHOW TASKS IN ${p => p.database}.${p => p.name}
`;

const fetchFunctions: IBaseQueries['fetchFunctions'] = queryFactory`
SHOW USER FUNCTIONS IN ${p => p.database}.${p => p.name}
`;

const fetchProcedures: IBaseQueries['fetchProcedures'] = queryFactory<NSDatabase.IProcedure>`
SHOW USER PROCEDURES IN ${p => p.database}.${p => p.name}
`;

const fetchFileFormats: IBaseQueries['fetchFileFormats'] = queryFactory<SnowflakeDatabase.IFileFormat>`
SHOW FILE FORMATS IN ${p => p.database}.${p => p.name}
`;

const fetchSequences: IBaseQueries['fetchSequences'] = queryFactory<SnowflakeDatabase.ISequence>`
SHOW SEQUENCES IN ${p => p.database}.${p => p.name}
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
  fetchFunctions,
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

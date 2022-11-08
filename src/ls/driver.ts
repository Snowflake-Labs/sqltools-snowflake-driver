import AbstractDriver from '@sqltools/base-driver';
import queries from './queries';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0 } from '@sqltools/types';
import { v4 as generateId } from 'uuid';
import { Snowflake } from 'snowflake-promise';
import { SnowflakeDatabase } from '../extension';

type DriverLib = any;
type DriverOptions = any;

export default class SnowflakeDriver extends AbstractDriver<DriverLib, DriverOptions> implements IConnectionDriver {
  queries = queries;
  public async open() {
    if (this.connection) {
      return this.connection;
    }
    
    let connOptions = {
      account: this.credentials.account,
      database: this.credentials.database,
      warehouse: this.credentials.warehouse,
      authenticator: this.credentials.authenticator,
      privateKeyPath: this.credentials.privateKeyPath,
      privateKeyPass: this.credentials.privateKeyPass,
      token: this.credentials.token,
      username: this.credentials.username,
      password: this.credentials.password
    };
    connOptions = {
      ...connOptions,
      ...(this.credentials["snowflakeOptions"] || {})
    };

    if(this.credentials.account.indexOf('snowflakecomputing.com') > 0) {
      return Promise.reject(
        new Error(
          "The account should not include snowflakecomputing.com. It needs to follow the <account_name>[.<region_id>][.<cloud>] \
          pattern and should be one of the supported account locators listed at \
          https://docs.snowflake.com/en/user-guide/nodejs-driver-use.html#required-connection-options"
        )
      );
    }

    // snowflake-sdk 1.6.0 does not generate the region in externalbrowser auth
    if(connOptions.authenticator === 'EXTERNALBROWSER' && this.credentials.account.indexOf('.') > 0) {
      connOptions = {
        ...connOptions,
        ...{
          region: this.credentials.account.substring(this.credentials.account.indexOf('.') + 1)
        }
      }
    }

    const loggingOptions = {};
    const configureOptions = {
      ocspFailOpen: this.credentials["ocspOptions"]["ocspFailOpen"]
    }
    
    try {
      const conn = new Snowflake(connOptions, loggingOptions, configureOptions);

      // Browser Based SSO requires connectAsync
      if(connOptions.authenticator === 'EXTERNALBROWSER') {
        await conn.connectAsync(); 
      }
      else {
        await conn.connect();
      }

      await conn.execute('ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE');
      this.connection = Promise.resolve(conn);
    } catch (error) {
      return Promise.reject(error);
    }

    const db = this.credentials.database;
    const warehouse = this.credentials.warehouse;
    if (!db || !db.trim()) {
      return Promise.reject({ message: 'Database parameter not set in connection. Please set it in the connection details.' });
    }
    if (!warehouse || !warehouse.trim()) {
      return Promise.reject({ message: 'Warehouse parameter not set in connection. Please set it in the connection details.' });
    }

    const whList = await this.query('SHOW WAREHOUSES', {});
    if (whList[0].error) {
      return Promise.reject({ message: `Cannot get warehouse list. ${whList[0].rawError}` });
    }
    const whFound = await this.query(
      'SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE UPPER("name") = UPPER(:1)',
      { binds: [warehouse] });
    if (whFound[0].error) {
      return Promise.reject({ message: `Cannot find ${warehouse} warehouse. ${whFound[0].rawError}` });
    }
    if (whFound[0].results.length !== 1) {
      return Promise.reject({ message: `Cannot find ${warehouse} warehouse`})
    }

    const dbList = await this.query('SHOW DATABASES', {});
    if (dbList[0].error) {
      return Promise.reject({ message: `Cannot get database list. ${dbList[0].rawError}` });
    }
    let dbFound: any;
    // Find unquoted databases
    if (db.indexOf('"') === -1) {
      dbFound = await this.query(
        'SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "name" = UPPER(:1)',
        { binds: [db] });
    // Find quoted databases
    } else {
      dbFound = await this.query(
        'SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "name" =:1',
        { binds: [db.replace(/"/g, '')] });
    }
    if (dbFound[0].error) {
      return Promise.reject({ message: `Cannot find ${db} database. ${dbFound[0].rawError}` });
    }
    if (dbFound[0].results.length !== 1) {
      return Promise.reject({ message: `Cannot find ${db.replace(/"/g, '')} database`})
    }

    return this.connection;
  }

  public async close() {
    if (!this.connection) return Promise.resolve();
    const conn = await this.connection;
    this.connection = null;
    conn.destroy();
  }

  public query: (typeof AbstractDriver)['prototype']['query'] = async (query, opt = {}) => {
    const messages = [];
    const { requestId, binds } = opt;
    return this.open()
      .then(async (conn) => {
        const results = await conn.execute(query, binds);
        return results
      })
      .then((results: any[] | any) => {
        if (!Array.isArray(results)) {
          results = [results];
        }
        const cols = [];
        if (results && results.length > 0) {
          for (const colName in results[0]) {
            cols.push(colName)
          }
        }
        
        return [<NSDatabase.IResult>{
          requestId,
          resultId: generateId(),
          connId: this.getId(),
          cols,
          messages: messages.concat([
            this.prepareMessage ([
              `Successfully executed. ${results.length} rows were affected.`
            ].filter(Boolean).join(' '))
          ]),
          query,
          results
        }];
      })
      .catch(err => {
        return [<NSDatabase.IResult>{
          connId: this.getId(),
          requestId,
          resultId: generateId(),
          cols: [],
          messages: messages.concat([
            this.prepareMessage ([
              err.message.replace(/\n/g, ' ')
            ].filter(Boolean).join(' '))
          ]),
          error: true,
          rawError: err,
          query,
          results: []
        }];
      });
  }

  private async getColumns(parent: NSDatabase.ITable): Promise<NSDatabase.IColumn[]> {
    const results = await this.queryResults(this.queries.fetchColumns(parent));
    return results.map(col => ({
      ...col,
      iconName: null,
      childType: ContextValue.NO_CHILD,
      table: parent
    }));
  }

  private async getMaterializedViews(parent): Promise<NSDatabase.ITable[]> {
    const results = await this.queryResults(this.queries.fetchMaterializedViews(parent));
    return results.map(mv => ({
      label: mv.name,
      database: mv.database_name,
      schema: mv.schema_name,
      iconId: 'table',
      type: ContextValue.TABLE,
      isView: true,
    }));
  }

  // Figure out what is supposed to be nested under this item
  private async getFunctions(parent): Promise<NSDatabase.IFunction[]> {
    const results = await this.queryResults(this.queries.fetchFunctions(parent));
    var thing = results.map(func => ({
      name: func.name,
      label: func.name,
      database: func.database_name,
      schema: func.schema_name,
      signature: func.arguments,
      args: func.arguments
              .substring(func.arguments.indexOf("(") + 1, func.arguments.lastIndexOf(")"))
              .split(',')
              .map(arg => { return arg.trim(); }),
      resultType: func.arguments.split('RETURN ')[1],
      detail: func.arguments.substring(func.arguments.indexOf("("), func.arguments.lastIndexOf(")") + 1)
      type: ContextValue.FUNCTION
      childType: ContextValue.NO_CHILD
    }));

    return thing;
  }

  private async getStages(parent: NSDatabase.ISchema): Promise<SnowflakeDatabase.IStage[]> {
    const results = await this.queryResults(this.queries.fetchStages(parent));
    return results.map(stage => ({
      label: stage.name.replace(/"/g, ''),
      database: stage.database_name,
      schema: stage.schema_name,
      type: ContextValue.RESOURCE_GROUP,
      childType: ContextValue.NO_CHILD,
      iconId: 'layers',
      detail: stage.url
    }));
  }

  private async getPipes(parent: NSDatabase.ISchema): Promise<SnowflakeDatabase.IPipe[]> {
    const results = await this.queryResults(this.queries.fetchPipes(parent));
    return results.map(pipe => ({
      label: pipe.name,
      database: pipe.database_name,
      schema: pipe.schema_name,
      type: ContextValue.RESOURCE_GROUP,
      childType: ContextValue.NO_CHILD,
      iconId: 'export'
    }));
  }

  // TODO: Validate that this is correct
  private async getStreams(parent: NSDatabase.ISchema): Promise<SnowflakeDatabase.IStream[]> {
    const results = await this.queryResults(this.queries.fetchStreams(parent));
    return results.map(stream => ({
      label: stream.name,
      database: stream.database_name,
      schema: stream.schema_name,
      type: ContextValue.RESOURCE_GROUP,
      childType: ContextValue.NO_CHILD,
      iconId: 'debug-line-by-line',
      detail: stream.table_name
    }));
  }

  private async getTasks(parent: NSDatabase.ISchema): Promise<SnowflakeDatabase.ITask[]> {
    const results = await this.queryResults(this.queries.fetchTasks(parent));
    return results.map(task => ({
      label: task.name,
      database: task.database_name,
      schema: task.schema_name,
      type: ContextValue.RESOURCE_GROUP,
      childType: ContextValue.NO_CHILD,
      iconId: 'check',
      detail: task.schedule
    }));
  }

  private async getProcedures(parent: NSDatabase.ISchema): Promise<SnowflakeDatabase.IProcedure[]> {
    const results = await this.queryResults(this.queries.fetchProcedures(parent));
    return results.map(proc => ({
      label: proc.name,
      database: parent.database,
      schema: proc.schema_name,
      type: ContextValue.RESOURCE_GROUP,
      childType: ContextValue.NO_CHILD,
      iconId: 'circuit-board',
      detail: proc.arguments.substring(proc.arguments.indexOf("("), proc.arguments.lastIndexOf(")") + 1)
    }));
  }

  private async getFileFormats(parent: NSDatabase.ISchema): Promise<SnowflakeDatabase.IFileFormat[]> {
    const results = await this.queryResults(this.queries.fetchFileFormats(parent));
    return results.map(fileFormat => ({
      label: fileFormat.name.replace(/"/g, ''),
      database: fileFormat.database_name,
      schema: fileFormat.schema_name,
      type: ContextValue.RESOURCE_GROUP,
      childType: ContextValue.NO_CHILD,
      iconId: 'file-code',
      detail: fileFormat.type
    }));
  }

  private async getSequences(parent: NSDatabase.ISchema): Promise<SnowflakeDatabase.ISequence[]> {
    const results = await this.queryResults(this.queries.fetchSequences(parent));
    return results.map(seq => ({
      label: seq.name,
      database: seq.database_name,
      schema: seq.schema_name,
      type: ContextValue.RESOURCE_GROUP,
      childType: ContextValue.NO_CHILD,
      iconId: 'list-ordered',
      detail: seq.next_value.toString()
    }));
  }

  public async testConnection() {
    await this.open();
    const testSelect = await this.query('SELECT 1', {});
    if (testSelect[0].error) {
      return Promise.reject({ message: `Connected but cannot run SQL. ${testSelect[0].rawError}` });
    }
    await this.close();
  }

  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return <NSDatabase.IDatabase[]>[{
          label: this.credentials.database.replace(/"/g, ''),
          database: this.credentials.database,
          type: ContextValue.DATABASE,
          detail: 'database'
        }];
      case ContextValue.DATABASE:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Schemas', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.SCHEMA },
        ];
      case ContextValue.SCHEMA:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Tables', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.TABLE },
          { label: 'Materialized Views', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.MATERIALIZED_VIEW },
          { label: 'Views', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.VIEW },
          { label: 'Stages', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.RESOURCE_GROUP },
          { label: 'Pipes', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.RESOURCE_GROUP },
          { label: 'Streams', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.RESOURCE_GROUP },
          { label: 'Tasks', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.RESOURCE_GROUP },
          { label: 'Functions', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.FUNCTION },
          { label: 'Procedures', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.RESOURCE_GROUP },
          { label: 'File Formats', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.RESOURCE_GROUP },
          { label: 'Sequences', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.RESOURCE_GROUP },
        ];
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.getColumns(item as NSDatabase.ITable);
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
    }
    return [];
  }

  private async getChildrenForGroup({ parent, item }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.childType) {
      case ContextValue.SCHEMA:
        return this.queryResults(this.queries.fetchSchemas(parent as NSDatabase.IDatabase));
      case ContextValue.TABLE:
        return this.queryResults(this.queries.fetchTables(parent as NSDatabase.ISchema));
      case ContextValue.VIEW:
        return this.queryResults(this.queries.fetchViews(parent as NSDatabase.ISchema));
      case ContextValue.MATERIALIZED_VIEW:
        return this.getMaterializedViews(parent as NSDatabase.ISchema);
      case ContextValue.FUNCTION:
        return this.getFunctions(parent as NSDatabase.ISchema);
      case ContextValue.RESOURCE_GROUP:
        switch (item.label) {
          case 'Stages':
            return this.getStages(parent as NSDatabase.ISchema);
          case 'Pipes':
            return this.getPipes(parent as NSDatabase.ISchema);
          case 'Streams':
            return this.getStreams(parent as NSDatabase.ISchema);
          case 'Tasks':
            return this.getTasks(parent as NSDatabase.ISchema);
          case 'Procedures':
            return this.getProcedures(parent as NSDatabase.ISchema);
          case 'File Formats':
            return this.getFileFormats(parent as NSDatabase.ISchema);
          case 'Sequences':
            return this.getSequences(parent as NSDatabase.ISchema);
        }
      }
    return [];
  }

  public async searchItems(itemType: ContextValue, search: string, _extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {
    switch (itemType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        this.queryResults(this.queries.searchTables({ search }));
      case ContextValue.COLUMN:
        return this.queryResults(this.queries.searchColumns({ search, ..._extraParams }));
    }
    return [];
  }

  public getStaticCompletions: IConnectionDriver['getStaticCompletions'] = async () => {
    return {};
  }
}

import AbstractDriver from '@sqltools/base-driver';
import queries from './queries';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0 } from '@sqltools/types';
import { v4 as generateId } from 'uuid';
import { Snowflake } from 'snowflake-promise';

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

  public async testConnection() {
    await this.open();

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
    const dbFound = await this.query(
      'SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE UPPER("name") = UPPER(:1)',
      { binds: [db] });
    if (dbFound[0].error) {
      return Promise.reject({ message: `Cannot find ${db} database. ${dbFound[0].rawError}` });
    }
    if (dbFound[0].results.length !== 1) {
      return Promise.reject({ message: `Cannot find ${db} database`})
    }
    await this.close();
  }

  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return <NSDatabase.IDatabase[]>[{
          label: this.credentials.database,
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
          { label: 'Views', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.VIEW },
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

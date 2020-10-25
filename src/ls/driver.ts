import AbstractDriver from '@sqltools/base-driver';
import queries from './queries';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0 } from '@sqltools/types';
import { v4 as generateId } from 'uuid';

/**
 * set Driver lib to the type of your connection.
 * Eg for postgres:
 * import { Pool, PoolConfig } from 'pg';
 * ...
 * type DriverLib = Pool;
 * type DriverOptions = PoolConfig;
 *
 * This will give you completions iside of the library
 */
type DriverLib = any;
type DriverOptions = any;

export default class SnowflakeDriver extends AbstractDriver<DriverLib, DriverOptions> implements IConnectionDriver {

  public readonly deps: typeof AbstractDriver.prototype['deps'] = [{
    type: AbstractDriver.CONSTANTS.DEPENDENCY_PACKAGE,
    name: 'snowflake-sdk',
    version: '1.5.3'
  }];


  queries = queries;

  private get lib() {
    return this.requireDep('snowflake-sdk');
  }

  public async open() {
    if (this.connection) {
      return this.connection;
    }
    
    let connOptions = {
      account: this.credentials.account,
      username: this.credentials.username,
      password: this.credentials.password
    };
    connOptions = {
      ...connOptions,
      ...(this.credentials["snowflakeOptions"] || {})
    };
    
    try {
      const conn = this.lib.createConnection(connOptions);

      this.connection = new Promise((resolve, reject) => conn.connect(err => {
        if (err) {
          reject(err);
        }
        resolve(conn);
      }));
    } catch (error) {
      return Promise.reject(error);
    }

    return this.connection;
  }

  public async close() {
    if (!this.connection) return Promise.resolve();

    await this.connection.then(conn => conn.disconnect());
    this.connection = null;
  }

  public query: (typeof AbstractDriver)['prototype']['query'] = async (queries, opt = {}) => {
    const db = await this.open();
    const queriesResults = await db.query(queries);
    const resultsAgg: NSDatabase.IResult[] = [];
    queriesResults.forEach(queryResult => {
      resultsAgg.push({
        cols: Object.keys(queryResult[0]),
        connId: this.getId(),
        messages: [{ date: new Date(), message: `Query ok with ${queriesResults.length} results`}],
        results: queryResult,
        query: queries.toString(),
        requestId: opt.requestId,
        resultId: generateId(),
      });
    });
    /**
     * write the method to execute queries here!!
     */
    return resultsAgg;
  }

  public async testConnection() {
    await this.open();
    await this.close()
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * it gets the child items based on current item
   */
  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Tables', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.TABLE },
          { label: 'Views', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.VIEW },
        ];
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        let i = 0;
        return <NSDatabase.IColumn[]>[{
          database: 'fakedb',
          label: `column${i++}`,
          type: ContextValue.COLUMN,
          dataType: 'faketype',
          schema: 'fakeschema',
          childType: ContextValue.NO_CHILD,
          isNullable: false,
          iconName: 'column',
          table: parent,
        },{
          database: 'fakedb',
          label: `column${i++}`,
          type: ContextValue.COLUMN,
          dataType: 'faketype',
          schema: 'fakeschema',
          childType: ContextValue.NO_CHILD,
          isNullable: false,
          iconName: 'column',
          table: parent,
        },{
          database: 'fakedb',
          label: `column${i++}`,
          type: ContextValue.COLUMN,
          dataType: 'faketype',
          schema: 'fakeschema',
          childType: ContextValue.NO_CHILD,
          isNullable: false,
          iconName: 'column',
          table: parent,
        },{
          database: 'fakedb',
          label: `column${i++}`,
          type: ContextValue.COLUMN,
          dataType: 'faketype',
          schema: 'fakeschema',
          childType: ContextValue.NO_CHILD,
          isNullable: false,
          iconName: 'column',
          table: parent,
        },{
          database: 'fakedb',
          label: `column${i++}`,
          type: ContextValue.COLUMN,
          dataType: 'faketype',
          schema: 'fakeschema',
          childType: ContextValue.NO_CHILD,
          isNullable: false,
          iconName: 'column',
          table: parent,
        }];
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
    }
    return [];
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * It gets the child based on child types
   */
  private async getChildrenForGroup({ parent, item }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    console.log({ item, parent });
    switch (item.childType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        let i = 0;
        return <MConnectionExplorer.IChildItem[]>[{
          database: 'fakedb',
          label: `${item.childType}${i++}`,
          type: item.childType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        },{
          database: 'fakedb',
          label: `${item.childType}${i++}`,
          type: item.childType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        },
        {
          database: 'fakedb',
          label: `${item.childType}${i++}`,
          type: item.childType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        }];
    }
    return [];
  }

  /**
   * This method is a helper for intellisense and quick picks.
   */
  public async searchItems(itemType: ContextValue, search: string, _extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {
    switch (itemType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        let j = 0;
        return [{
          database: 'fakedb',
          label: `${search || 'table'}${j++}`,
          type: itemType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        },{
          database: 'fakedb',
          label: `${search || 'table'}${j++}`,
          type: itemType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        },
        {
          database: 'fakedb',
          label: `${search || 'table'}${j++}`,
          type: itemType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        }]
      case ContextValue.COLUMN:
        let i = 0;
        return [
          {
            database: 'fakedb',
            label: `${search || 'column'}${i++}`,
            type: ContextValue.COLUMN,
            dataType: 'faketype',
            schema: 'fakeschema',
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: 'column',
            table: 'fakeTable'
          },{
            database: 'fakedb',
            label: `${search || 'column'}${i++}`,
            type: ContextValue.COLUMN,
            dataType: 'faketype',
            schema: 'fakeschema',
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: 'column',
            table: 'fakeTable'
          },{
            database: 'fakedb',
            label: `${search || 'column'}${i++}`,
            type: ContextValue.COLUMN,
            dataType: 'faketype',
            schema: 'fakeschema',
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: 'column',
            table: 'fakeTable'
          },{
            database: 'fakedb',
            label: `${search || 'column'}${i++}`,
            type: ContextValue.COLUMN,
            dataType: 'faketype',
            schema: 'fakeschema',
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: 'column',
            table: 'fakeTable'
          },{
            database: 'fakedb',
            label: `${search || 'column'}${i++}`,
            type: ContextValue.COLUMN,
            dataType: 'faketype',
            schema: 'fakeschema',
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: 'column',
            table: 'fakeTable'
          }
        ];
    }
    return [];
  }

  public getStaticCompletions: IConnectionDriver['getStaticCompletions'] = async () => {
    return {};
  }
}

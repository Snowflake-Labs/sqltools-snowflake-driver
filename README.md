[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# SQLTools Snowflake Driver

A Visual Studio Code extension which extends the [SQLTools extension](https://marketplace.visualstudio.com/items?itemName=mtxr.sqltools), with a driver to work with the Snowflake Database. It supports tables and views, as well as running queries on Snowflake.

![](https://raw.githubusercontent.com/koszti/sqltools-snowflake-driver/master/resources/readme/preview-sqltools-snowflak-driver.gif)
## Installation

### From the VS Code Marketplace

In the [Snowflake Driver for SQLTools](https://marketplace.visualstudio.com/items?itemName=koszti.snowflake-driver-for-sqltools) VS Code marketplace page, click **Install**.

## Usage

After installing the Snowflake Driver for SQLTools, you will be able to create connections to Snowflake, explore tables and views, and run queries. For more information on how to use SQLTools please refer to [SQLTools extension](https://marketplace.visualstudio.com/items?itemName=mtxr.sqltools)

## What's not implemented:

* **Intellisense auto completion**: Fast code completion has to use the [SHOW commands](https://docs.snowflake.com/en/sql-reference/sql/show-objects.html) to get search results quickly but it requires to run multiple sql queries for each search term. SQLTools not supporting it at the moment.
* **Automatic limit results**: Running `SELECT * FROM big_table` with no limit clause will fetch every row form the table. SQLTools doesn't have abort query function. Please always add `LIMIT 100` to the end of your SQL, otherwise you need to abort the query from the snowflake console.

## To develop

1. Clone this repository and open it in VS Code.
2. Run `npm install` to install dependencies.
3. Press `F5` to start a debuging session. This opens a new VS Code window with the SQLTools Snowflake Driver extension loaded.
   Output from the extension with your local changes shows up in the `Debug Console`. You can set break points, step through
   your code, and inspect variables either in the `Debug` view or the `Debug Console`.

## License

Copyright (c) 2021 MIT License

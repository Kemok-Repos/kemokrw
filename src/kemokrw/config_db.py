COLUMN_TYPES = {
  "numeric": [
    "INT",
    "INTEGER",
    "NUMERIC",
    "MONEY"
  ],
  "text": [
    "CHAR",
    "VARCHAR",
    "CHARACTER VARYING",
    "TEXT",
    "STRING"
  ],
  "datetime": [
    "DATETIME",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMP WITHOUT TIME ZONE"
  ],
  "boolean": [
    "BOOL",
    "BOOLEAN"
  ],
  "other": [
    "BIT"
  ]
}
TABLE_QUERY = "SELECT {columns} FROM {table} {condition} {order};"

TABLE_QUERY_MAX = "WITH particion AS  " \
                  "(select {key} as valor " \
                  "FROM {table} order by 1 offset {offset} limit {limit}) " \
                  "select max(valor)" \
                  "from particion;"

TABLE_DATE_YEAR = "SELECT string_agg(distinct TO_CHAR({key},'YYYY'),',') as years " \
                   "FROM {table} WHERE TO_CHAR({key},'YYYY') {oper} '{year}';"

TABLE_DATE_MONTH = "SELECT string_agg(distinct TO_CHAR({key},'{patern}'),',') as months " \
                   "FROM {table} WHERE TO_CHAR({key},'YYYY')='{year}' and TO_CHAR({key},'MM') {oper} '{month};'"

TABLE_CHECK = {
  "check_rows": "SELECT COUNT(*) FROM {table} {condition};"
}
TABLE_CHECK_LIMIT = {
  "check_rows": "SELECT COUNT(*) FROM {table} {condition} {limit};"
}

COLUMN_QUERY = {
  "postgresql": "select column_name, data_type from information_schema.columns where table_name = '{table}';",
  "mssql": "SELECT c.name column_name, t.Name data_type FROM sys.columns c INNER JOIN sys.types t ON c.user_type_id = t.user_type_id WHERE c.object_id = OBJECT_ID('{table}')"
}

COLUMN_CHECK = {
  "postgresql": {
    "numeric": {
      "check_sum": "SELECT ROUND(SUM(COALESCE({column}::numeric,0)),0) FROM {table} {condition} {order};",
      "check_nn": "SELECT COUNT({column}) FROM {table} {condition} {order};"
    },
    "text": {
      "check_hash": "SELECT md5(string_agg({column},'' {order})) FROM {table} {condition};",
      "check_nn": "SELECT COUNT({column}) FROM {table} {condition} {order};"
    },
    "datetime": {
      "check_nn": "SELECT COUNT({column}) FROM {table} {condition} {order};"
    },
    "boolean": {
      "check_true": "SELECT COUNT(NULLIF({column},False)) FROM {table} {condition}{order};",
      "check_nn": "SELECT COUNT({column}) FROM {table} {condition} {order};"
    },
    "other": {
      "check_nn": "SELECT COUNT({column}) FROM {table} {condition} {order}"
    }
  },
  "mssql": {
    "numeric": {
      "check_sum": "SELECT ROUND(SUM(COALESCE({column},0)),0) FROM {table} {condition};",
      "check_nn": "SELECT COUNT({column}) FROM {table} {condition};"
    },
    "text": {
      "check_nn": "SELECT COUNT({column}) FROM {table} {condition};"
    },
    "datetime": {
      "check_nn": "SELECT COUNT({column}) FROM {table} {condition};"
    },
    "boolean": {
      "check_true": "SELECT COUNT(NULLIF({column},False)) FROM {table} {condition};",
      "check_nn": "SELECT COUNT({column}) FROM {table} {condition};"
    },
    "other": {
      "check_nn": "SELECT COUNT({column}) FROM {table} {condition};"
    }
  },
  "pandas": {
    "numeric": {
      "check_sum": "sum",
      "check_nn": "count"
    },
    "text": {
      "check_nn": "count"
    },
    "datetime": {
      "check_nn": "count;"
    },
    "boolean": {
      "check_true": "count",
      "check_nn": "count"
    },
    "other": {
      "check_nn": "count"
    }
  }
}
DB_COLLATION = {
  "postgresql": {"lc_monetary":"show lc_monetary"
                }
  }
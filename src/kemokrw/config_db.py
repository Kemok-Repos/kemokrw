COLUMN_TYPES = {
  "numeric": [
    "INT",
    "INTEGER",
    "NUMERIC"
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

TABLE_CHECK = {
  "check_rows": "SELECT COUNT(*) FROM {table} {condition};"
}

COLUMN_QUERY = {
  "postgresql": "select column_name, data_type from information_schema.columns where table_name = '{table}';",
  "mssql": "SELECT c.name column_name, t.Name data_type FROM sys.columns c INNER JOIN sys.types t ON c.user_type_id = t.user_type_id WHERE c.object_id = OBJECT_ID('{table}')"
}

COLUMN_CHECK = {
  "postgresql": {
    "numeric": {
      "check_sum": "SELECT ROUND(SUM(COALESCE({column},0)),0) FROM {table} {condition};",
      "check_nn": "SELECT COUNT({column}) FROM {table} {condition};"
    },
    "text": {
      "check_hash": "SELECT md5(string_agg({column},'')) FROM {table} {condition};",
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
  }
}
COLUMN_TYPES = {
  "numeric": [
    "INT",
    "INTEGER",
    "NUMERIC",
    "FLOAT",
    "BIGINT"
  ],
  "text": [
    "CHAR",
    "VARCHAR",
    "CHARACTER VARYING",
    "TEXT",
    "STRING",
    "STR"
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
    "BIT",
    "DICT",
    "JSON",
    "JSONB"
  ]
}

QUERY_MODEL = 'SELECT configuracion FROM maestro_de_modelos WHERE id = {0} LIMIT 1;'

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
    "BIT"
  ]
}

HUBSTAFF = {
    "by_id": ["users"],
    "by_organization": ["projects", "activities", "aplication_activities", "url_activities"],
    "by_project": [],
    "users": {
        "base_url": "https://api.hubstaff.com/v2/users/{id}",
        "key": "user",
        "model": {
            'col1': {
                'name': 'id',
                'type': 'int32'
                },
            'col2': {
                'name': 'name',
                'type': 'str'
                },
            'col3': {
                'name': 'email',
                'type': 'str'
                },
            'col4': {
                'name': 'time_zone',
                'type': 'str'
            },
            'col5': {
                'name': 'status',
                'type': 'str'
            },
            'col6': {
                'name': 'created_at',
                'type': 'datetime64'
            },
            'col7': {
                'name': 'updated_at',
                'type': 'datetime64'
            }
        }
    },
    "organizations": {
        "base_url": "https://api.hubstaff.com/v2/organizations",
        "key": "organizations",
        "model": {}
    },
    "projects": {
        "base_url": "https://api.hubstaff.com/v2/organizations/{organization_id}/projects",
        "key": "projects",
        "model": {
            'col1': {
                'name': 'id',
                'type': 'int32'
            },
            'col2': {
                'name': 'name',
                'type': 'str'
            },
            'col3': {
                'name': 'description',
                'type': 'str'
            },
            'col4': {
                'name': 'created_at',
                'type': 'datetime64'
            },
            'col5': {
                'name': 'updated_at',
                'type': 'datetime64'
            },
            'col6': {
                'name': 'status',
                'type': 'str'
            },
            'col7': {
                'name': 'client_id',
                'type': 'numeric'
            },
            'col8': {
                'name': 'billable',
                'type': 'boolean'
            },
            'col9': {
                'name': 'budget',
                'type': 'str'
            },
            'col10': {
                'name': 'metadata',
                'type': 'str'
            }
        }
    },
    "activities": {
        "base_url": "https://api.hubstaff.com/v2/organizations/{organization_id}/activities",
        "key": "activities",
        "model": {}
    },
    "aplication_activities": {
        "base_url": "https://api.hubstaff.com/v2/organizations/{organization_id}/application_activities",
        "key": "applications",
        "model": {}
    },
    "url_activities": {
        "base_url": "https://api.hubstaff.com/v2/organizations/{organization_id}/url_activities",
        "key": "urls",
        "model": {}
    }
}

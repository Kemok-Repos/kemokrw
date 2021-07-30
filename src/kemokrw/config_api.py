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
    "BIT",
    "DICT"
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
                'type': 'numeric'
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
                'type': 'dict'
            },
            'col10': {
                'name': 'metadata',
                'type': 'dict'
            }
        }
    },
    "activities": {
        "base_url": "https://api.hubstaff.com/v2/organizations/{organization_id}/activities",
        "key": "activities",
        "model": {
            'col1': {
                'name': 'id',
                'type': 'int32'
            },
            'col2': {
                'name': 'date',
                'type': 'datetime64'
            },
            'col3': {
                'name': 'created_at',
                'type': 'datetime64'
            },
            'col4': {
                'name': 'updated_at',
                'type': 'datetime64'
            },
            'col5': {
                'name': 'time_slot',
                'type': 'str'
            },
            'col6': {
                'name': 'starts_at',
                'type': 'str'
            },
            'col7': {
                'name': 'user_id',
                'type': 'numeric'
            },
            'col8': {
                'name': 'project_id',
                'type': 'numeric'
            },
            'col9': {
                'name': 'task_id',
                'type': 'numeric'
            },
            'col10': {
                'name': 'keyboard',
                'type': 'numeric'
            },
            'col11': {
                'name': 'mouse',
                'type': 'numeric'
            },
            'col12': {
                'name': 'overall',
                'type': 'numeric'
            },
            'col13': {
                'name': 'tracked',
                'type': 'numeric'
            },
            'col14': {
                'name': 'billable',
                'type': 'boolean'
            },
            'col15': {
                'name': 'paid',
                'type': 'boolean'
            },
            'col16': {
                'name': 'client_invoiced',
                'type': 'boolean'
            },
            'col17': {
                'name': 'team_invoiced',
                'type': 'boolean'
            },
            'col18': {
                'name': 'immutable',
                'type': 'boolean'
            },
            'col19': {
                'name': 'timesheet_id',
                'type': 'numeric'
            },
            'col20': {
                'name': 'timesheet_locked',
                'type': 'boolean'
            },
            'col21': {
                'name': 'time_type',
                'type': 'str'
            },
            'col22': {
                'name': 'client',
                'type': 'str'
            }
        }
    },
    "aplication_activities": {
        "base_url": "https://api.hubstaff.com/v2/organizations/{organization_id}/application_activities",
        "key": "applications",
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
                'name': 'date',
                'type': 'datetime64'
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
                'name': 'time_slot',
                'type': 'str'
            },
            'col7': {
                'name': 'user_id',
                'type': 'numeric'
            },
            'col8': {
                'name': 'project_id',
                'type': 'numeric'
            },
            'col9': {
                'name': 'task_id',
                'type': 'numeric'
            },
            'col10': {
                'name': 'tracked',
                'type': 'numeric'
            },
            'col11': {
                'name': 'activations',
                'type': 'numeric'
            }
        }
    },
    "url_activities": {
        "base_url": "https://api.hubstaff.com/v2/organizations/{organization_id}/url_activities",
        "key": "urls",
        "model": {
            'col1': {
                'name': 'id',
                'type': 'int32'
            },
            'col2': {
                'name': 'site',
                'type': 'str'
            },
            'col3': {
                'name': 'date',
                'type': 'datetime64'
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
                'name': 'time_slot',
                'type': 'str'
            },
            'col7': {
                'name': 'user_id',
                'type': 'numeric'
            },
            'col8': {
                'name': 'project_id',
                'type': 'numeric'
            },
            'col9': {
                'name': 'task_id',
                'type': 'numeric'
            },
            'col10': {
                'name': 'tracked',
                'type': 'numeric'
            },
            'col11': {
                'name': 'details',
                'type': 'str'
            }
        }
    }
}

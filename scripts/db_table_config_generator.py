
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import SQLConnector
import polars as pl
import pyperclip

'''
This file generates the neccessary entry in TABLES (config/settings.py) for checked_upsert to function
'''


dbc = 'db_CentralStore'
acudb = 'AcumaticaDb'
query = '''

with TopLevel as(
select s.schema_id SchemaID
     , t.object_id TableID
     , c.column_id ColumnID
     , s.name sName
     , t.name tName
     , c.Name cName
     , case when k.object_id is not null then 'Key' 
            when c.is_nullable = 0 then 'Not Null (Maybe key)'
       else 'Update' end ColumnType
     , row_number() over(partition by s.schema_id, t.object_id, c.column_id order by case when k.object_id is not null then 'Key' when c.is_nullable = 0 then 'Not Null (Maybe key)' else 'Update' end ) rownum
from sys.schemas s
inner join sys.tables t on s.schema_id = t.schema_id
inner join sys.columns c on t.object_id = c.object_id
left join sys.index_columns k on t.object_id = k.object_id and c.column_id = k.column_id 
where s.name = '{schema}' and t.name = '{table}'
)
select t.SchemaID
     , t.TableID
     , t.ColumnID
     , t.sName
     , t.tName
     , t.cName
     , t.ColumnType
from TopLevel t
where rownum = 1
'''

db_input = input('Enter db name or at least first 2 characters: ').lower()

try:
    is_str = str(db_input)
except Exception as e:
    print("That's not a string!")
    db_input = input('Enter db name or at least first 2 characters: ').lower()
    
input_len = len(db_input)
if db_input == dbc[0:input_len]:
    db = dbc
elif db_input == acudb[0:input_len]:
    db = acudb

db = SQLConnector('config-generator', db)

if type(db) != str:
    print('Connected!')
    name = input('Enter fully qualified table name: ')
    if '.' not in name:
        print('Error! Not fully qualified')
        name = input('Enter fully qualified table name: ')
    else:
        sname = name.split('.')
        schema = sname[0]
        table = sname[1]
        config_results = db.query_db(query.format(schema=schema, table=table))
        results = {
            name: {}
        }
        results[name]['keys'] = [value['cName'] for value in config_results.sql("select cName from self where ColumnType in('Key', 'Not Null (Maybe key)')").to_dicts()]
        results[name]['columns'] = [row['cName'] for row in config_results.iter_rows(named=True)]
        results[name]['update_columns'] = [value['cName'] for value in config_results.sql("select cName from self where ColumnType = 'Update'").to_dicts()]
        bp = 'here'

        def format_entry(table_name, cfg):
            lines = [f"    '{table_name}': {{"]
            for section in ('keys', 'columns', 'update_columns'):
                lines.append(f"        '{section}': [")
                for item in cfg[section]:
                    lines.append(f"            '{item}',")
                lines.append("        ],")
            lines.append("    },")
            return '\n'.join(lines)

        pyperclip.copy(format_entry(name, results[name]))
        bp = 'here'
else:
    bp = 'error'



bp = 'here'

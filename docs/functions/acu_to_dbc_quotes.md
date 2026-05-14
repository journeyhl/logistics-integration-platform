# acu_to_dbc_quotes
Gets all QT type Sales Orders from AcumaticaDb that were modified within the last day and loads them to **acu.Quotes**

## Schedule
- ### :00, :30

## Execution Behavior
Executes single pipeline, **AcuToDbcQuotes**

## Pipelines

### AcuToDbcQuotes
#### `AcuToDbcQuotes` Pipeline Documentation — [pipelines/acu_to_dbc_quotes.py](../../pipelines/acu_to_dbc_quotes.py)
```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([acu_to_dbc_quotes]) --> B[AcuToDbcQuotes.__init__]
    B --> B1[inherits Pipeline]
    
    A--> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(
        <b><i>AcuDb</i></b>
        AcuToDbc_Quotes: Query)]

    RUN --> TR[transform]
    TR --> T1[fill null LineNbr with 99]
    TR --> T2[convert to list of dicts]

    RUN --> LD[load]
    LD --> L1[(
        <b><i>CentralStore</i></b>
        upsert acu.Quotes
    )]

    RUN --> LOGS[(
        <b><i>CentralStore</i></b>
        _util.Logs<br/>insert run logs
    )]
```


## Queries
### AcumaticaDb
 - #### [AcuToDbc_Quotes.sql](../../sql/queries/AcumaticaDb/AcuToDbc_Quotes.sql)
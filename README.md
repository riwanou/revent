# Revent

Utilities to manage elastic search events between differents instances.

## Usage

```bash
mkdir events/02-11-2023
# fetch events from ip-source, limit to 1 million per index (l flag), output to specified directory
./revent fetch -o events/02-11-2023 -u http://[ip-source]:9200 -l 1000000
# push events from specified directory to ip-target  
./revent push -i events/02-11-2023 -u http://[ip-target]:9200
```

## Fetch

Fetch events from an ElasticSearch api and store them.

**file format**

```json
{
  "indexName": "index name",
  "indexData": {},
  "eventsFetchLimit": 10,
  "eventsNb": 100,
  "events": [
    {},
    {},
    {}
  ]
}
```

## Push

Push events from events files to an ElasticSearch api.

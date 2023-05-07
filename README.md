# Revent

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

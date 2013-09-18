# Metadata Harvester

[![Build Status](https://travis-ci.org/platzhirsch/metadata-harvester.png)](http://travis-ci.org/platzhirsch/metadata-harvester)

A harvesting framework for accumulating different metadata.

## Getting Started

The Metadata Harvester uses Sidekiq and ElasticSearch, hence a running Redis and ElasticSearch server is required. If that is given, launch Sidekiq with the harvester module like so:

```
$ sidekiq -r ./lib/harvester.rb
```

Then the harvest process is started by:

```
$ ruby lib/mdh.rb
```

All sources listed in the **repositories.yml** will be harvested and indexed.

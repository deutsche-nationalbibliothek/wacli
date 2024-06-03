# Get Archived Webpages

This tool gets Archived Webpages from the DNB Catalog.
It queries for items (*snapshots*) that are part of (`dcterms:isPartOf`) [online resource](http://rdaregistry.info/termList/RDACarrierType/1018) media.

This projects uses [poetry](https://python-poetry.org/) (maybe in the [rye](https://rye.astral.sh/) future).

## Installation

```
$ poetry install
```

## Run

First you need to set the environment variables:

```
$ source default.env
```

List random IDNs for snapshots

```
$ poetry run wacli list
```

# Manage the Webarchive

This tool is a command line interface to manage the web archive setup at the German National Library (DNB).

It's tasks are:
- get meta-data of archived webpages from the catalog graph
- retrieve archive files from the repository
- execute the indexers to make the archived webpages available

## Get Archived Webpages

This tool gets archived webpages from the DNB catalog graph.
It queries for items (*snapshots*) that are part of (`dcterms:isPartOf`) [online resource](http://rdaregistry.info/termList/RDACarrierType/1018) media.

# Requirements and Usage

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

List IDNs of the snapshots

```
$ poetry run wacli list
```

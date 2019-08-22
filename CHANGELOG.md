# Changelog
All notable changes to this project should be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2019-08-22
### Added
- `http`: Rewrite specific URL patterns to DBpedia resource URIs:
  - `http://<locale>.dbpedia.org/page/<slug>`
  - `https?://<locale>.wikipedia.org/wiki/<slug>`
- `http`: Allow lookup of multiple URI clusters at once with the `uris` parameter.
- `http`: log in the Uvicorn logger that app startup is ready.

### Changed
- Install the latest RocksDB release from GitHub archive instead of git (see [#6](https://github.com/dbpedia/dbp-same-thing-service/issues/6)).
- Update python to v3.7 and install the latest version of python packages.
- Added `mypy` to development dependencies.

## [0.3.3] - 2019-05-07
### Fixed
- `db`: lock contention (`RocksIOError`, see [#8](https://github.com/dbpedia/dbp-same-thing-service/issues/8)), fixed by reusing the admin DB connection in `restore.create_backup()`.

## [0.3.2] - 2019-05-06
### Added
- `http`: purge old data DBs on Gunicorn/Uvicorn initialization.

### Changed
- `http`: let Gunicorn preload the app (instead of per worker).

## [0.3.1] - 2019-05-05
### Added
- `loader`: ability to restore from backup by registering them in the admin DB.
- `loader`: CLI to interactively select a backup to restore.

### Changed
- `loader`: hot-swap DB when the current DB never completed loading.

### Fixed
- `db.get_connection()`: RocksDB API change.

## [0.3.0] - 2019-05-01
### Changed
- `loader`: find and fetch latest snapshot via Databus SPARQL query (see [#5](https://github.com/dbpedia/dbp-same-thing-service/issues/5)).

## [0.2.2] - 2019-03-23
### Changed
- `http`: Gunicorn performance tuning.
- `loader`: RocksDB performance tuning.

## [0.2.1] - 2018-12-06
### Changed
- `http`: move `meta` info to its own top-level key.
- Docs: updated readme.

### Fixed
- `loader`: don't overlook decompressed chunks that are smaller than one line.
- `loader`: only split on the secondary seperator once.

## [0.2.0] - 2018-12-05
### Added
- Docs: deployment and maintenance notes.
- `parser`: parse RDF literals from ntriple lines.
- `loader`: progress bars.
- `http`: include singleton IDs and meta info in response (see [#1](https://github.com/dbpedia/dbp-same-thing-service/issues/1)).

### Changed
- Adjust DBp Global namespace to global.dbpedia.org/id/.
- `loader`: only load and backup if a new snapshot is available.
- `loader`: load compressed TSV snapshot instead of ntriples part files.
- `loader`: separate concerns with producer/consumer pattern.
- `http`: replace ApiStar with Starlette.
- Restructured Docker configuration.

### Fixed
- pinned python version at 3.6.
- `loader`: close the event loop after load and backup are done.

## [0.1.2] - 2018-06-24
### Added
- Docs: installation and usage notes.

### Changed
- `loader`: only load and backup if new parts are available.

### Fixed
- Docker image path.
- `loader`: close the event loop after load and backup are done.

## [0.1.1] - 2018-06-23
### Changed
- `http`: run with Gunicorn server in production.
- `loader`: don't attempt manual compaction (prone to stalling).

### Fixed
- `loader`: admin part key encoding.

## [0.1.0] - 2018-06-23
### Added
- URI cluster `loader` from Spark output.
- `http` interface based on ApiStar.

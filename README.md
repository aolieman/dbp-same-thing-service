# DBpedia Same Thing Service
Microservice that looks up global and local IRIs based on the most recent [DBpedia ID Management](http://dev.dbpedia.org/ID%20and%20Clustering) release.

Query the DBpedia Same Thing Service with the IRI of a Wikidata or DBpedia entity (more coming soon) and it will return the current DBpedia global IRI for the queried IRI and all known 'local' IRIs which are considered to be the same thing. The 'local' IRIs for a global id are the members of the cluster represented/identified by the global id. The members of a cluster are assigned by the [DBpedia ID Management Cluster algorithm](http://dev.dbpedia.org/ID%20and%20Clustering) based on transitive closure of sameAs links. Querying a global ID will also show all the members of the according cluster.

## Acknowledgements
The microservice has been developed and contributed by Alex Olieman ([@aolieman](https://github.com/aolieman)).

## Usage 
You can query the experimental service deployed within the DBpedia Association infrastructure

[http://downloads.dbpedia.org/same-thing/lookup/?uri=http://www.wikidata.org/entity/Q31910788](http://downloads.dbpedia.org/same-thing/lookup/?uri=http://www.wikidata.org/entity/Q31910788) 
 
 or setup your local instance based on the latest DBpedia ID Management Dump. The service is based on simple HTTP requests, whereas the uri parameter contains either the local or global IRI of interest.

`curl "http://localhost:8027/lookup/?uri=http://www.wikidata.org/entity/Q8087"`
```
{
  "global": "https://global.dbpedia.org/id/4y9Et",
  "locals": [
    "http://dbpedia.org/resource/Geometry",
    "http://de.dbpedia.org/resource/Geometrie",
    "http://fr.dbpedia.org/resource/Géométrie",
    "http://nl.dbpedia.org/resource/Meetkunde",
    "http://sv.dbpedia.org/resource/Geometri",
    "http://www.wikidata.org/entity/Q8087"
  ]
}
```
Percent-encoding of the `uri` parameter is optional. If this example does not work when running the service locally, check if which port is specified in `docker-compose.yml`.

## Local Deployment
The microservice is shipped as docker compose setup.
### Requirements
- Installed docker and [Docker Compose](https://docs.docker.com/compose/install/)
### Running from pre-compiled docker image
- Download the compose file: `wget https://raw.githubusercontent.com/dbpedia/dbp-same-thing-service/master/docker-compose.yml`
- Run: `docker-compose up`

This will download the latest image and runs two containers: the `loader` and the webserver (`http`). The port on which the webserver listens is configurable in the compose file.

When running multiple containers in this way, the loading progress can unfortunately not be displayed. To monitor the progress of the (initial) Global ID snapshot downloading and ingestion, instead run the following:
- `docker-compose run loader`

After the loader is finished, both containers may be run with `docker-compose up`.

### Bulk Loading
The `loader` downloads the latest Global ID release from `downloads.dbpedia.org` and proceeds to load any source files that haven't been loaded yet into the database. This might take a while to complete. After all data is loaded, a backup is made and the loader stops running. 

On subsequent restarts of the loader container (with docker-compose up or docker start command) the loader will check if a new snapshot release is available on the download server, remove old cached downloads, and load the new ID release into a fresh database. 

### Update, Maintenance, & Zero Downtime Features
#### Initial loading
On the first start, the webserver  (`http` container) waits for the database to initialize. Only on the first run, it will have to wait until all source files have been downloaded, and will start listening for requests once the (empty) database has been created. While files are being loaded, the service will respond to requests, but will return `404` for any URI that hasn't been loaded yet. 

#### Update to a new release
If a new dataset is available just rerun `docker-compose up` or use `docker-compose run loader`. The  `loader` will discover the new release, download it, and start to create a new database version. The running webserver however, will not be affected during the download and update process. It will keep serving requests from the already existing fully-loaded database, and will not switch to the newer database while it is running. The next time the `http` container is booted it will use the most recent database (which is typically the one that was latest to download).

#### Database versioning (Backup) and Rollback
All database versions will stay in the `dbdata` volume until they are manually removed. 
This allows to switch back to older database version at any point of time (e.g. in case the loading of the latest release was interrupted and lead to an inconsistent state.
In order to do so
- get the mountpoint of the `dbdata` volume: `docker volume inspect dbp-same-thing-service_dbdata`
- `cd` into the mountpoint directory 
- `touch` the folder of the database version you would like to 'restore'
- restart the `http` container

After restarting the `http` container, it will use the database folder you `touched` (i.e. the database folder with the most recent timestamp).
**In order to reduce storage consumption, the database versions should be cleaned occasionally.**

### Development Setup
In case you would like to modify the behavior of your local instance (by modifying python files) or to contribute enhancements to this project, you can build your own docker image. In order to do so:
- Clone or fork this repository
- `docker-compose -f docker-compose.yml -f docker-compose.dev.yml up`
- any changes to the webapp code will trigger a live reload

We use [pipenv](https://docs.pipenv.org/) for package management. Set up a local python environment by running `pipenv install` from the project root. Introduce new dependencies with `pipenv install <package_name>` and ensure that the latest `Pipfile.lock` (generated with `pipenv lock`) is included in the same commit as an updated `Pipfile`.

After making any changes other than to python source files, rebuild the image with `docker-compose -f docker-compose.yml -f docker-compose.dev.yml build`. The compose file automatically builds the image if none exists, but will not rebuild after changes when using `docker-compose up`.

## Troubleshooting 

If the pre-compiled version of the embedded RocksDB does not work on your CPU architecture (e.g. virtual machine, or older AMD), this is likely due to an optimization that depends on an instruction set which is not available on your CPU. To build your own image that runs with production settings, follow these steps:
- Clone this repository
- Run `docker-compose up`

This works because the `docker-compose.override.yml` file is automatically applied, which specifies a local image instead of the one from Docker Hub. To rebuild the image, e.g. after updating with `git pull`, run `docker-compose build`.

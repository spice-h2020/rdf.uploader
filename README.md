# RDF Uploader

The RDF Uploader aims at transforming and uploading JSON documents shared via Object Stream API.

### Compilation

You can compile RDF Uploader using maven as follows:

```
git clone https://github.com/spice-h2020/rdf.uploader.git
cd rdf.uploader/
mvn clean install
```

Before running RDF Uploader make sure that [SPARQL Anything](https://github.com/SPARQL-Anything/sparql.anything) v0.6.0 is installed on your machine.

### Configuration

Before running the RDF Uploader set the configuration file available at ``src/main/resources/config.properties`` 

```

## Username and password of the datahub administrator
username=datahub-admin
password=DATAHUB1234567890

## Host exposing APIFactory APIs
apif_host=spice-apif.local
apif_uri_scheme=http

## Path for querying the activity_log 
activity_log_path=/object/activity_log

## Namespace to use as base for the resources retrieved from the activity_log dataset
baseNS=http://spice-apif.local/object/activity_log/

## URL of the Blazegraph repository
repositoryURL=http://localhost:9999/blazegraph

## Path of the file containing the properties for building Blazegraph's repositories
blazegraphPropertiesFilepath=src/main/resources/blazegraph.properties

## Prefix used for generating the RDF resources
baseResource=https://w3id.org/spice/resource/

## Prefix used for generating the URIs of the graphs
baseGraph=${baseResource}graph/

## Prefix used for generating the URIs of the ontology entities
ontologyURIPRefix=https://w3id.org/spice/ontology/

## Set useNamedresources as true to generate named resources from JSON documents instead of blank nodes
useNamedresources=true

## Maximum number of upload/update/delete requests that will be kept in memory
requestQueueSize=100

## Number of seconds between two API factory lookups
lookupRateSeconds=10

## Path to a temporary folder that will be created
tmpFolder=tmp

## Path to file containing the timestamp of the last request accomplished
lastTimestampFile=${tmpFolder}/timestamp

## Prefix of the blazegraph namespaces that will be created
blazegraphNamespacePrefix=spice_

## UUID of the dataset used for sending commands to the uploader
uuidDatasetCommands=cf0392e1-1340-4024-90eb-d9e0f5120cfd

## UUID of the dataset used for sending commands to the uploader
rdf_jobs_dataset=spice_rdfjobs

## Query defined according SPARQL Anything conventions for transforming files
sa_query_filepath=src/main/resources/saQuery.sparql

```

### Usage

You can run RDF Uploader using maven as follows:

```
mvn exec:java -Dexec.mainClass="eu.spice.rdfuploader.RDFUploader"
```

Or, you can run the JAR file ``spice.rdf-uploader.jar`` located in the ``target/`` directory as follows

```
java -jar spice.rdf-uploader.jar
```

RDF Uploader assumes that the configuration file is located at ``./config.properties``.

### License

RDF Uploader is distributed under [Apache 2.0 license](LICENSE)

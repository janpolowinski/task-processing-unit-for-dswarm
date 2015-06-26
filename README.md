---

![TU Dortmund Logo](http://www.ub.tu-dortmund.de/images/tu-logo.png)

![UB Dortmund Logo](http://www.ub.tu-dortmund.de/images/ub-schriftzug.jpg)

---
(in cooperation with [SLUB Dresden](http://slub-dresden.de) + [Avantgarde Labs](http://avantgarde-labs.de))

# Task Processing Unit for [D:SWARM](http://dswarm.org)

[![Join the chat at https://gitter.im/dswarm/dswarm](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/dswarm/dswarm?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

The task processing unit (TPU) is intented to process large amounts of data via [tasks](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#task) that make use of the [mappings](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#mapping) that you have prepared and tested with the [D:SWARM backoffice webgui](https://github.com/dswarm/dswarm-documentation/wiki/Overview). So it can act as the production unit for D:SWARM, whereby the backoffice acts as development and/or testing unit (on smaller amounts of data).

## TPU Task

A TPU task can consist of three parts, where by each part can be optional. These are:
* ```ingest```: transforms data from a [data resource](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#data-resource) (of a certain data format, e.g. XML) with help of a [configuration](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#configuration) into a [data model](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#data-model) that makes use of a [generic data format](https://github.com/dswarm/dswarm-documentation/wiki/Graph-Data-Model) (so that it can be consumed by the [transformation engine](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#transformation-engine) of D:SWARM)
* ```transform```: transforms data from an input data model via a task (refers to a [job](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#job)) into an output data model
* ```export```: transforms data from a data model (mainly output data model) into a certain data format, e.g. XML

## Processing Scenarios

The task processing unit can be configured for various scenarios, e.g.,
* ```ingest``` (only; persistent in the [data hub](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#data-hub))
* ```export``` (only; from data in the data hub)
* ```ingest``` (persistent), ```transform```, ```export``` (from persistent result)
* ```on-the-fly transform``` (input data will be ingested (/generated) on-the-fly + export data will be directly returned from the transformation result (without storing it in the data hub)
* any combination of the previous scenarios ;)

The fastest scenario is ```on-the-fly transform```, since it doesn't store anything in the data hub and does only the pure data processing. So it's recommend for data transformation scenarios, where only the output is important, but not the archiving of the data. Currently, this scenario only supports XML export. So if you would like to have an RDF export of your transformed data, then you need to run the TPU with the parameter for persisting the task execution result in the data hub (since RDF export is only implement from there at the moment).






Die *Task Processing Unit* geht von folgenden Annahmen aus:

* Es gibt innerhalb der D:SWARM-Plattform ein Projekt, welches "repräsentativ" ein Mapping für eine größere Menge von Quelldateien konfiguriert.
* Die im Prozess erzeugten *Resources* und *Data Models* zu den Quellen werden nach - erfolgreicher aber auch nach nicht erfolgreicher - Transformation aus der Plattform gelöscht (verhindert "Aufblähen" der Listen im Bereich "Data" des WebUI).

## Konfiguration eines Proezesses

Für die Konfiguration eines Prozesses müssen folgende Parameter in der `config.properties` angepasst werden:

	project.name=CrossRef
	
	# resources
	resource.watchfolder=data/sources
	resource.preprocessing=true
	
	# preprocessing for xml files
	preprocessing.xslt=xslt/cdata.xsl
	preprocessing.folder=data/tmp
	
	# prototype project
	prototype.dataModelID=bbd368e8-b75c-0e64-b96a-ab812a700b4f
	prototype.projectID=819f2f6e-98ed-90e2-372e-71a0a1eec786
	prototype.outputDataModelID=DataModel-cf998267-392a-4d87-a33a-88dd1bffb016
	
	# results
	results.persistInDMP=false
	results.persistInFolder=true
	results.folder=data/results

## Ausführen eines Prozesses

	$JAVA_HOME/jre/bin/java -cp TaskProcessingUnit-1.0-SNAPSHOT-onejar.jar de.tu_dortmund.ub.data.dswarm.TaskProcessingUnit -conf=conf/config.properties
  

## Algorithmus

### Gegeben

* uuid des Datenmodells zum "Prototyp"-Projekts
* uuid des "Prototyp"-Projekts
* uuid des Zielschemas

### Aufgabe

Transformiere jede Datei aus einem definierten Quellverzeichnis mittels des Mappings eines ausgewählten "Prototyp"-Projekts
in das ausgewählte Zielschema und speichere die Resultate in ein definiertes Zielverzeichnis

### Verfahren

**1. Schritt:** Erzeuge für jede Quelldatei eine *InputDataModell*

* (a) Upload der Datei via `POST {engine.dswarm.api}/resources/`; ggf. vorher *Preprocessing*
* (b) Ermitteln der ID zur Ressource zum Datenmodells zum "Prototyp"-Projekts via `GET {engine.dswarm.api}/datamodels/{uuid des Datenmodels zum "Prototyp"-Projekt}`
* (c) Lese die Konfiguration der Ressource zum Datenmodells zum "Prototyp"-Projekts via `GET {engine.dswarm.api}/resources/{uuid der "Prototyp"-Ressource}/configurations`
* (d) Konfiguration der Datei mit angepassten Daten via `POST {engine.dswarm.api}/resources/{uuid der neuen Ressource}/configurations`
* (e) Definition des Datenmodells via `POST {engine.dswarm.api}/datamodels`

**2. Schritt:** Erzeuge für jede Quelldatei ein *Task*

* (a) Hole aus dem ausgwählten "Prototyp"-Projekt die Informationen zum Mapping
* (b) hole die Konfiguration zum *InputDataModell* mittels `GET {engine.dswarm.api}/datamodels/{uuid}`
* (c) Hole die Konfiguration zum ausgewählten Zielschema mittels `GET {engine.dswarm.api}/datamodels/{uuid}`
* (d) Baue den *Task* zusammen

*Task* JSON:

	{
	  "name" : "my task",
	  "description" : "my task description",
	  "job" : {
	    "mappings" : [[[[INSERT HERE THE MAPPINGS ARRAY FROM YOUR PROJECT]]]],
	    "uuid" : "[[[[INSERT HERE A UUID]]]]"
	  },
	  "input_data_model" : [[[[INSERT HERE THE INPUT DATA MODEL RETRIEVED FROM THE DATA MODELS ENDPOINT]]]],
	  "output_data_model" : [[[[INSERT HERE THE OUTPUT DATA MODEL ((OPTIONALLY) RETRIEVED FROM THE DATA MODELS ENDPOINT)]]]]
	}


**3. Schritt:** Führe den *Task* mittels `POST {engine.dswarm.api}/tasks?persist={result.persistInDMP}` aus

**4. Schritt:** Verarbeite ggf. das Ergebnis-JSON (falls `result.persistInFolder=true`)


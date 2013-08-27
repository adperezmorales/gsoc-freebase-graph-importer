# Freebase Importer to Graph Database #

Freebase importer is a tool to import the Freebase data dump (through [BaseKB][1]) into a Graph database managed by [Tinkerpop Blueprints][2].  

The aim of this tool is to create a graph with the entity information contained in Freebase in order to be applied in other tasks like Entity Disambiguation.  

## How to use it ##

In order to use the tool, do the following:  

1. Download the code
2. Run `'mvn package'` command
3. In the *target/* directory, run the command `java -Xmx1g -jar gsoc-freebase-graph-importer-{version}-jar-with-dependencies.jar`. It will show the help.

The tool supports the following params:

* -i,--inputDirectory <arg>  The BaseKBLime Freebase dataset file to be
                              processed or the input directory containing
                              the Freebase dataset files

* -g,--generateGraph         Tell the importer to generate the graph
                              structure (one vertex for each entity which
                              is a common.topic)

* -h,--help                  Display this help and exit

* -i,--inputDirectory <arg>  The BaseKBLime Freebase dataset file to be
                              processed or the input directory containing
                              the Freebase dataset files

* -o,--outputDirectory <arg> The output directory where the graph wil be
                              generated

* -r,--generateRelations     Tell the importer to generate the graph
                              relations (edges between entities directly
                              or indirectly connected

In order to generate a complete graph with this tool, the command to be run is:  
`java -Xmx1g -jar gsoc-freebase-graph-importer-{version}-jar-with-dependencies.jar -i "input_directory" -o "output_directory" -g -r`


It should be noted that this process is performed in two steps:

* The first step reads all files and creates one vertex in the graph for every entity which is a topic (rdf.type equals to common.topic)
* The second step reads again all files and creates the relations between vertex (edges) using a custom algorithm.

## Algorithm used to create the relations ##

* If an entity is a topic
	* Check entity properties to see if there is a reference to other topic entities.
		* If yes, creates a "direct relation" between (edge) both vertices and update the edge properties using the property which references the vertices
		* If not, continue.

* If an entity is not a topic
	* Check entity properties to see if there is a reference to a topic entity
	* if it exists, store the property and the topic entity (reference)
	* For the stored references, it creates a new "mediated relation" (edge) between all the referenced topic entities and updates the edge properties using the property referencing the first entity and the other property referencing the second entity.

**Example:**   

NoTopic -> Topic1, Topic2, Topic3  

Topic1 <--- property ----- No topic ----- property2 --> Topic2   
Topic1 <--- property ----- No topic ----- property3 --> Topic3  
Topic2 <--- property2 ----- No topic ----- property3 --> Topic3  

It creates: 

Topic1 <---- property,property2 ----> Topic2  
Topic1 <---- property,property3 ----> Topic3  
Topic2 <---- property2,property2 ----> Topic3  


The rule to update the edge properties is the following:  

 * A property can be simple (property) or composed (property.subproperty.subsubproperty)
 * The property is divided into subproperties using the *dot* character
 * Add 1 to the previous value of the subproperty in the edge or creates the subproperty with value of 1

**Example**  
- Property: *aaa.bbb.ccc*  
- Subproperties: *aaa*, *aaa.bbb*, *aaa.bbb.ccc* 
- Add 1 (or create with value of 1) to the properties *aaa*, *aaa.bbb* and *aaa.bbb.ccc*

## Notes ##

* Currently though all Tinkerpop Graphs are supported (by means of Graph interface), this tool uses the **Neo4jGraph** implementation of Tinkerpop Blueprints (which use **Neo4j** implementation), because it supports indices and transactions which are key issues to speed up the importer process and avoid memory problems.

*   The importer parses for each step all the files. This is thus since it is not possible to create vertex for entities which are referenced by "topic" entities because it is not known beforehand (until all the entities are processed) whether a referenced entity is a topic or not.

## Benchmark ##

The Freebase data dump provided by BaseKB Lime contains more than 1000 files.  
Some tests have been performed in order to see the performance of the importer.

### Environment ###

* Hardware: MacBook Pro 2,3 Intel Core i7 (8 GB 1600 MHz DDR3)  
* OS: OS X 10.8.4 (12E55) Mountain Lion 
* Java 1.7 (jdk1.7.0_21.jdk)
* Apache Jena 2.10
* Tinkerpop Blueprints 2.3.0

### Execution ###

* Processing 1 file with with ~40.000 topics) takes about 40 seconds 
* Processing 100 files with ~5.000.000 topics takes about 2.5 hours

## Jira ##

This tool is related to the [issue 1040](https://issues.apache.org/jira/browse/STANBOL-1140) of Stanbol Jira.  

## License

GSoC Freebase To Graph Importer is distributed under the terms of the [Apache License, 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
[1]: http://basekb.com
[2]: http://blueprints.tinkerpop.com

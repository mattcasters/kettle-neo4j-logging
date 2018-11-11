# kettle-neo4j-logging
Logs Kettle execution information to Neo4j


#### Pre-requisites for building the project:
* Maven, version 3+
* Java JDK 1.8
* This [settings.xml](https://raw.githubusercontent.com/pentaho/maven-parent-poms/master/maven-support-files/settings.xml) in your <user-home>/.m2 directory

#### Downloads for Pentaho Data Integration

* [Neo4JOutput Step 3.14.3](https://github.com/knowbi/knowbi-pentaho-pdi-neo4j-output/releases/download/3.14.3/Neo4JOutput-3.14.3.zip) - Unzip into **./data-integration/plugins**

* [neo4j-jdbc-driver-3.4.0.jar](https://github.com/neo4j-contrib/neo4j-jdbc/releases/download/3.4.0/neo4j-jdbc-driver-3.4.0.jar) - copy it into **./data-integration/lib**


#### Building it

This is a maven project, and to build it use the following command

```
$ mvn clean install
```
#### Installing and configuring the plugin

After this command the plugin will be contained within a single jar file in
__kettle-neo4j-logging/target/kettle-neo4j-logging-0.2.0-SNAPSHOT.jar__

1. Create a folder *kettle-neo4j-logging* for this jar in
data-integration/plugins
```
$ mkdir /opt/pentaho/data-integration/plugins/kettle-neo4j-logging
```

2. Copy kettle-neo4j-logging-0.2.0-SNAPSHOT.jar to the kettle-neo4j-logging folder.
```
$ cp ./target/kettle-neo4j-logging*.jar /opt/pentaho/data-integration/kettle-neo4j-logging
```

3. Next we start PDI and create an empty transform where we can place Neo4J Output Step on the canvas so that we can have an opportunity to create a connection to Neo4j. Unlike normal JDBC connections, it will not show up in other step drop downs.  Creating with the Neo4j Output step will cause a file to be created in _~/.pentaho/metastore/Neo4j/Neo4j Connection_ defining the connection parameters. The connection name in the picture is called _SimpleNEO_

![Neo4J Connection Details](/doc-images/neo4j-output-step-define-connection-details.jpg)


4. Quit PDI and edit the ~/.kettle/kettle.properties file adding a variable **NEO4J_LOGGING_CONNECTION**

   ```vim ~/.kettle/kettle.properties``` then add the line 
   **NEO4J_LOGGING_CONNECTION=SimpleNEO**

At this point the plugin is installed and configured. When you load PDI and run jobs or transformations, they will all be logged to Neo4J.

A useful Cypher query
```
MATCH (n:JobEntry) RETURN n LIMIT 25
```
![Neo4J Query Result](/doc-images/neo4j-query-result-example.jpg)


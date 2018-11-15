package com.neo4j.kettle.logging.util;

import com.neo4j.kettle.logging.Defaults;
import com.neo4j.kettle.shared.NeoConnection;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionConfig;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingHierarchy;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoggingCore {

  public static final boolean isEnabled( VariableSpace space ) {
    String connectionName = space.getVariable( Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION );
    return StringUtils.isNotEmpty(connectionName);
  }

  public static final NeoConnection getConnection( IMetaStore metaStore, VariableSpace space) throws MetaStoreException {
    String connectionName = space.getVariable( Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION );
    if (StringUtils.isEmpty( connectionName )) {
      return null;
    }
    MetaStoreFactory<NeoConnection> factory = new MetaStoreFactory<NeoConnection>( NeoConnection.class, metaStore, Defaults.NAMESPACE );
    NeoConnection connection = factory.loadElement( connectionName );
    if (connection!=null) {
      connection.initializeVariablesFrom( space );
    }
    return connection;
  }


  public final static void writeHierarchies( LogChannelInterface log, NeoConnection connection, Transaction transaction,
                                             List<LoggingHierarchy> hierarchies, String rootLogChannelId ) {

    // First create the Execution nodes
    //
    for ( LoggingHierarchy hierarchy : hierarchies ) {
      LoggingObjectInterface loggingObject = hierarchy.getLoggingObject();
      LogLevel logLevel = loggingObject.getLogLevel();
      Map<String, Object> execPars = new HashMap<>();
      execPars.put( "name", loggingObject.getObjectName() );
      execPars.put( "type", loggingObject.getObjectType().name() );
      execPars.put( "id", loggingObject.getLogChannelId() );
      execPars.put( "containerId", loggingObject.getContainerObjectId() );
      execPars.put( "logLevel", logLevel != null ? logLevel.getCode() : null );
      execPars.put( "root", loggingObject.getLogChannelId().equals(rootLogChannelId));
      execPars.put( "registrationDate", new SimpleDateFormat( "yyyy/MM/dd'T'HH:mm:ss" ).format( loggingObject.getRegistrationDate() ) );

      StringBuilder execCypher = new StringBuilder();
      execCypher.append( "MERGE (exec:Execution { name : {name}, type : {type}, id : {id}} ) " );
      execCypher.append( "SET " );
      execCypher.append( "  exec.containerId = {containerId} " );
      execCypher.append( ", exec.logLevel = {logLevel} " );
      execCypher.append( ", exec.registrationDate = {registrationDate} " );
      execCypher.append( ", exec.root = {root} " );

      transaction.run( execCypher.toString(), execPars );
    }

    // Now create the relationships between them
    //
    for ( LoggingHierarchy hierarchy : hierarchies ) {
      LoggingObjectInterface loggingObject = hierarchy.getLoggingObject();
      LoggingObjectInterface parentObject = loggingObject.getParent();
      if ( parentObject != null ) {
        Map<String, Object> execPars = new HashMap<>();
        execPars.put( "name", loggingObject.getObjectName() );
        execPars.put( "type", loggingObject.getObjectType().name() );
        execPars.put( "id", loggingObject.getLogChannelId() );
        execPars.put( "parentName", parentObject.getObjectName() );
        execPars.put( "parentType", parentObject.getObjectType().name() );
        execPars.put( "parentId", parentObject.getLogChannelId() );

        StringBuilder execCypher = new StringBuilder();
        execCypher.append( "MATCH (child:Execution { name : {name}, type : {type}, id : {id}} ) " );
        execCypher.append( "MATCH (parent:Execution { name : {parentName}, type : {parentType}, id : {parentId}} ) " );
        execCypher.append( "MERGE (parent)-[rel:EXECUTES_"+loggingObject.getObjectType().name()+"]->(child) " );
        transaction.run( execCypher.toString(), execPars );
      }
    }
  }


  public static StatementResult executeCypher( LogChannelInterface log, NeoConnection connection, String cypher, Map<String, Object> parameters ) throws Exception {
    Driver driver = null;
    Session session = null;
    StatementResult result = null;

    try {
      driver = connection.getDriver( log );
      session = driver.session();

      return session.run( cypher, parameters );

    } finally {
      if (session!=null) {
        session.close();
      }
      if (driver!=null) {
        driver.close();
      }
    }
  }
}

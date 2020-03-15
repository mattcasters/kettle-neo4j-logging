package com.neo4j.kettle.logging.util;

import com.neo4j.kettle.logging.Defaults;
import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.graphics.Rectangle;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.kettle.core.metastore.MetaStoreFactory;
import org.neo4j.kettle.shared.NeoConnection;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingHierarchy;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoggingCore {

  public static final boolean isEnabled( VariableSpace space ) {
    String connectionName = space.getVariable( Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION );
    return
      StringUtils.isNotEmpty( connectionName ) &&
        !Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION_DISABLED.equals( connectionName );
  }

  public static final NeoConnection getConnection( IMetaStore metaStore, VariableSpace space ) throws MetaStoreException {
    String connectionName = space.getVariable( Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION );
    if ( StringUtils.isEmpty( connectionName ) ) {
      return null;
    }
    MetaStoreFactory<NeoConnection> factory = new MetaStoreFactory<NeoConnection>( NeoConnection.class, metaStore, Defaults.NAMESPACE );
    NeoConnection connection = factory.loadElement( connectionName );
    if ( connection != null ) {
      connection.initializeVariablesFrom( space );
    }
    return connection;
  }


  public final static void writeHierarchies( LogChannelInterface log, NeoConnection connection, Transaction transaction,
                                             List<LoggingHierarchy> hierarchies, String rootLogChannelId ) {

    try {
      // First create the Execution nodes
      //
      for ( LoggingHierarchy hierarchy : hierarchies ) {
        LoggingObjectInterface loggingObject = hierarchy.getLoggingObject();
        LogLevel logLevel = loggingObject.getLogLevel();
        Map<String, Object> execPars = new HashMap<>();
        execPars.put( "name", loggingObject.getObjectName() );
        execPars.put( "type", loggingObject.getObjectType().name() );
        execPars.put( "copy", loggingObject.getObjectCopy() );
        execPars.put( "id", loggingObject.getLogChannelId() );
        execPars.put( "containerId", loggingObject.getContainerObjectId() );
        execPars.put( "logLevel", logLevel != null ? logLevel.getCode() : null );
        execPars.put( "root", loggingObject.getLogChannelId().equals( rootLogChannelId ) );
        execPars.put( "registrationDate", new SimpleDateFormat( "yyyy/MM/dd'T'HH:mm:ss" ).format( loggingObject.getRegistrationDate() ) );

        StringBuilder execCypher = new StringBuilder();
        execCypher.append( "MERGE (exec:Execution { name : $name, type : $type, id : $id } ) " );
        execCypher.append( "SET " );
        execCypher.append( "  exec.containerId = $containerId " );
        execCypher.append( ", exec.logLevel = $logLevel " );
        execCypher.append( ", exec.copy = $copy " );
        execCypher.append( ", exec.registrationDate = $registrationDate " );
        execCypher.append( ", exec.root = $root " );

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
          execCypher.append( "MATCH (child:Execution { name : $name, type : $type, id : $id } ) " );
          execCypher.append( "MATCH (parent:Execution { name : $parentName, type : $parentType, id : $parentId } ) " );
          execCypher.append( "MERGE (parent)-[rel:EXECUTES]->(child) " );
          transaction.run( execCypher.toString(), execPars );
        }
      }
      transaction.commit();
    } catch(Exception e) {
      transaction.rollback();
      log.logError( "Error logging hierarchies", e );
    }
  }


  public static <T> T executeCypher( LogChannelInterface log, NeoConnection connection, String cypher, Map<String, Object> parameters, WorkLambda<T> lambda ) throws Exception {

    Session session = null;
    try {
      session = connection.getSession(log);

      return session.readTransaction( new TransactionWork<T>() {
        @Override public T execute( Transaction tx ) {
          Result result = tx.run( cypher, parameters );
          return lambda.getResultValue( result );
        }
      } );
    } finally {
      if ( session != null ) {
        session.close();
      }
    }
  }

  public static String getStringValue( Record record, int i ) {
    if ( i >= record.size() ) {
      return null;
    }
    Value value = record.get( i );
    if ( value == null || value.isNull() ) {
      return null;
    }
    return value.asString();
  }

  public static Long getLongValue( Record record, int i ) {
    if ( i >= record.size() ) {
      return null;
    }
    Value value = record.get( i );
    if ( value == null || value.isNull() ) {
      return null;
    }
    return value.asLong();
  }

  public static Date getDateValue( Record record, int i ) {
    if ( i >= record.size() ) {
      return null;
    }
    Value value = record.get( i );
    if ( value == null || value.isNull() ) {
      return null;
    }
    LocalDateTime localDateTime = value.asLocalDateTime();
    if ( localDateTime == null ) {
      return null;
    }
    return Date.from( localDateTime.atZone( ZoneId.systemDefault() ).toInstant() );
  }

  public static Boolean getBooleanValue( Record record, int i ) {
    if ( i >= record.size() ) {
      return null;
    }
    Value value = record.get( i );
    if ( value == null || value.isNull() ) {
      return null;
    }
    return value.asBoolean();
  }

  public static String getStringValue( Node node, String name ) {
    Value value = node.get( name );
    if ( value == null || value.isNull() ) {
      return null;
    }
    return value.asString();
  }

  public static Long getLongValue( Node node, String name ) {
    Value value = node.get( name );
    if ( value == null || value.isNull() ) {
      return null;
    }
    return value.asLong();
  }

  public static Boolean getBooleanValue( Node node, String name ) {
    Value value = node.get( name );
    if ( value == null || value.isNull() ) {
      return null;
    }
    return value.asBoolean();
  }

  public static Date getDateValue( Node node, String name ) {
    Value value = node.get( name );
    if ( value == null || value.isNull() ) {
      return null;
    }
    LocalDateTime localDateTime = value.asLocalDateTime();
    if ( localDateTime == null ) {
      return null;
    }
    return Date.from( localDateTime.atZone( ZoneId.systemDefault() ).toInstant() );
  }

  public static double calculateRadius( Rectangle bounds ) {
    double radius = (double) ( Math.min( bounds.width, bounds.height ) ) * 0.8 / 2; // 20% margin around circle
    return radius;
  }

  public static double calculateOptDistance( Rectangle bounds, int nrNodes ) {

    if ( nrNodes == 0 ) {
      return -1;
    }
    // Layout around a circle in essense.
    // So get a spot for every node on the circle.
    //

    // The radius is at most the smallest of width or height
    //
    double radius = calculateRadius( bounds );

    // Circumference
    //
    double circleLength = Math.PI * 2 * radius;

    // Optimal distance estimate is line segment on circle circumference
    // 25% margin between segments
    // Only put half of the nodes on the circle, the rest not.
    //
    double optDistance = 0.75 * circleLength / ( nrNodes * 2 );

    return optDistance;
  }

}

package com.neo4j.kettle.logging.trans;

import com.neo4j.kettle.logging.Defaults;
import com.neo4j.kettle.logging.util.LoggingCore;
import com.neo4j.kettle.logging.util.LoggingSession;
import com.neo4j.kettle.logging.util.MetaStoreUtil;
import com.neo4j.kettle.shared.NeoConnection;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingHierarchy;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransAdapter;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.metastore.api.IMetaStore;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ExtensionPoint(
  id = "TransLoggingExtensionPoint",
  extensionPointId = "TransformationStartThreads",
  description = "Handle logging to Neo4j for a transformation"
)
public class TransLoggingExtensionPoint implements ExtensionPointInterface {

  public static final String TRANS_START_DATE = "TRANS_START_DATE";
  public static final String TRANS_END_DATE = "TRANS_END_DATE";

  public static final String EXECUTION_TYPE_TRANSFORMATION = LoggingObjectType.TRANS.name();
  public static final String EXECUTION_TYPE_STEP = LoggingObjectType.STEP.name();

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof Trans ) ) {
      return;
    }

    final Trans trans = (Trans) object;

    // See if logging is enabled
    //
    if ( !LoggingCore.isEnabled( trans ) ) {
      return;
    }

    // Keep the start date
    //
    trans.getExtensionDataMap().put( TRANS_START_DATE, new Date() );

    // We only log after a top level transformation is done.
    //
    if ( trans.getParentJob() != null || trans.getParentTrans() != null ) {
      return;
    }

    String connectionName = trans.getVariable( Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION );

    try {

      // Which connection are we logging to?
      //
      IMetaStore metaStore = MetaStoreUtil.findMetaStore( trans );
      if (metaStore==null) {
        log.logBasic( "Warning! Unable to find a metastore to load Neo4j connection to log to '" + connectionName +"'" );
        return;
      }
      final NeoConnection connection = LoggingCore.getConnection( metaStore, trans );
      if ( connection == null ) {
        log.logBasic( "Warning! Unable to find Neo4j connection to log to : " + connectionName );
        return;
      }
      log.logDetailed( "Logging transformation information to Neo4j connection : " + connection.getName() );

      Session session = LoggingSession.getInstance().getSession( connection );

      logTransformationMetadata( log, session, connection, trans );
      logStartOfTransformation( log, session, connection, trans );

      trans.addTransListener( new TransAdapter() {
        @Override public void transFinished( Trans trans ) throws KettleException {
          logEndOfTransformation( log, session, connection, trans );

          // If there are no other parents, we now have the complete log channel hierarchy
          //
          logHierarchy( log, session, connection, trans.getLoggingHierarchy(), trans.getLogChannelId() );
        }
      } );

    } catch (
      Exception e ) {
      // Let's not kill the transformation just yet, just log the error
      // otherwise: throw new KettleException(...);
      //
      log.logError( "Error logging to Neo4j:", e );
    }

  }

  private void logTransformationMetadata( final LogChannelInterface log, final Session session, final NeoConnection connection, final Trans trans ) throws KettleException {
    log.logDetailed( "Logging transformation metadata to Neo4j connection : " + connection.getName() );

    final TransMeta transMeta = trans.getTransMeta();

    synchronized ( session ) {
      session.writeTransaction( new TransactionWork<Void>() {
        @Override public Void execute( Transaction transaction ) {
          try {

            Map<String, Object> transPars = new HashMap<>();
            transPars.put( "transName", transMeta.getName() );
            transPars.put( "description", transMeta.getDescription() );
            transPars.put( "filename", transMeta.getFilename() );
            StringBuilder transCypher = new StringBuilder();
            transCypher.append( "MERGE (trans:Transformation { name : {transName}} ) " );
            transCypher.append( "SET trans.filename = {filename}, trans.description = {description} " );
            transaction.run( transCypher.toString(), transPars );

            log.logDetailed( "Trans cypher : " + transCypher );

            for ( StepMeta stepMeta : transMeta.getSteps() ) {

              Map<String, Object> stepPars = new HashMap<>();
              stepPars.put( "transName", transMeta.getName() );
              stepPars.put( "name", stepMeta.getName() );
              stepPars.put( "description", stepMeta.getDescription() );
              stepPars.put( "pluginId", stepMeta.getStepID() );
              stepPars.put( "copies", stepMeta.getCopies() );
              stepPars.put( "locationX", stepMeta.getLocation().x );
              stepPars.put( "locationY", stepMeta.getLocation().y );
              stepPars.put( "drawn", stepMeta.isDrawn() );

              StringBuilder stepCypher = new StringBuilder();
              stepCypher.append( "MATCH (trans:Transformation { name : {transName}} ) " );
              stepCypher.append( "MERGE (step:Step { transName : {transName}, name : {name}}) " );
              stepCypher.append( "SET " );
              stepCypher.append( "   step.description = {description} " );
              stepCypher.append( ", step.pluginId = {pluginId} " );
              stepCypher.append( ", step.copies = {copies} " );
              stepCypher.append( ", step.locationX = {locationX} " );
              stepCypher.append( ", step.locationY = {locationY} " );
              stepCypher.append( ", step.drawn = {drawn} " );

              // Also MERGE the relationship
              //
              stepCypher.append( "MERGE (step)-[rel:STEP_OF_TRANSFORMATION]->(trans) " );

              log.logDetailed( "Step '" + stepMeta.getName() + "' cypher : " + stepCypher );

              // run it
              //
              transaction.run( stepCypher.toString(), stepPars );
            }

            // Save hops
            //
            for ( int i = 0; i < transMeta.nrTransHops(); i++ ) {
              TransHopMeta hopMeta = transMeta.getTransHop( i );

              Map<String, Object> hopPars = new HashMap<>();
              hopPars.put( "fromStep", hopMeta.getFromStep().getName() );
              hopPars.put( "toStep", hopMeta.getToStep().getName() );
              hopPars.put( "transName", transMeta.getName() );

              StringBuilder hopCypher = new StringBuilder();
              hopCypher.append( "MATCH (from:Step { transName : {transName}, name : {fromStep}}) " );
              hopCypher.append( "MATCH (to:Step { transName : {transName}, name : {toStep}}) " );
              hopCypher.append( "MERGE (from)-[rel:WRITES_TO]->(to) " );
              transaction.run( hopCypher.toString(), hopPars );
            }

            transaction.success();
          } catch ( Exception e ) {
            transaction.failure();
            log.logError( "Error logging transformation metadata", e );
          }
          return null;
        }
      } );
    }

  }

  private void logStartOfTransformation( final LogChannelInterface log, final Session session, final NeoConnection connection, final Trans trans ) throws KettleException {
    log.logDetailed( "Logging execution start of transformation to Neo4j connection : " + connection.getName() );

    final TransMeta transMeta = trans.getTransMeta();

    synchronized ( session ) {

      session.writeTransaction( new TransactionWork<Void>() {
        @Override public Void execute( Transaction transaction ) {
          try {
            // Create a new node for each log channel and it's owner
            // Start with the transformation
            //
            LogChannelInterface channel = trans.getLogChannel();
            Date startDate = (Date) trans.getExtensionDataMap().get( TRANS_START_DATE );

            Map<String, Object> transPars = new HashMap<>();
            transPars.put( "transName", transMeta.getName() );
            transPars.put( "id", channel.getLogChannelId() );
            transPars.put( "type", EXECUTION_TYPE_TRANSFORMATION );
            transPars.put( "executionStart", new SimpleDateFormat( "yyyy/MM/dd'T'HH:mm:ss" ).format( startDate ) );
            transPars.put( "status", trans.getStatus() );

            StringBuilder transCypher = new StringBuilder();
            transCypher.append( "MATCH (trans:Transformation { name : {transName}} ) " );
            transCypher.append( "MERGE (exec:Execution { name : {transName}, type : {type}, id : {id}} ) " );
            transCypher.append( "SET " );
            transCypher.append( "  exec.executionStart = {executionStart} " );
            transCypher.append( ", exec.status = {status} " );
            transCypher.append( "MERGE (exec)-[r:EXECUTION_OF_TRANSFORMATION]->(trans) " );

            transaction.run( transCypher.toString(), transPars );

            transaction.success();
          } catch ( Exception e ) {
            transaction.failure();
            log.logError( "Error logging transformation start", e );
          }

          return null;
        }
      } );
    }
  }

  private void logEndOfTransformation( final LogChannelInterface log, final Session session, final NeoConnection connection, final Trans trans ) throws KettleException {
    log.logDetailed( "Logging execution end of transformation to Neo4j connection : " + connection.getName() );

    final TransMeta transMeta = trans.getTransMeta();

    synchronized ( session ) {
      session.writeTransaction( new TransactionWork<Void>() {
        @Override public Void execute( Transaction transaction ) {
          try {

            // Create a new node for each log channel and it's owner
            // Start with the transformation
            //
            LogChannelInterface channel = trans.getLogChannel();
            Result result = trans.getResult();
            String transLogChannelId = trans.getLogChannelId();
            String transLoggingText = KettleLogStore.getAppender().getBuffer( transLogChannelId, false ).toString();
            Date endDate = new Date();
            trans.getExtensionDataMap().put( TRANS_END_DATE, endDate );
            Date startDate = (Date) trans.getExtensionDataMap().get( TRANS_START_DATE );

            Map<String, Object> transPars = new HashMap<>();
            transPars.put( "transName", transMeta.getName() );
            transPars.put( "type", EXECUTION_TYPE_TRANSFORMATION );
            transPars.put( "id", channel.getLogChannelId() );
            transPars.put( "executionEnd", new SimpleDateFormat( "yyyy/MM/dd'T'HH:mm:ss" ).format( endDate ) );
            transPars.put( "durationMs", endDate.getTime() - startDate.getTime() );
            transPars.put( "errors", result.getNrErrors() );
            transPars.put( "linesInput", result.getNrLinesInput() );
            transPars.put( "linesOutput", result.getNrLinesOutput() );
            transPars.put( "linesRead", result.getNrLinesRead() );
            transPars.put( "linesWritten", result.getNrLinesWritten() );
            transPars.put( "linesRejected", result.getNrLinesRejected() );
            transPars.put( "loggingText", transLoggingText );
            transPars.put( "status", trans.getStatus() );

            StringBuilder transCypher = new StringBuilder();
            transCypher.append( "MATCH (trans:Transformation { name : {transName}} ) " );
            transCypher.append( "MERGE (exec:Execution { name : {transName}, type : {type}, id : {id}} ) " );
            transCypher.append( "SET " );
            transCypher.append( "  exec.executionEnd = {executionEnd} " );
            transCypher.append( ", exec.durationMs = {durationMs} " );
            transCypher.append( ", exec.status = {status} " );
            transCypher.append( ", exec.errors = {errors} " );
            transCypher.append( ", exec.linesInput = {linesInput} " );
            transCypher.append( ", exec.linesOutput = {linesOutput} " );
            transCypher.append( ", exec.linesRead = {linesRead} " );
            transCypher.append( ", exec.linesWritten = {linesWritten} " );
            transCypher.append( ", exec.linesRejected = {linesRejected} " );
            transCypher.append( ", exec.loggingText = {loggingText} " );
            transCypher.append( "MERGE (exec)-[r:EXECUTION_OF_TRANSFORMATION]->(trans) " );

            transaction.run( transCypher.toString(), transPars );

            // Also log every step copy
            //
            List<StepMetaDataCombi> combis = trans.getSteps();
            for ( StepMetaDataCombi combi : combis ) {
              String stepLogChannelId = combi.step.getLogChannel().getLogChannelId();
              String stepLoggingText = KettleLogStore.getAppender().getBuffer( stepLogChannelId, false ).toString();
              Map<String, Object> stepPars = new HashMap<>();
              stepPars.put( "transName", transMeta.getName() );
              stepPars.put( "name", combi.stepname );
              stepPars.put( "type", EXECUTION_TYPE_STEP );
              stepPars.put( "id", stepLogChannelId );
              stepPars.put( "transId", transLogChannelId );
              stepPars.put( "copy", Long.valueOf( combi.copy ) );
              stepPars.put( "status", combi.step.getStatus().getDescription() );
              stepPars.put( "loggingText", stepLoggingText );
              stepPars.put( "errors", combi.step.getErrors() );
              stepPars.put( "linesRead", combi.step.getLinesRead() );
              stepPars.put( "linesWritten", combi.step.getLinesWritten() );
              stepPars.put( "linesInput", combi.step.getLinesInput() );
              stepPars.put( "linesOutput", combi.step.getLinesOutput() );
              stepPars.put( "linesRejected", combi.step.getLinesRejected() );

              StringBuilder stepCypher = new StringBuilder();
              stepCypher.append( "MATCH (step:Step { transName : {transName}, name : {name} } ) " );
              stepCypher.append( "MERGE (exec:Execution { name : {name}, type : {type}, id : {id}} ) " );
              stepCypher.append( "SET " );
              stepCypher.append( "  exec.transId = {transId} " );
              stepCypher.append( ", exec.copy = {copy} " );
              stepCypher.append( ", exec.status = {status} " );
              stepCypher.append( ", exec.loggingText = {loggingText} " );
              stepCypher.append( ", exec.errors = {errors} " );
              stepCypher.append( ", exec.linesRead = {linesRead} " );
              stepCypher.append( ", exec.linesWritten = {linesWritten} " );
              stepCypher.append( ", exec.linesInput = {linesInput} " );
              stepCypher.append( ", exec.linesOutput = {linesOutput} " );
              stepCypher.append( ", exec.linesRejected = {linesRejected} " );
              stepCypher.append( "MERGE (exec)-[r:EXECUTION_OF_STEP]->(step) " );

              transaction.run( stepCypher.toString(), stepPars );

              // Log graph usage as well
              // This Map is left by the Neo4j step plugins : Neo4j Output and Neo4j Graph Output
              //
              Map<String, Map<String, Set<String>>> usageMap = (Map<String, Map<String, Set<String>>>) trans.getExtensionDataMap().get( Defaults.TRANS_NODE_UPDATES_GROUP );
              if ( usageMap != null ) {
                for ( String graphUsage : usageMap.keySet() ) {
                  Map<String, Set<String>> stepsMap = usageMap.get( graphUsage );

                  Set<String> labels = stepsMap.get( combi.stepname );
                  if ( labels != null ) {
                    for ( String label : labels ) {
                      // Save relationship to GraphUsage node
                      //
                      Map<String, Object> usagePars = new HashMap<>();
                      usagePars.put( "step", combi.stepname );
                      usagePars.put( "type", "STEP" );
                      usagePars.put( "id", stepLogChannelId );
                      usagePars.put( "label", label );
                      usagePars.put( "usage", graphUsage );

                      StringBuilder usageCypher = new StringBuilder();
                      usageCypher.append( "MATCH (step:Execution { name : {step}, type : {type}, id : {id} } ) " );
                      usageCypher.append( "MERGE (usage:Usage { usage : {usage}, label : {label} } ) " );
                      usageCypher.append( "MERGE (step)-[r:PERFORMS_" + graphUsage + "]->(usage)" );

                      transaction.run( usageCypher.toString(), usagePars );
                    }
                  }
                }
              }
            }

            transaction.success();
          } catch ( Exception e ) {
            transaction.failure();
            log.logError( "Error logging transformation end", e );
          }
          return null;
        }
      } );
    }
  }


  private void logHierarchy( final LogChannelInterface log, final Session session, final NeoConnection connection,
                             final List<LoggingHierarchy> hierarchies, String rootLogChannelId ) {
    synchronized ( session ) {
      session.writeTransaction( new TransactionWork<Void>() {
        @Override public Void execute( Transaction transaction ) {
          // Update create the Execution relationships
          //
          LoggingCore.writeHierarchies( log, connection, transaction, hierarchies, rootLogChannelId );
          return null;
        }
      } );
    }
  }
}

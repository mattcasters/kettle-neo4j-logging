package com.neo4j.kettle.logging.trans;

import com.neo4j.kettle.logging.util.LoggingCore;
import com.neo4j.kettle.shared.NeoConnection;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingHierarchy;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransAdapter;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExtensionPoint(
  id = "TransformationStartThreads",
  extensionPointId = "TransformationStartThreads",
  description = "Handle transformation logging to Neo4j for a transformation"
)
public class TransLoggingExtensionPoint implements ExtensionPointInterface {

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

    try {

      // Which connection are we logging to?
      //
      final NeoConnection connection = LoggingCore.getConnection( trans.getMetaStore(), trans );
      log.logDetailed("Logging information to Neo4j connection : "+connection.getName());

      logTransformationMetadata( log, connection, trans );
      logStartOfTransformation( log, connection, trans );

      trans.addTransListener( new TransAdapter() {
        @Override public void transFinished( Trans trans ) throws KettleException {
          logEndOfTransformation( log, connection, trans );

          // If there are no other parents, we now have the complete log channel hierarchy
          //
          if ( trans.getParentJob() == null && trans.getParentTrans() == null ) {
            logHierarchy( log, connection, trans.getLogChannel() );
          }
        }
      } );

    } catch ( Exception e ) {
      // Let's not kill the transformation just yet, just log the error
      // otherwise: throw new KettleException(...);
      //
      log.logError( "Error logging to Neo4j:", e );
    }
  }

  private void logTransformationMetadata( LogChannelInterface log, NeoConnection connection, Trans trans) throws KettleException {
    log.logDetailed("Logging transformation metadata to Neo4j connection : "+connection.getName());

    TransMeta transMeta = trans.getTransMeta();
    Driver driver = null;
    Session session = null;
    Transaction transaction = null;
    try {
      driver = connection.getDriver( log );
      session = driver.session();
      transaction = session.beginTransaction();

      Map<String, Object> transPars = new HashMap<>();
      transPars.put("transName", transMeta.getName());
      transPars.put("description", transMeta.getDescription());
      transPars.put("filename", transMeta.getFilename());
      StringBuilder transCypher = new StringBuilder();
      transCypher.append("MERGE (trans:Transformation { name : {transName}} ) ");
      transCypher.append("SET trans.filename = {filename}, trans.description = {description} ");
      transaction.run( transCypher.toString(), transPars );

      log.logDetailed("Trans cypher : "+transCypher);

      for ( StepMeta stepMeta : transMeta.getSteps()) {

        Map<String, Object> stepPars = new HashMap<>();
        stepPars.put("transName", transMeta.getName());
        stepPars.put("name", stepMeta.getName());
        stepPars.put("description", stepMeta.getDescription());
        stepPars.put("pluginId", stepMeta.getStepID());
        stepPars.put("copies", stepMeta.getCopies());
        stepPars.put("locationX", stepMeta.getLocation().x);
        stepPars.put("locationY", stepMeta.getLocation().y);
        stepPars.put("drawn", stepMeta.isDrawn());

        StringBuilder stepCypher = new StringBuilder(  );
        stepCypher.append( "MATCH (trans:Transformation { name : {transName}} ) ");
        stepCypher.append( "MERGE (step:Step { transName : {transName}, name : {name}}) " );
        stepCypher.append( "SET ");
        stepCypher.append("   step.description = {description} ");
        stepCypher.append( ", step.pluginId = {pluginId} ");
        stepCypher.append( ", step.copies = {copies} ");
        stepCypher.append( ", step.locationX = {locationX} ");
        stepCypher.append( ", step.locationY = {locationY} ");
        stepCypher.append( ", step.drawn = {drawn} ");

        // Get all the Strings
        // TODO: not saving everything, better find another option
        /*
        StepMetaInterface metaInterface = stepMeta.getStepMetaInterface();
        List<StringSearchResult> results = new ArrayList<>();
        StringSearcher.findMetaData( metaInterface, 1, results, stepMeta, transMeta );

        for (StringSearchResult result : results) {
          String propName = "step_"+result.getFieldName();
          String propValue = result.getString();
          stepPars.put(propName, propValue);
          stepCypher.append(", step."+propName+" = {"+propName+"} ");
        }
        */

        // Also MERGE the relationship
        //
        stepCypher.append("MERGE (step)-[rel:STEP_OF_TRANSFORMATION]->(trans) ");

        log.logDetailed("Step '"+stepMeta.getName()+"' cypher : "+stepCypher);

        // run it
        //
        transaction.run( stepCypher.toString(), stepPars );
      }

      // Save hops
      //
      for (int i=0;i<transMeta.nrTransHops();i++) {
        TransHopMeta hopMeta = transMeta.getTransHop( i );

        Map<String, Object> hopPars = new HashMap<>();
        hopPars.put("fromStep", hopMeta.getFromStep().getName());
        hopPars.put("toStep", hopMeta.getToStep().getName());
        hopPars.put("transName", transMeta.getName());

        StringBuilder hopCypher = new StringBuilder(  );
        hopCypher.append( "MATCH (from:Step { transName : {transName}, name : {fromStep}}) " );
        hopCypher.append( "MATCH (to:Step { transName : {transName}, name : {toStep}}) " );
        hopCypher.append( "MERGE (from)-[rel:WRITES_TO]->(to) ");
        transaction.run( hopCypher.toString(), hopPars );
      }

      transaction.success();
    } catch ( Exception e ) {
      transaction.failure();
      throw new KettleException( "Error logging transformation metadata", e );
    } finally {
      if ( transaction != null ) {
        transaction.close();
      }
      if ( session != null ) {
        session.close();
      }
      if ( driver != null ) {
        driver.close();
      }
    }

  }

  private void logStartOfTransformation( LogChannelInterface log, NeoConnection connection, Trans trans ) throws KettleException {
    log.logDetailed("Logging execution start of transformation to Neo4j connection : "+connection.getName());

    TransMeta transMeta = trans.getTransMeta();
    Driver driver = null;
    Session session = null;
    Transaction transaction = null;
    try {
      driver = connection.getDriver( log );
      session = driver.session();
      transaction = session.beginTransaction();

      // Create a new node for each log channel and it's owner
      // Start with the transformation
      //
      LogChannelInterface channel = trans.getLogChannel();
      Map<String, Object> transPars = new HashMap<>();
      transPars.put("transName", transMeta.getName());
      transPars.put("id", channel.getLogChannelId());
      transPars.put("type", EXECUTION_TYPE_TRANSFORMATION );
      transPars.put("executionStart", new SimpleDateFormat( "yyyy/MM/dd'T'HH:mm:ss" ).format( trans.getStartDate() ) );

      StringBuilder transCypher = new StringBuilder();
      transCypher.append("MATCH (trans:Transformation { name : {transName}} ) ");
      transCypher.append("MERGE (exec:Execution { objectName : {transName}, type : {type}, id : {id}} ) ");
      transCypher.append("SET ");
      transCypher.append(" exec.executionStart = {executionStart} ");
      transCypher.append("MERGE (exec)-[r:EXECUTION_OF_TRANSFORMATION]->(trans) ");

      transaction.run( transCypher.toString(), transPars );

      transaction.success();
    } catch ( Exception e ) {
      transaction.failure();
      throw new KettleException( "Error logging transformation start", e );
    } finally {
      if ( transaction != null ) {
        transaction.close();
      }
      if ( session != null ) {
        session.close();
      }
      if ( driver != null ) {
        driver.close();
      }
    }
  }

  private void logEndOfTransformation( LogChannelInterface log, NeoConnection connection, Trans trans ) throws KettleException {
    log.logDetailed("Logging execution end of transformation to Neo4j connection : "+connection.getName());

    TransMeta transMeta = trans.getTransMeta();
    Driver driver = null;
    Session session = null;
    Transaction transaction = null;
    try {
      driver = connection.getDriver( log );
      session = driver.session();
      transaction = session.beginTransaction();

      // Create a new node for each log channel and it's owner
      // Start with the transformation
      //
      LogChannelInterface channel = trans.getLogChannel();
      Result result = trans.getResult();
      String transLogChannelId = trans.getLogChannelId();
      String transLoggingText = KettleLogStore.getAppender().getBuffer( transLogChannelId, true ).toString();

      Map<String, Object> transPars = new HashMap<>();
      transPars.put("transName", transMeta.getName());
      transPars.put("type", EXECUTION_TYPE_TRANSFORMATION );
      transPars.put("id", channel.getLogChannelId());
      transPars.put("executionEnd", new SimpleDateFormat( "yyyy/MM/dd'T'HH:mm:ss" ).format( trans.getEndDate() ) );
      transPars.put("errors", result.getNrErrors() );
      transPars.put("linesInput", result.getNrLinesInput() );
      transPars.put("linesOutput", result.getNrLinesOutput() );
      transPars.put("linesRead", result.getNrLinesRead() );
      transPars.put("linesWritten", result.getNrLinesWritten() );
      transPars.put("linesRejected", result.getNrLinesRejected() );
      transPars.put("loggingText", transLoggingText);

      StringBuilder transCypher = new StringBuilder();
      transCypher.append("MATCH (trans:Transformation { name : {transName}} ) ");
      transCypher.append("MERGE (exec:Execution { objectName : {transName}, type : {type}, id : {id}} ) ");
      transCypher.append("SET ");
      transCypher.append("  exec.executionEnd = {executionEnd} ");
      transCypher.append(", exec.errors = {errors} ");
      transCypher.append(", exec.linesInput = {linesInput} ");
      transCypher.append(", exec.linesOutput = {linesOutput} ");
      transCypher.append(", exec.linesRead = {linesRead} ");
      transCypher.append(", exec.linesWritten = {linesWritten} ");
      transCypher.append(", exec.linesRejected = {linesRejected} ");
      transCypher.append(", exec.loggingText = {loggingText} ");
      transCypher.append("MERGE (exec)-[r:EXECUTION_OF_TRANSFORMATION]->(trans) ");

      transaction.run( transCypher.toString(), transPars );

      // Also log every step copy
      //
      List<StepMetaDataCombi> combis = trans.getSteps();
      for (StepMetaDataCombi combi : combis) {
        String stepLogChannelId = combi.step.getLogChannel().getLogChannelId();
        String stepLoggingText = KettleLogStore.getAppender().getBuffer( stepLogChannelId, true ).toString();
        Map<String, Object> stepPars = new HashMap<>();
        stepPars.put("transName", transMeta.getName());
        stepPars.put("name", combi.stepname);
        stepPars.put("type", EXECUTION_TYPE_STEP );
        stepPars.put("transType", EXECUTION_TYPE_TRANSFORMATION );
        stepPars.put("id", stepLogChannelId);
        stepPars.put("transId", transLogChannelId);
        stepPars.put("copy", Long.valueOf(combi.copy) );
        stepPars.put("status", combi.step.getStatus().getDescription() );
        stepPars.put("loggingText", stepLoggingText );
        stepPars.put("errors", combi.step.getErrors() );

        StringBuilder stepCypher = new StringBuilder();
        stepCypher.append("MATCH (step:Step { transName : {transName}, name : {name} } ) ");
        stepCypher.append("MERGE (exec:Execution { objectName : {name}, type : {type}, id : {id}} ) ");
        stepCypher.append("SET ");
        stepCypher.append("  exec.copy = {copy} ");
        stepCypher.append(", exec.status = {status} ");
        stepCypher.append(", exec.loggingText = {loggingText} ");
        stepCypher.append(", exec.errors = {errors} ");
        stepCypher.append("MERGE (exec)-[r:EXECUTION_OF_STEP]->(step) ");

        transaction.run( stepCypher.toString(), stepPars );
      }


      // Now log the complete execution hierarchy
      //
      List<LoggingHierarchy> hierarchies = trans.getLoggingHierarchy();

      // First create the Execution nodes
      //
      for (LoggingHierarchy hierarchy : hierarchies) {
        LoggingObjectInterface loggingObject = hierarchy.getLoggingObject();
        LogLevel logLevel = loggingObject.getLogLevel();
        Map<String, Object> execPars = new HashMap<>();
        execPars.put("name", loggingObject.getObjectName());
        execPars.put("type", loggingObject.getObjectType().name());
        execPars.put("id", loggingObject.getLogChannelId());
        execPars.put("containerId", loggingObject.getContainerObjectId());
        execPars.put("logLevel", logLevel!=null ? logLevel.getCode() : null);
        execPars.put("registrationDate", new SimpleDateFormat( "yyyy/MM/dd'T'HH:mm:ss" ).format( loggingObject.getRegistrationDate()));

        StringBuilder execCypher = new StringBuilder();
        execCypher.append("MERGE (exec:Execution { objectName : {name}, type : {type}, id : {id}} ) ");
        execCypher.append("SET ");
        execCypher.append("  exec.containerId = {containerId} ");
        execCypher.append(", exec.logLevel = {logLevel} ");
        execCypher.append(", exec.registrationDate = {registrationDate} ");

        transaction.run( execCypher.toString(), execPars );
      }

      // Now create the Execution relationships
      //
      for (LoggingHierarchy hierarchy : hierarchies) {
        LoggingObjectInterface loggingObject = hierarchy.getLoggingObject();
        LoggingObjectInterface parentObject = loggingObject.getParent();
        if (parentObject!=null) {
          Map<String, Object> execPars = new HashMap<>();
          execPars.put( "name", loggingObject.getObjectName() );
          execPars.put( "type", loggingObject.getObjectType().name() );
          execPars.put( "id", loggingObject.getLogChannelId() );
          execPars.put( "parentName", parentObject.getObjectName() );
          execPars.put( "parentType", parentObject.getObjectType().name() );
          execPars.put( "parentId", parentObject.getLogChannelId() );

          StringBuilder execCypher = new StringBuilder();
          execCypher.append( "MATCH (parent:Execution { objectName : {name}, type : {type}, id : {id}} ) " );
          execCypher.append( "MATCH (child:Execution { objectName : {parentName}, type : {parentType}, id : {parentId}} ) " );
          execCypher.append( "MERGE (parent)-[rel:EXECUTES]->(child) " );
          transaction.run( execCypher.toString(), execPars );
        }
      }




      transaction.success();
    } catch ( Exception e ) {
      transaction.failure();
      throw new KettleException( "Error logging transformation end", e );
    } finally {
      if ( transaction != null ) {
        transaction.close();
      }
      if ( session != null ) {
        session.close();
      }
      if ( driver != null ) {
        driver.close();
      }
    }
  }


  private void logHierarchy( LogChannelInterface log, NeoConnection connection, LogChannelInterface logChannel ) {
  }
}

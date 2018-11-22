/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package com.neo4j.kettle.spoon;

import com.neo4j.kettle.logging.Defaults;
import com.neo4j.kettle.logging.util.LoggingCore;
import com.neo4j.kettle.model.DataModel;
import com.neo4j.kettle.model.DataNode;
import com.neo4j.kettle.model.DataProperty;
import com.neo4j.kettle.model.DataRelationship;
import com.neo4j.kettle.shared.NeoConnection;
import com.neo4j.kettle.spoon.history.HistoryResult;
import com.neo4j.kettle.spoon.history.HistoryResults;
import com.neo4j.kettle.spoon.history.HistoryResultsDialog;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.job.JobGraph;
import org.pentaho.di.ui.spoon.trans.TransGraph;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NeoLoggingHelper extends AbstractXulEventHandler implements ISpoonMenuController {
  protected static Class<?> PKG = NeoLoggingHelper.class; // for i18n

  private static NeoLoggingHelper instance = null;

  private Spoon spoon;

  private NeoConnection connection;
  private Driver driver;

  private NeoLoggingHelper() {
    spoon = Spoon.getInstance();
  }

  public static NeoLoggingHelper getInstance() {
    if ( instance == null ) {
      instance = new NeoLoggingHelper(); ;
      instance.spoon.addSpoonMenuController( instance );
      try {
        instance.connection = loadConnection();
        if ( instance.connection != null ) {
          instance.driver = instance.connection.getDriver( instance.spoon.getLog() );
        }
      } catch ( Exception e ) {
        instance.spoon.getLog().logError( "No usable Neo4j logging connection configured in variable " + Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION, e );
        instance.connection = null;
        instance.driver = null;
      }
    }
    return instance;
  }

  public String getName() {
    return "neoLoggingHelper";
  }

  public void updateMenu( Document doc ) {
    // Nothing so far.
  }

  private static NeoConnection loadConnection() throws MetaStoreException {
    VariableSpace space = new Variables();
    space.initializeVariablesFrom( null );
    NeoConnection neoConnection = LoggingCore.getConnection( Spoon.getInstance().getMetaStore(), space );
    return neoConnection;
  }


  public void examineStepLogs() {
    examineStepLogs( false );
  }

  public void examineStepErrorLogs() {
    examineStepLogs( true );
  }

  public void examineStepLogs( boolean errorsOnly ) {
    try {
      TransGraph transGraph = spoon.getActiveTransGraph();
      TransMeta transMeta = spoon.getActiveTransformation();
      StepMeta stepMeta = transGraph.getCurrentStep();
      if ( driver == null || connection == null || transGraph == null || transMeta == null || stepMeta == null ) {
        return;
      }

      // Keep some information around
      HistoryResults historyResults = new HistoryResults();
      historyResults.setSubjectName( stepMeta.getName() );
      historyResults.setSubjectType( "STEP" );
      historyResults.setSubjectCopy( "0" );
      historyResults.setParentName( transMeta.getName() );
      historyResults.setParentType( "TRANS" );
      historyResults.setTopic( "Execution history of step '" + stepMeta.getName() + "'" );

      Session session = null;
      try {
        session = driver.session();
        lookupExecutionHistory( historyResults, spoon.getLog(), session, errorsOnly, false );

        testGraphReturns( session, historyResults );

        HistoryResultsDialog historyResultsDialog = new HistoryResultsDialog( spoon.getShell(), historyResults );
        historyResultsDialog.open();

      } finally {
        if ( session != null ) {
          session.close();
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error examining step logs", e );
    }
  }

  private void testGraphReturns( Session session, HistoryResults historyResults ) {

    // Test, won't always work
    //
    String cypher = historyResults.getLastExecutions().get( 0 ).getShortestPathWithMetadataCommand( 0 );

    StatementResult result = session.run( cypher );

    /*
    while (result.hasNext()) {
      Record record = result.next();
      System.out.println( "Found record : "+record.size()+" values");
      for (String key : record.keys()) {
        Value value = record.get( key );
        System.out.println( "  - "+key+": "+value.type().name());
      }
    }
    */

    DataModel graphModel = new DataModel( "Execution and metadata", result );

  }

  public void showLogging() {
    if ( driver == null || connection == null ) {
      return;
    }

    try {
      HistoryResults historyResults = getParentHistoryResults(spoon, "Execution history");

      if ( historyResults != null ) {
        Session session = null;
        try {
          session = driver.session();
          lookupExecutionHistory( historyResults, spoon.getLog(), session, false, false );

          HistoryResultsDialog historyResultsDialog = new HistoryResultsDialog( spoon.getShell(), historyResults );
          historyResultsDialog.open();

        } finally {
          if ( session != null ) {
            session.close();
          }
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error examining logs", e );
    }
  }

  private HistoryResults getParentHistoryResults( Spoon spoon, String baseMessage ) {
    HistoryResults historyResults = null;

    JobGraph jobGraph = spoon.getActiveJobGraph();
    if ( jobGraph != null ) {
      JobMeta jobMeta = spoon.getActiveJob();

      historyResults = new HistoryResults();
      historyResults.setSubjectName( jobMeta.getName() );
      historyResults.setSubjectType( "JOB" );
      historyResults.setSubjectCopy( null );
      historyResults.setParentName( null );
      historyResults.setParentType( null );
      historyResults.setTopic( baseMessage+" of job'" + jobMeta.getName() + "'" );
    }
    TransGraph transGraph = spoon.getActiveTransGraph();
    if ( transGraph != null ) {
      TransMeta transMeta = spoon.getActiveTransformation();

      historyResults = new HistoryResults();
      historyResults.setSubjectName( transMeta.getName() );
      historyResults.setSubjectType( "TRANS" );
      historyResults.setSubjectCopy( null );
      historyResults.setParentName( null );
      historyResults.setParentType( null );
      historyResults.setTopic( baseMessage+" of transformation'" + transMeta.getName() + "'" );
    }

    return historyResults;
  }

  public void examineJobEntryLogs() {
    examineJobEntryLogs( false );
  }

  public void examineJobEntryErrorLogs() {
    examineJobEntryLogs( true );
  }

  public void examineJobEntryLogs( boolean errorsOnly ) {
    try {
      JobGraph jobGraph = spoon.getActiveJobGraph();
      JobMeta jobMeta = spoon.getActiveJob();
      JobEntryCopy jobEntry = jobGraph.getJobEntry();
      if ( driver == null || connection == null || jobGraph == null || jobMeta == null || jobEntry == null ) {
        return;
      }

      // Keep some information around
      HistoryResults historyResults = new HistoryResults();
      historyResults.setSubjectName( jobEntry.getName() );
      historyResults.setSubjectType( "JOBENTRY" );
      historyResults.setSubjectCopy( "0" );
      historyResults.setParentName( jobMeta.getName() );
      historyResults.setParentType( "JOB" );
      historyResults.setTopic( "Execution history of job entry '" + jobEntry.getName() + "'" );

      Session session = null;
      try {
        session = driver.session();
        lookupExecutionHistory( historyResults, spoon.getLog(), session, errorsOnly, false );

        HistoryResultsDialog historyResultsDialog = new HistoryResultsDialog( spoon.getShell(), historyResults );
        historyResultsDialog.open();

      } finally {
        if ( session != null ) {
          session.close();
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error examining job entry logs", e );
    }
  }

  public void lookupExecutionHistory( HistoryResults historyResults, LogChannelInterface log, Session session, boolean errorsOnly, boolean errorPath ) {

    boolean hasParent = StringUtils.isNotEmpty( historyResults.getParentName() );

    // First get the ID of the last :Execution ID
    //
    Map<String, Object> params = new HashMap<>();
    params.put( "subjectName", historyResults.getSubjectName() );
    params.put( "subjectType", historyResults.getSubjectType() );
    params.put( "parentName", historyResults.getParentName() );
    params.put( "parentType", historyResults.getParentType() );

    StringBuilder cypher = new StringBuilder();
    cypher.append( "MATCH(se:Execution { name : {subjectName}, type : {subjectType}}) " );
    if ( hasParent ) {
      cypher.append( "MATCH(te:Execution { name : {parentName}, type : {parentType}}) " );
      cypher.append( "MATCH(te)-[r:EXECUTES]->(se) " );
    }
    cypher.append( "WHERE se.registrationDate IS NOT NULL " );
    if ( hasParent ) {
      cypher.append( "  AND te.registrationDate IS NOT NULL " );
    }

    if ( errorsOnly ) {
      cypher.append( "  AND se.errors>0 " );
    }

    cypher.append( "RETURN se.id, se.name, se.type, se.copy, se.registrationDate, " );
    cypher.append( "        se.linesWritten, se.linesRead, se.linesInput, " );
    cypher.append( "        se.linesOutput, se.linesRejected, se.errors, se.durationMs," );
    cypher.append( "        se.loggingText, se.root " );
    cypher.append( " ORDER BY se.registrationDate DESC " );
    cypher.append( "LIMIT 10 " );

    // Get the last 10 execution IDs
    //
    StatementResult statementResult = session.run( cypher.toString(), params );
    while ( statementResult.hasNext() ) {
      Record record = statementResult.next();

      HistoryResult execution = new HistoryResult();

      int index = 0;
      String subjectLogChannelId = LoggingCore.getStringValue( record, index++ );
      execution.setId( subjectLogChannelId );
      execution.setName( LoggingCore.getStringValue( record, index++ ) );
      execution.setType( LoggingCore.getStringValue( record, index++ ) );
      execution.setCopy( LoggingCore.getStringValue( record, index++ ) );
      execution.setRegistrationDate( LoggingCore.getStringValue( record, index++ ) );
      execution.setWritten( LoggingCore.getLongValue( record, index++ ) );
      execution.setRead( LoggingCore.getLongValue( record, index++ ) );
      execution.setInput( LoggingCore.getLongValue( record, index++ ) );
      execution.setOutput( LoggingCore.getLongValue( record, index++ ) );
      execution.setRejected( LoggingCore.getLongValue( record, index++ ) );
      execution.setErrors( LoggingCore.getLongValue( record, index++ ) );
      execution.setDurationMs( LoggingCore.getLongValue( record, index++ ) );
      execution.setLoggingText( LoggingCore.getStringValue( record, index++ ) );
      execution.setRoot( LoggingCore.getBooleanValue( record, index++ ) );
      historyResults.getLastExecutions().add( execution );

      if ( execution.isRoot() == null || !execution.isRoot() || errorPath ) {
        // OK, we're still here.
        // We now know the unique key for the selected step's last execution in the given transformation
        //
        // Now get the path to the root job or transformation
        //
        Map<String, Object> pathParams = new HashMap<>();
        pathParams.put( "subjectName", historyResults.getSubjectName() );
        pathParams.put( "subjectType", historyResults.getSubjectType() );
        pathParams.put( "subjectId", subjectLogChannelId );

        StringBuilder pathCypher = new StringBuilder();
        if (errorPath) {

          // System.out.println( "Error path top ID : "+subjectLogChannelId );

          pathCypher.append( "MATCH(top:Execution { name : {subjectName}, type : {subjectType}, id : {subjectId}})-[rel:EXECUTES*]-(err:Execution) " );
          pathCypher.append( "   , p=shortestpath((top)-[:EXECUTES*]-(err)) " );
          pathCypher.append( "WHERE top.registrationDate IS NOT NULL " );
          pathCypher.append( "  AND err.errors > 0 " );
          pathCypher.append( "  AND size((err)-[:EXECUTES]->())=0 " );
          pathCypher.append( "RETURN p " );
          pathCypher.append( "ORDER BY size(RELATIONSHIPS(p)) DESC " );
          pathCypher.append( "LIMIT 5" );
        } else {
          pathCypher.append( "MATCH(se:Execution { name : {subjectName}, type : {subjectType}, id : {subjectId}}) " );
          pathCypher.append( "   , (je:Execution { root : true }) " );
          pathCypher.append( "   , p=shortestpath((se)-[:EXECUTES*]-(je)) " );
          pathCypher.append( "WHERE se.registrationDate IS NOT NULL " );
          pathCypher.append( "RETURN p " );
          pathCypher.append( "LIMIT 5 " );
        }

        StatementResult pathResult = session.run( pathCypher.toString(), pathParams );

        List<List<HistoryResult>> shortestPaths = execution.getShortestPaths();

        while ( pathResult.hasNext() ) {
          // System.out.println("Path found!");
          Record pathRecord = pathResult.next();
          Value pathValue = pathRecord.get( 0 );
          Path path = pathValue.asPath();
          List<HistoryResult> shortestPath = new ArrayList<>();
          for ( Node node : path.nodes() ) {
            HistoryResult pathExecution = new HistoryResult();
            pathExecution.setId( LoggingCore.getStringValue( node, "id" ) );
            pathExecution.setName( LoggingCore.getStringValue( node, "name" ) );
            // System.out.println(" - Node name : "+pathExecution.getName());
            pathExecution.setType( LoggingCore.getStringValue( node, "type" ) );
            pathExecution.setCopy( LoggingCore.getStringValue( node, "copy" ) );
            pathExecution.setRegistrationDate( LoggingCore.getStringValue( node, "registrationDate" ) );
            pathExecution.setWritten( LoggingCore.getLongValue( node, "linesWritten" ) );
            pathExecution.setRead( LoggingCore.getLongValue( node, "linesRead" ) );
            pathExecution.setInput( LoggingCore.getLongValue( node, "linesInput" ) );
            pathExecution.setOutput( LoggingCore.getLongValue( node, "linesOutput" ) );
            pathExecution.setRejected( LoggingCore.getLongValue( node, "linesRejected" ) );
            pathExecution.setErrors( LoggingCore.getLongValue( node, "errors" ) );
            pathExecution.setLoggingText( LoggingCore.getStringValue( node, "loggingText" ) );
            pathExecution.setDurationMs( LoggingCore.getLongValue( node, "durationMs" ) );

            if (errorPath) {
              shortestPath.add( 0, pathExecution );
            } else {
              shortestPath.add( pathExecution );
            }
          }
          shortestPaths.add( shortestPath );
        }
      }
    }
  }

  public Driver getDriver() {
    return driver;
  }

  /**
   * Look from the last job execution into the logs and retrieve a step or job entry with an error.
   * Then display the shortest path to that.
   */
  public void findError() {
    if ( driver == null || connection == null ) {
      return;
    }

    Session session = null;

    Spoon spoon = Spoon.getInstance();
    try {
      session = driver.session();

      // First, find the last execution of the current transformation or job.
      //
      HistoryResults historyResults = getParentHistoryResults( spoon, "Error analyses" );

      // Get error execution, find error paths to the problem
      //
      lookupExecutionHistory( historyResults, spoon.getLog(), session, false, true );

      HistoryResultsDialog historyResultsDialog = new HistoryResultsDialog( spoon.getShell(), historyResults, true );
      historyResultsDialog.open();

    } catch(Exception e) {
      new ErrorDialog(spoon.getShell(), "Error", "Error finding error path", e);
    } finally {
      session.close();
    }
  }
}

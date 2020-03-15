package com.neo4j.kettle.logging.trans;

import com.neo4j.kettle.logging.Defaults;
import com.neo4j.kettle.logging.util.LoggingCore;
import com.neo4j.kettle.logging.util.MetaStoreUtil;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.kettle.shared.NeoConnection;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingHierarchy;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobAdapter;
import org.pentaho.di.job.JobEntryResult;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.metastore.api.IMetaStore;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExtensionPoint(
  id = "JobLoggingExtensionPoint",
  extensionPointId = "JobStart",
  description = "Handle logging to Neo4j for a job"
)
public class JobLoggingExtensionPoint implements ExtensionPointInterface {

  public static final String JOB_START_DATE = "JOB_START_DATE";
  public static final String JOB_END_DATE = "JOB_END_DATE";

  public static final String EXECUTION_TYPE_JOB = LoggingObjectType.JOB.name();
  public static final String EXECUTION_TYPE_JOBENTRY = LoggingObjectType.JOBENTRY.name();

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof Job ) ) {
      return;
    }

    final Job job = (Job) object;

    // See if logging is enabled
    //
    if ( !LoggingCore.isEnabled( job ) ) {
      return;
    }

    // Keep the start date
    //
    job.getExtensionDataMap().put( JOB_START_DATE, new Date() );

    String connectionName = job.getVariable( Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION );

    try {

      // Which connection are we logging to?
      //
      IMetaStore metaStore = MetaStoreUtil.findMetaStore( job );
      if (metaStore==null) {
        log.logBasic( "Warning! Unable to find a metastore to load Neo4j connection to log to '" + connectionName +"'" );
        return;
      }
      final NeoConnection connection = LoggingCore.getConnection( metaStore, job );
      if ( connection == null ) {
        log.logBasic( "Warning! Unable to find Neo4j connection to log to : " + connectionName );
        return;
      }
      log.logDetailed( "Logging job information to Neo4j connection : " + connection.getName() );

      Session session = connection.getSession( log );

      logJobMetadata( log, session, connection, job );
      logStartOfJob( log, session, connection, job );

      job.addJobListener( new JobAdapter() {

        @Override public void jobFinished( Job job ) throws KettleException {
          logEndOfJob( log, session, connection, job );

          // If there are no other parents, we now have the complete log channel hierarchy
          //
          if ( job.getParentJob() == null && job.getParentTrans() == null ) {
            logHierarchy( log, session, connection, job.getLoggingHierarchy(), job.getLogChannelId() );
          }
        }
      } );

    } catch ( Exception e ) {
      // Let's not kill the job just yet, just log the error
      // otherwise: throw new KettleException(...);
      //
      log.logError( "Error logging to Neo4j:", e );
    }
  }

  private void logJobMetadata( final LogChannelInterface log, final Session session, final NeoConnection connection, final Job job ) throws KettleException {
    log.logDetailed( "Logging job metadata to Neo4j connection : " + connection.getName() );

    final JobMeta jobMeta = job.getJobMeta();

    synchronized ( session ) {
      session.writeTransaction( new TransactionWork<Void>() {
        @Override public Void execute( Transaction transaction ) {
          try {

            Map<String, Object> jobPars = new HashMap<>();
            jobPars.put( "jobName", jobMeta.getName() );
            jobPars.put( "description", jobMeta.getDescription() );
            jobPars.put( "filename", jobMeta.getFilename() );
            StringBuilder jobCypher = new StringBuilder();
            jobCypher.append( "MERGE (job:Job { name : $jobName} ) " );
            jobCypher.append( "SET job.filename = $filename, job.description = $description " );
            transaction.run( jobCypher.toString(), jobPars );

            log.logDetailed( "Trans cypher : " + jobCypher );

            for ( JobEntryCopy copy : jobMeta.getJobCopies() ) {

              Map<String, Object> entryPars = new HashMap<>();
              entryPars.put( "jobName", jobMeta.getName() );
              entryPars.put( "name", copy.getName() );
              entryPars.put( "description", copy.getDescription() );
              entryPars.put( "pluginId", copy.getEntry().getPluginId() );
              entryPars.put( "evaluation", copy.isEvaluation() );
              entryPars.put( "launchingParallel", copy.isLaunchingInParallel() );
              entryPars.put( "start", copy.isStart() );
              entryPars.put( "unconditional", copy.isUnconditional() );
              entryPars.put( "copyNr", Long.valueOf( copy.getNr() ) );
              entryPars.put( "locationX", copy.getLocation().x );
              entryPars.put( "locationY", copy.getLocation().y );
              entryPars.put( "drawn", copy.isDrawn() );

              StringBuilder entryCypher = new StringBuilder();
              entryCypher.append( "MATCH (job:Job { name : $jobName} ) " );
              entryCypher.append( "MERGE (entry:JobEntry { jobName : $jobName, name : $name}) " );
              entryCypher.append( "SET " );
              entryCypher.append( "   entry.description = $description " );
              entryCypher.append( ", entry.pluginId = $pluginId " );
              entryCypher.append( ", entry.evaluation = $evaluation " );
              entryCypher.append( ", entry.launchingParallel = $launchingParallel " );
              entryCypher.append( ", entry.start = $start " );
              entryCypher.append( ", entry.unconditional = $unconditional " );
              entryCypher.append( ", entry.copyNr = $copyNr " );
              entryCypher.append( ", entry.locationX = $locationX " );
              entryCypher.append( ", entry.locationY = $locationY " );
              entryCypher.append( ", entry.drawn = $drawn " );

              // Also update the relationship
              //
              entryCypher.append( "MERGE (entry)-[rel:JOBENTRY_OF_JOB]->(job) " );

              log.logDetailed( "JobEntry copy '" + copy.getName() + "' cypher : " + entryCypher );

              // run it
              //
              transaction.run( entryCypher.toString(), entryPars );
            }

            // Save hops
            //
            for ( int i = 0; i < jobMeta.nrJobHops(); i++ ) {
              JobHopMeta hopMeta = jobMeta.getJobHop( i );

              Map<String, Object> hopPars = new HashMap<>();
              hopPars.put( "fromEntry", hopMeta.getFromEntry().getName() );
              hopPars.put( "toEntry", hopMeta.getToEntry().getName() );
              hopPars.put( "jobName", jobMeta.getName() );

              StringBuilder hopCypher = new StringBuilder();
              hopCypher.append( "MATCH (from:JobEntry { jobName : $jobName, name : $fromEntry}) " );
              hopCypher.append( "MATCH (to:JobEntry { jobName : $jobName, name : $toEntry}) " );
              hopCypher.append( "MERGE (from)-[rel:PRECEDES]->(to) " );
              transaction.run( hopCypher.toString(), hopPars );
            }

            transaction.commit();
          } catch ( Exception e ) {
            transaction.rollback();
            log.logError( "Error logging job metadata", e );
          }
          return null;
        }
      } );
    }
  }

  private void logStartOfJob( final LogChannelInterface log, final Session session, final NeoConnection connection, final Job job ) throws KettleException {
    log.logDetailed( "Logging execution start of job to Neo4j connection : " + connection.getName() );

    final JobMeta jobMeta = job.getJobMeta();

    synchronized ( session ) {
      session.writeTransaction( new TransactionWork<Void>() {
        @Override public Void execute( Transaction transaction ) {
          try {
            // Create a new node for each log channel and it's owner
            // Start with the job
            //
            LogChannelInterface channel = job.getLogChannel();
            Date startDate = (Date) job.getExtensionDataMap().get( JOB_START_DATE );

            Map<String, Object> jobPars = new HashMap<>();
            jobPars.put( "jobName", jobMeta.getName() );
            jobPars.put( "id", channel.getLogChannelId() );
            jobPars.put( "type", EXECUTION_TYPE_JOB );
            jobPars.put( "executionStart", new SimpleDateFormat( "yyyy/MM/dd'T'HH:mm:ss" ).format( startDate ) );

            StringBuilder jobCypher = new StringBuilder();
            jobCypher.append( "MATCH (job:Job { name : $jobName} ) " );
            jobCypher.append( "MERGE (exec:Execution { name : $jobName, type : $type, id : $id} ) " );
            jobCypher.append( "SET " );
            jobCypher.append( " exec.executionStart = $executionStart " );
            jobCypher.append( "MERGE (exec)-[r:EXECUTION_OF_JOB]->(job) " );

            transaction.run( jobCypher.toString(), jobPars );

            transaction.commit();
          } catch ( Exception e ) {
            transaction.rollback();
            log.logError( "Error logging job start", e );
          }

          return null;
        }
      } );
    }
  }

  private void logEndOfJob( final LogChannelInterface log, final Session session, final NeoConnection connection, final Job job ) throws KettleException {
    log.logDetailed( "Logging execution end of job to Neo4j connection : " + connection.getName() );

    final JobMeta jobMeta = job.getJobMeta();

    synchronized ( session ) {
      session.writeTransaction( new TransactionWork<Void>() {
        @Override public Void execute( Transaction transaction ) {
          try {

            // Create a new node for each log channel and it's owner
            // Start with the job
            //
            LogChannelInterface channel = job.getLogChannel();
            Result jobResult = job.getResult();
            String jobLogChannelId = job.getLogChannelId();
            String jobLoggingText = KettleLogStore.getAppender().getBuffer( jobLogChannelId, true ).toString();

            Date endDate = new Date();
            job.getExtensionDataMap().put( JOB_END_DATE, new Date() );
            Date startDate = (Date) job.getExtensionDataMap().get( JOB_START_DATE );

            Map<String, Object> jobPars = new HashMap<>();
            jobPars.put( "jobName", jobMeta.getName() );
            jobPars.put( "type", EXECUTION_TYPE_JOB );
            jobPars.put( "id", channel.getLogChannelId() );
            jobPars.put( "executionEnd", new SimpleDateFormat( "yyyy/MM/dd'T'HH:mm:ss" ).format( endDate ) );
            jobPars.put( "durationMs", endDate.getTime() - startDate.getTime() );
            jobPars.put( "errors", jobResult.getNrErrors() );
            jobPars.put( "linesInput", jobResult.getNrLinesInput() );
            jobPars.put( "linesOutput", jobResult.getNrLinesOutput() );
            jobPars.put( "linesRead", jobResult.getNrLinesRead() );
            jobPars.put( "linesWritten", jobResult.getNrLinesWritten() );
            jobPars.put( "linesRejected", jobResult.getNrLinesRejected() );
            jobPars.put( "loggingText", jobLoggingText );
            jobPars.put( "result", jobResult.getResult() );
            jobPars.put( "nrResultRows", jobResult.getRows().size() );
            jobPars.put( "nrResultFiles", jobResult.getResultFilesList().size() );

            StringBuilder execCypher = new StringBuilder();
            execCypher.append( "MERGE (exec:Execution { name : $jobName, type : $type, id : $id } ) " );
            execCypher.append( "SET " );
            execCypher.append( "  exec.executionEnd = $executionEnd " );
            execCypher.append( ", exec.durationMs = $durationMs " );
            execCypher.append( ", exec.errors = $errors " );
            execCypher.append( ", exec.linesInput = $linesInput " );
            execCypher.append( ", exec.linesOutput = $linesOutput " );
            execCypher.append( ", exec.linesRead = $linesRead " );
            execCypher.append( ", exec.linesWritten = $linesWritten " );
            execCypher.append( ", exec.linesRejected = $linesRejected " );
            execCypher.append( ", exec.loggingText = $loggingText " );
            execCypher.append( ", exec.result = $result " );
            execCypher.append( ", exec.nrResultRows = $nrResultRows " );
            execCypher.append( ", exec.nrResultFiles = $nrResultFiles " );
            transaction.run( execCypher.toString(), jobPars );

            StringBuilder relCypher = new StringBuilder();
            relCypher.append( "MATCH (job:Job { name : $jobName } ) " );
            relCypher.append( "MATCH (exec:Execution { name : $jobName, type : $type, id : $id } ) " );
            relCypher.append( "MERGE (exec)-[r:EXECUTION_OF_JOB]->(job) " );
            transaction.run( relCypher.toString(), jobPars );

            // Also log every job entry execution results.
            //
            List<JobEntryResult> entryResults = job.getJobEntryResults();
            for ( JobEntryResult entryResult : entryResults ) {
              String entryLogChannelId = entryResult.getLogChannelId();
              String stepLoggingText = KettleLogStore.getAppender().getBuffer( entryLogChannelId, true ).toString();
              Result result = entryResult.getResult();
              Map<String, Object> entryPars = new HashMap<>();
              entryPars.put( "jobName", jobMeta.getName() );
              entryPars.put( "name", entryResult.getJobEntryName() );
              entryPars.put( "type", EXECUTION_TYPE_JOBENTRY );
              entryPars.put( "id", entryLogChannelId );
              entryPars.put( "jobId", jobLogChannelId );
              entryPars.put( "nr", entryResult.getJobEntryNr() );
              entryPars.put( "comment", entryResult.getComment() );
              entryPars.put( "reason", entryResult.getReason() );
              entryPars.put( "loggingText", stepLoggingText );
              entryPars.put( "errors", result.getNrErrors() );
              entryPars.put( "linesRead", result.getNrLinesRead() );
              entryPars.put( "linesWritten", result.getNrLinesWritten() );
              entryPars.put( "linesInput", result.getNrLinesInput() );
              entryPars.put( "linesOutput", result.getNrLinesOutput() );
              entryPars.put( "linesRejected", result.getNrLinesRejected() );

              StringBuilder entryExecCypher = new StringBuilder();
              entryExecCypher.append( "MERGE (exec:Execution { name : $name, type : $type, id : $id } ) " );
              entryExecCypher.append( "SET " );
              entryExecCypher.append( "  exec.jobId = $jobId " );
              entryExecCypher.append( ", exec.loggingText = $loggingText " );
              entryExecCypher.append( ", exec.nr = $nr " );
              entryExecCypher.append( ", exec.comment = $comment " );
              entryExecCypher.append( ", exec.reason = $reason " );
              entryExecCypher.append( ", exec.linesRead = $linesRead " );
              entryExecCypher.append( ", exec.linesWritten = $linesWritten " );
              entryExecCypher.append( ", exec.linesInput = $linesInput " );
              entryExecCypher.append( ", exec.linesOutput = $linesOutput " );
              entryExecCypher.append( ", exec.linesRejected = $linesRejected " );
              transaction.run( entryExecCypher.toString(), entryPars );

              StringBuilder entryRelCypher = new StringBuilder();
              entryRelCypher.append( "MATCH (entry:JobEntry { jobName : $jobName, name : $name } ) " );
              entryRelCypher.append( "MATCH (exec:Execution { name : $name, type : $type, id : $id } ) " );
              entryRelCypher.append( "MERGE (exec)-[r:EXECUTION_OF_JOBENTRY]->(entry) " );
              transaction.run( entryRelCypher.toString(), entryPars );
            }

            transaction.commit();
          } catch ( Exception e ) {
            transaction.rollback();
            log.logError( "Error logging job end", e );
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

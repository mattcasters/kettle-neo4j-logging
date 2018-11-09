package com.neo4j.kettle.logging.trans;

import com.neo4j.kettle.logging.util.LoggingCore;
import com.neo4j.kettle.logging.util.LoggingSession;
import com.neo4j.kettle.shared.NeoConnection;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingHierarchy;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobAdapter;
import org.pentaho.di.job.JobEntryResult;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;

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

  public static final String EXECUTION_TYPE_JOB = LoggingObjectType.JOB.name();
  public static final String EXECUTION_TYPE_JOBENTRY = LoggingObjectType.JOBENTRY.name();

  protected Date startDate;

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

    // This is executed right at the start of the execution
    //
    startDate = new Date();

    try {

      // Which connection are we logging to?
      //
      final NeoConnection connection = LoggingCore.getConnection( job.getJobMeta().getMetaStore(), job );
      log.logDetailed( "Logging job information to Neo4j connection : " + connection.getName() );

      Session session = LoggingSession.getInstance().getSession( connection );

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
            jobCypher.append( "MERGE (job:Job { name : {jobName}} ) " );
            jobCypher.append( "SET job.filename = {filename}, job.description = {description} " );
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
              entryCypher.append( "MATCH (job:Job { name : {jobName}} ) " );
              entryCypher.append( "MERGE (entry:JobEntry { jobName : {jobName}, name : {name}}) " );
              entryCypher.append( "SET " );
              entryCypher.append( "   entry.description = {description} " );
              entryCypher.append( ", entry.pluginId = {pluginId} " );
              entryCypher.append( ", entry.evaluation = {evaluation} " );
              entryCypher.append( ", entry.launchingParallel = {launchingParallel} " );
              entryCypher.append( ", entry.start = {start} " );
              entryCypher.append( ", entry.unconditional = {unconditional} " );
              entryCypher.append( ", entry.copyNr = {copyNr} " );
              entryCypher.append( ", entry.locationX = {locationX} " );
              entryCypher.append( ", entry.locationY = {locationY} " );
              entryCypher.append( ", entry.drawn = {drawn} " );

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
              hopCypher.append( "MATCH (from:JobEntry { jobName : {jobName}, name : {fromEntry}}) " );
              hopCypher.append( "MATCH (to:JobEntry { jobName : {jobName}, name : {toEntry}}) " );
              hopCypher.append( "MERGE (from)-[rel:PRECEDES]->(to) " );
              transaction.run( hopCypher.toString(), hopPars );
            }

            transaction.success();
          } catch ( Exception e ) {
            transaction.failure();
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
            Map<String, Object> jobPars = new HashMap<>();
            jobPars.put( "jobName", jobMeta.getName() );
            jobPars.put( "id", channel.getLogChannelId() );
            jobPars.put( "type", EXECUTION_TYPE_JOB );
            jobPars.put( "executionStart", new SimpleDateFormat( "yyyy/MM/dd'T'HH:mm:ss" ).format( startDate ) );

            StringBuilder jobCypher = new StringBuilder();
            jobCypher.append( "MATCH (job:Job { name : {jobName}} ) " );
            jobCypher.append( "MERGE (exec:Execution { name : {jobName}, type : {type}, id : {id}} ) " );
            jobCypher.append( "SET " );
            jobCypher.append( " exec.executionStart = {executionStart} " );
            jobCypher.append( "MERGE (exec)-[r:EXECUTION_OF_JOB]->(job) " );

            transaction.run( jobCypher.toString(), jobPars );

            transaction.success();
          } catch ( Exception e ) {
            transaction.failure();
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

            StringBuilder jobCypher = new StringBuilder();
            jobCypher.append( "MATCH (job:Job { name : {jobName}} ) " );
            jobCypher.append( "MERGE (exec:Execution { name : {jobName}, type : {type}, id : {id}} ) " );
            jobCypher.append( "SET " );
            jobCypher.append( "  exec.executionEnd = {executionEnd} " );
            jobCypher.append( ", exec.durationMs = {durationMs} " );
            jobCypher.append( ", exec.errors = {errors} " );
            jobCypher.append( ", exec.linesInput = {linesInput} " );
            jobCypher.append( ", exec.linesOutput = {linesOutput} " );
            jobCypher.append( ", exec.linesRead = {linesRead} " );
            jobCypher.append( ", exec.linesWritten = {linesWritten} " );
            jobCypher.append( ", exec.linesRejected = {linesRejected} " );
            jobCypher.append( ", exec.loggingText = {loggingText} " );
            jobCypher.append( ", exec.result = {result} " );
            jobCypher.append( ", exec.nrResultRows = {nrResultRows} " );
            jobCypher.append( ", exec.nrResultFiles = {nrResultFiles} " );
            jobCypher.append( "MERGE (exec)-[r:EXECUTION_OF_JOB]->(job) " );

            transaction.run( jobCypher.toString(), jobPars );

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

              StringBuilder entryCypher = new StringBuilder();
              entryCypher.append( "MATCH (entry:JobEntry { jobName : {jobName}, name : {name} } ) " );
              entryCypher.append( "MERGE (exec:Execution { name : {name}, type : {type}, id : {id}} ) " );
              entryCypher.append( "SET " );
              entryCypher.append( "  exec.jobId = {jobId} " );
              entryCypher.append( ", exec.loggingText = {loggingText} " );
              entryCypher.append( ", exec.nr = {nr} " );
              entryCypher.append( ", exec.comment = {comment} " );
              entryCypher.append( ", exec.reason = {reason} " );
              entryCypher.append( ", exec.linesRead = {linesRead} " );
              entryCypher.append( ", exec.linesWritten = {linesWritten} " );
              entryCypher.append( ", exec.linesInput = {linesInput} " );
              entryCypher.append( ", exec.linesOutput = {linesOutput} " );
              entryCypher.append( ", exec.linesRejected = {linesRejected} " );
              entryCypher.append( "MERGE (exec)-[r:EXECUTION_OF_JOBENTRY]->(entry) " );

              transaction.run( entryCypher.toString(), entryPars );
            }

            transaction.success();
          } catch ( Exception e ) {
            transaction.failure();
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

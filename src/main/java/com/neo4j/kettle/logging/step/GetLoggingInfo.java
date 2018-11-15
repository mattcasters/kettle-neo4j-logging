/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package com.neo4j.kettle.logging.step;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neo4j.kettle.logging.Defaults;
import com.neo4j.kettle.logging.util.LoggingCore;
import com.neo4j.kettle.shared.NeoConnection;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.job.Job;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.www.jaxrs.TransformationStatus;

/**
 * Get information from the System or the supervising transformation.
 *
 * @author Matt
 * @since 4-aug-2003
 */
public class GetLoggingInfo extends BaseStep implements StepInterface {
  private GetLoggingInfoMeta meta;
  private GetLoggingInfoData data;

  public GetLoggingInfo( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                         Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  private Object[] getLoggingInfo( RowMetaInterface inputRowMeta, Object[] inputRowData ) throws Exception {
    Object[] row = new Object[data.outputRowMeta.size()];
    for ( int i = 0; i < inputRowMeta.size(); i++ ) {
      row[i] = inputRowData[i]; // no data is changed, clone is not needed here.
    }
    for ( int i = 0, index = inputRowMeta.size(); i < meta.getFieldName().length; i++, index++ ) {
      Calendar cal;

      int argnr = 0;

      String argument = meta.getFieldArgument()[i];
      if ( StringUtils.isEmpty(argument)) {
        argument = getTrans().getTransMeta().getName();
      } else {
        argument = environmentSubstitute(argument);
      }

      switch ( meta.getFieldType()[i] ) {
        case TYPE_SYSTEM_INFO_TRANS_DATE_FROM: {
            Date previousSuccess = getPreviousTransSuccess( argument );
            if ( previousSuccess == null ) {
              previousSuccess = Const.MIN_DATE;
            }
            row[ index ] = previousSuccess;
          }
          break;
        case TYPE_SYSTEM_INFO_TRANS_DATE_TO:
          row[index] = getTrans().getCurrentDate();
          break;
        case TYPE_SYSTEM_INFO_JOB_DATE_FROM: {
            Date previousSuccess = getPreviousJobSuccess( argument );
            if ( previousSuccess == null ) {
              previousSuccess = Const.MIN_DATE;
            }
            row[ index ] = previousSuccess;
          }
          break;
        case TYPE_SYSTEM_INFO_JOB_DATE_TO:
          row[index] = getTrans().getCurrentDate();
          break;

        case TYPE_SYSTEM_INFO_TRANS_PREVIOUS_EXECUTION_DATE:
          row[index] = getPreviousTransExecution( argument );
          break;
        case TYPE_SYSTEM_INFO_TRANS_PREVIOUS_SUCCESS_DATE:
          row[index] = getPreviousTransSuccess( argument );
          break;
        case TYPE_SYSTEM_INFO_JOB_PREVIOUS_EXECUTION_DATE:
          row[index] = getPreviousJobExecution( argument );
          break;
        case TYPE_SYSTEM_INFO_JOB_PREVIOUS_SUCCESS_DATE:
          row[index] = getPreviousJobSuccess( argument );
          break;

        default:
          break;
      }
    }

    return row;
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    Object[] row;
    if ( data.readsRows ) {
      row = getRow();
      if ( row == null ) {
        setOutputDone();
        return false;
      }

      if ( first ) {
        first = false;
        data.outputRowMeta = getInputRowMeta().clone();
        meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
      }

    } else {
      row = new Object[] {}; // empty row
      incrementLinesRead();

      if ( first ) {
        first = false;
        data.outputRowMeta = new RowMeta();
        meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
      }
    }

    RowMetaInterface imeta = getInputRowMeta();
    if ( imeta == null ) {
      imeta = new RowMeta();
      this.setInputRowMeta( imeta );
    }

    try {
      row = getLoggingInfo( imeta, row );
    } catch(Exception e) {
      throw new KettleException( "Error getting Neo4j logging information", e );
    }

    if ( log.isRowLevel() ) {
      logRowlevel( "System info returned: " + data.outputRowMeta.getString( row ) );
    }

    putRow( data.outputRowMeta, row );

    if ( !data.readsRows ) {
      // Just one row and then stop!
      setOutputDone();
      return false;
    }

    return true;
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (GetLoggingInfoMeta) smi;
    data = (GetLoggingInfoData) sdi;

    if ( super.init( smi, sdi ) ) {
      data.readsRows = getStepMeta().getRemoteInputSteps().size() > 0;
      List<StepMeta> previous = getTransMeta().findPreviousSteps( getStepMeta() );
      if ( previous != null && previous.size() > 0 ) {
        data.readsRows = true;
      }

      return true;
    }
    return false;
  }

  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    super.dispose( smi, sdi );
  }

  private Date getPreviousTransExecution(String transformationName) throws Exception {

    final NeoConnection connection = LoggingCore.getConnection( getTrans().getMetaStore(), getTrans() );
    if (connection==null) {
      throw new KettleException("Unable to find logging Neo4j connection (variable "+Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION+")");
    }

    Map<String, Object> parameters = new HashMap<>(  );
    parameters.put( "type", "TRANS" );
    parameters.put( "trans", transformationName );
    parameters.put( "status", Trans.STRING_FINISHED );

    String cypher = "MATCH(e:Execution { type: {type}, name : {trans}}) "
      + "WHERE e.status = {status} "
      + "RETURN e.name AS Name, e.executionStart AS startDate, e.errors AS errors, e.id AS id "
      + "ORDER BY startDate DESC "
      + "LIMIT 1 ";

    StatementResult result = LoggingCore.executeCypher( log, connection, cypher, parameters );
    return getResultDate(result, "startDate");
  }

  private Date getPreviousTransSuccess(String transformationName) throws Exception {

    final NeoConnection connection = LoggingCore.getConnection( getTrans().getMetaStore(), getTrans() );
    if (connection==null) {
      throw new KettleException("Unable to find logging Neo4j connection (variable "+Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION+")");
    }

    Map<String, Object> parameters = new HashMap<>(  );
    parameters.put( "type", "TRANS" );
    parameters.put( "trans", transformationName );
    parameters.put( "status", Trans.STRING_FINISHED );

    String cypher = "MATCH(e:Execution { type: {type}, name : {trans}}) "
      + "WHERE e.errors = 0 "
      + "  AND e.status = {status} "
      + "RETURN e.name AS Name, e.executionStart AS startDate, e.errors AS errors, e.id AS id "
      + "ORDER BY startDate DESC "
      + "LIMIT 1 ";

    StatementResult result = LoggingCore.executeCypher( log, connection, cypher, parameters );
    return getResultDate(result, "startDate");
  }

  private Date getPreviousJobExecution(String jobName) throws Exception {

    final NeoConnection connection = LoggingCore.getConnection( getTrans().getMetaStore(), getTrans() );
    if (connection==null) {
      throw new KettleException("Unable to find logging Neo4j connection (variable "+Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION+")");
    }

    Map<String, Object> parameters = new HashMap<>(  );
    parameters.put( "type", "JOB" );
    parameters.put( "job", jobName );
    parameters.put( "status", Trans.STRING_FINISHED );

    String cypher = "MATCH(e:Execution { type: {type}, name : {job}}) "
      + "WHERE e.status = {status} "
      + "RETURN e.name AS Name, e.executionStart AS startDate, e.errors AS errors, e.id AS id "
      + "ORDER BY startDate DESC "
      + "LIMIT 1 ";

    StatementResult result = LoggingCore.executeCypher( log, connection, cypher, parameters );
    return getResultDate(result, "startDate");
  }

  private Date getPreviousJobSuccess(String jobName) throws Exception {

    final NeoConnection connection = LoggingCore.getConnection( getTrans().getMetaStore(), getTrans() );
    if (connection==null) {
      throw new KettleException("Unable to find logging Neo4j connection (variable "+Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION+")");
    }

    Map<String, Object> parameters = new HashMap<>(  );
    parameters.put( "type", "JOB" );
    parameters.put( "job", jobName );
    parameters.put( "status", Trans.STRING_FINISHED );

    String cypher = "MATCH(e:Execution { type: {type}, name : {job}}) "
      + "WHERE e.errors = 0 "
      + "  AND e.status = {status} "
      + "RETURN e.name AS Name, e.executionStart AS startDate, e.errors AS errors, e.id AS id "
      + "ORDER BY startDate DESC "
      + "LIMIT 1 ";

    StatementResult result = LoggingCore.executeCypher( log, connection, cypher, parameters );
    return getResultDate(result, "startDate");
  }

  private Date getResultDate( StatementResult result, String startDate ) throws ParseException {
    // One row, get it
    //
    if (result.hasNext()) {
      Record record = result.next();
      String string = record.get( "startDate" ).asString();// Dates in logging are in String formats
      return new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss").parse(string);
    }

    return null;
  }

}
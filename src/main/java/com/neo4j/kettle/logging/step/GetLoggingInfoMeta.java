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

import java.util.Arrays;
import java.util.List;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaNone;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/*
 * Created on 05-aug-2003
 *
 */
@Step(
  id = "GetLoggingInfo",
  name = "Get Neo4j Logging Info",
  description = "Queries the Neo4j logging graph and gets information back",
  categoryDescription = "Neo4j",
  image = "ui/images/SYS.svg"
)
@InjectionSupported( localizationPrefix = "GetLoggingInfoMeta.Injection." )
public class GetLoggingInfoMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = GetLoggingInfoMeta.class; // for i18n purposes, needed by Translator2!!

  @Injection( name = "FIELD_NAME" )
  private String[] fieldName;

  @Injection( name = "FIELD_TYPE", converter = GetLoggingInfoMetaInjectionTypeConverter.class )
  private GetLoggingInfoTypes[] fieldType;

  @Injection( name = "FIELD_ARGUMENT" )
  private String[] fieldArgument;


  public GetLoggingInfoMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the fieldName.
   */
  public String[] getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName
   *          The fieldName to set.
   */
  public void setFieldName( String[] fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * @return Returns the fieldType.
   */
  public GetLoggingInfoTypes[] getFieldType() {
    return fieldType;
  }

  /**
   * @param fieldType
   *          The fieldType to set.
   */
  public void setFieldType( GetLoggingInfoTypes[] fieldType ) {
    this.fieldType = fieldType;
  }

  /**
   * Gets fieldArgument
   *
   * @return value of fieldArgument
   */
  public String[] getFieldArgument() {
    return fieldArgument;
  }

  /**
   * @param fieldArgument The fieldArgument to set
   */
  public void setFieldArgument( String[] fieldArgument ) {
    this.fieldArgument = fieldArgument;
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  public void allocate( int count ) {
    fieldName = new String[count];
    fieldType = new GetLoggingInfoTypes[count];
    fieldArgument = new String[count];
  }

  @Override
  public Object clone() {
    GetLoggingInfoMeta retval = (GetLoggingInfoMeta) super.clone();

    int count = fieldName.length;

    retval.allocate( count );

    System.arraycopy( fieldName, 0, retval.fieldName, 0, count );
    System.arraycopy( fieldType, 0, retval.fieldType, 0, count );
    System.arraycopy( fieldArgument, 0, retval.fieldArgument, 0, count );

    return retval;
  }

  private void readData( Node stepnode ) throws KettleXMLException {
    try {
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int count = XMLHandler.countNodes( fields, "field" );
      String type;

      allocate( count );

      for ( int i = 0; i < count; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        fieldName[i] = XMLHandler.getTagValue( fnode, "name" );
        type = XMLHandler.getTagValue( fnode, "type" );
        fieldType[i] = GetLoggingInfoTypes.getTypeFromString( type );
        fieldArgument[i] = XMLHandler.getTagValue( fnode, "argument" );
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to read step information from XML", e );
    }
  }

  @Override
  public void setDefault() {
    allocate( 4 );

    fieldName[0] = "startOfTransDelta";
    fieldType[0] = GetLoggingInfoTypes.TYPE_SYSTEM_INFO_TRANS_DATE_FROM;
    fieldName[1] = "endOfTransDelta";
    fieldType[1] = GetLoggingInfoTypes.TYPE_SYSTEM_INFO_TRANS_DATE_TO;
    fieldName[2] = "startOfJobDelta";
    fieldType[2] = GetLoggingInfoTypes.TYPE_SYSTEM_INFO_JOB_DATE_FROM;
    fieldName[3] = "endOfJobDelta";
    fieldType[3] = GetLoggingInfoTypes.TYPE_SYSTEM_INFO_JOB_DATE_TO;
  }

  @Override
  public void getFields( RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    for ( int i = 0; i < fieldName.length; i++ ) {
      ValueMetaInterface v;

      switch ( fieldType[i] ) {
        case TYPE_SYSTEM_INFO_TRANS_DATE_FROM:
        case TYPE_SYSTEM_INFO_TRANS_DATE_TO:
        case TYPE_SYSTEM_INFO_JOB_DATE_FROM:
        case TYPE_SYSTEM_INFO_JOB_DATE_TO:
        case TYPE_SYSTEM_INFO_TRANS_PREVIOUS_EXECUTION_DATE:
        case TYPE_SYSTEM_INFO_TRANS_PREVIOUS_SUCCESS_DATE:
        case TYPE_SYSTEM_INFO_JOB_PREVIOUS_EXECUTION_DATE:
        case TYPE_SYSTEM_INFO_JOB_PREVIOUS_SUCCESS_DATE:
          v = new ValueMetaDate( fieldName[i] );
          break;
        default:
          v = new ValueMetaNone( fieldName[i] );
          break;
      }
      v.setOrigin( name );
      row.addValueMeta( v );
    }
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    <fields>" + Const.CR );

    for ( int i = 0; i < fieldName.length; i++ ) {
      retval.append( "      <field>" + Const.CR );
      retval.append( "        " + XMLHandler.addTagValue( "name", fieldName[i] ) );
      retval.append( "        " + XMLHandler.addTagValue( "type",
              fieldType[i] != null ? fieldType[i].getCode() : "" ) );
      retval.append( "        " + XMLHandler.addTagValue( "argument", fieldArgument[i] ) );
      retval.append( "        </field>" + Const.CR );
    }
    retval.append( "      </fields>" + Const.CR );

    return retval.toString();
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    try {
      int nrfields = rep.countNrStepAttributes( id_step, "field_name" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        fieldName[i] = rep.getStepAttributeString( id_step, i, "field_name" );
        fieldType[i] = GetLoggingInfoTypes.getTypeFromString( rep.getStepAttributeString( id_step, i, "field_type" ) );
        fieldArgument[i] = rep.getStepAttributeString( id_step, i, "field_argument" );
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    try {
      for ( int i = 0; i < fieldName.length; i++ ) {
        rep.saveStepAttribute( id_transformation, id_step, i, "field_name", fieldName[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "field_type", fieldType[i] != null ? fieldType[i]
            .getCode() : "" );
        rep.saveStepAttribute( id_transformation, id_step, i, "field_argument", fieldArgument[i] );
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }

  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    Repository repository, IMetaStore metaStore ) {
    // See if we have input streams leading to this step!
    int nrRemarks = remarks.size();
    for ( int i = 0; i < fieldName.length; i++ ) {
      if ( fieldType[i].ordinal() <= GetLoggingInfoTypes.TYPE_SYSTEM_INFO_NONE.ordinal() ) {
        CheckResult cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "SystemDataMeta.CheckResult.FieldHasNoType", fieldName[i] ), stepMeta );
        remarks.add( cr );
      }
    }
    if ( remarks.size() == nrRemarks ) {
      CheckResult cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SystemDataMeta.CheckResult.AllTypesSpecified" ), stepMeta );
      remarks.add( cr );
    }
  }


  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans ) {
    return new GetLoggingInfo( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new GetLoggingInfoData();
  }

  @Override
  public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( !( o instanceof GetLoggingInfoMeta ) ) {
      return false;
    }
    GetLoggingInfoMeta that = (GetLoggingInfoMeta) o;

    if ( !Arrays.equals( fieldName, that.fieldName ) ) {
      return false;
    }
    if ( !Arrays.equals( fieldType, that.fieldType ) ) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode( fieldName );
    result = 31 * result + Arrays.hashCode( fieldType );
    return result;
  }
}
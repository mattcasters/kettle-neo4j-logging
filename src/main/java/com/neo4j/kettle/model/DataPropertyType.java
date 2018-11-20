package com.neo4j.kettle.model;

import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.time.ZoneId;

public enum DataPropertyType {
  String,
  Integer,
  Float,
  Boolean,
  Date,
  LocalDateTime,
  ByteArray,
  Time,
  Point,
  Duration,
  LocalTime,
  DateTime;

  /**
   * Get the code for a type, handles the null case
   *
   * @param type
   * @return
   */

  public static String getCode( DataPropertyType type ) {
    if ( type == null ) {
      return null;
    }
    return type.name();
  }

  /**
   * Default to String in case we can't recognize the code or is null
   *
   * @param code
   * @return
   */
  public static DataPropertyType parseCode( String code ) {
    if ( code == null ) {
      return String;
    }
    try {
      return DataPropertyType.valueOf( code );
    } catch ( IllegalArgumentException e ) {
      return String;
    }
  }

  public static String[] getNames() {
    String[] names = new String[ values().length ];
    for ( int i = 0; i < names.length; i++ ) {
      names[ i ] = values()[ i ].name();
    }
    return names;
  }


  /**
   * Convert the given Kettle value to a Neo4j data type
   *
   * @param valueMeta
   * @param valueData
   * @return
   */
  public Object convertFromKettle( ValueMetaInterface valueMeta, Object valueData ) throws KettleValueException {

    if ( valueMeta.isNull( valueData ) ) {
      return null;
    }
    switch ( this ) {
      case String:
        return valueMeta.getString( valueData );
      case Boolean:
        return valueMeta.getBoolean( valueData );
      case Float:
        return valueMeta.getNumber( valueData );
      case Integer:
        return valueMeta.getInteger( valueData );
      case Date:
        return valueMeta.getDate( valueData ).toInstant().atZone( ZoneId.systemDefault() ).toLocalDate();
      case LocalDateTime:
        return valueMeta.getDate( valueData ).toInstant().atZone( ZoneId.systemDefault() ).toLocalDateTime();
      case ByteArray:
        return valueMeta.getBinary( valueData );
      case Duration:
      case DateTime:
      case Time:
      case Point:
      case LocalTime:
      default:
        throw new KettleValueException(
          "Data conversion to Neo4j type '" + name() + "' from value '" + valueMeta.toStringMeta() + "' is not supported yet" );
    }
  }

  public static final DataPropertyType getTypeFromKettle( ValueMetaInterface valueMeta ) {
    switch ( valueMeta.getType() ) {
      case ValueMetaInterface.TYPE_STRING:
        return DataPropertyType.String;
      case ValueMetaInterface.TYPE_NUMBER:
        return DataPropertyType.Float;
      case ValueMetaInterface.TYPE_DATE:
        return DataPropertyType.LocalDateTime;
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return DataPropertyType.LocalDateTime;
      case ValueMetaInterface.TYPE_BOOLEAN:
        return DataPropertyType.Boolean;
      case ValueMetaInterface.TYPE_BINARY:
        return DataPropertyType.ByteArray;
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return DataPropertyType.String;
      case ValueMetaInterface.TYPE_INTEGER:
        return DataPropertyType.Integer;
      default:
        return DataPropertyType.String;
    }
  }

}
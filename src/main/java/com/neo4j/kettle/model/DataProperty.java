package com.neo4j.kettle.model;

import org.neo4j.driver.v1.types.Type;

public class DataProperty {

  private String id;

  private Object value;

  private Type type;

  public DataProperty() {
  }

  public DataProperty( String id, Object value, Type type ) {
    this.id = id;
    this.value = value;
    this.type = type;
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id The id to set
   */
  public void setId( String id ) {
    this.id = id;
  }

  /**
   * Gets value
   *
   * @return value of value
   */
  public Object getValue() {
    return value;
  }

  /**
   * @param value The value to set
   */
  public void setValue( Object value ) {
    this.value = value;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public Type getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( Type type ) {
    this.type = type;
  }
}

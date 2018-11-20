package com.neo4j.kettle.model;

import org.neo4j.driver.v1.types.Relationship;

import java.util.ArrayList;
import java.util.List;

public class DataRelationship {

  private String id;

  private String label;

  private List<DataProperty> properties;

  private String nodeSource;

  private String nodeTarget;

  public DataRelationship() {
    properties = new ArrayList<>();
  }

  public DataRelationship( String id, String label, List<DataProperty> properties, String nodeSource, String nodeTarget ) {
    this.id = id;
    this.label = label;
    this.properties = properties;
    this.nodeSource = nodeSource;
    this.nodeTarget = nodeTarget;
  }

  @Override public boolean equals( Object o ) {
    if ( o == null ) {
      return false;
    }
    if ( !( o instanceof DataRelationship ) ) {
      return false;
    }
    if ( o == this ) {
      return true;
    }
    return ( (DataRelationship) o ).getId().equalsIgnoreCase( id );
  }

  @Override public String toString() {
    return id == null ? super.toString() : id;
  }

  public DataRelationship( DataRelationship graphRelationship ) {
    this();

    setId(graphRelationship.getId());
    setLabel( graphRelationship.getLabel() );
    setNodeSource( graphRelationship.getNodeSource() );
    setNodeTarget( graphRelationship.getNodeTarget() );

    List<DataProperty> properties = new ArrayList<>();
    for ( DataProperty property : graphRelationship.getProperties() ) {
      properties.add( new DataProperty( property.getId(), property.getValue(), property.getType() ) );
    }
    setProperties( properties );
  }

  public DataRelationship( Relationship relationship ) {
    setId( Long.toString( relationship.id() ) );
    setNodeSource( Long.toString( relationship.startNodeId() ) );
    setNodeTarget( Long.toString( relationship.endNodeId() ) );
    setLabel( relationship.type() );
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
   * Gets label
   *
   * @return value of label
   */
  public String getLabel() {
    return label;
  }

  /**
   * @param label The label to set
   */
  public void setLabel( String label ) {
    this.label = label;
  }

  /**
   * Gets properties
   *
   * @return value of properties
   */
  public List<DataProperty> getProperties() {
    return properties;
  }

  /**
   * @param properties The properties to set
   */
  public void setProperties( List<DataProperty> properties ) {
    this.properties = properties;
  }

  /**
   * Gets nodeSource
   *
   * @return value of nodeSource
   */
  public String getNodeSource() {
    return nodeSource;
  }

  /**
   * @param nodeSource The nodeSource to set
   */
  public void setNodeSource( String nodeSource ) {
    this.nodeSource = nodeSource;
  }

  /**
   * Gets nodeTarget
   *
   * @return value of nodeTarget
   */
  public String getNodeTarget() {
    return nodeTarget;
  }

  /**
   * @param nodeTarget The nodeTarget to set
   */
  public void setNodeTarget( String nodeTarget ) {
    this.nodeTarget = nodeTarget;
  }
}

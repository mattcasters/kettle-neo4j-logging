package com.neo4j.kettle.model;

import org.eclipse.swt.graphics.Point;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.pentaho.di.core.Const;

import java.util.ArrayList;
import java.util.List;

public class DataNode {

  private String id;

  private List<String> labels;

  private List<DataProperty> properties;

  private DataPresentation presentation;

  public DataNode() {
    labels = new ArrayList<>();
    properties = new ArrayList<>();
    presentation = new DataPresentation( 0,0 );
  }

  public DataNode( String id ) {
    this();
    this.id = id;
  }

  public DataNode( String id, List<String> labels, List<DataProperty> properties, DataPresentation presentation ) {
    this.id = id;
    this.labels = labels;
    this.properties = properties;
    this.presentation = presentation;
  }

  public DataNode( Node node ) {
    this();
    this.id = Long.toString( node.id() );
    for ( String label : node.labels() ) {
      labels.add( label );
    }
    for ( String propertyKey : node.keys() ) {
      Value propertyValue = node.get( propertyKey );
      Object propertyObject = propertyValue.asObject();
      properties.add( new DataProperty( propertyKey, propertyObject, propertyValue.type() ) );
    }
  }


  public DataNode( DataNode graphNode ) {
    this();
    setId( graphNode.getId() );


    // Copy labels
    setLabels( new ArrayList<>( graphNode.getLabels() ) );

    // Copy properties
    List<DataProperty> propertiesCopy = new ArrayList<>();
    for ( DataProperty property : graphNode.getProperties() ) {
      DataProperty propertyCopy = new DataProperty( property.getId(), property.getValue(), property.getType());
      propertiesCopy.add( propertyCopy );
    }
    setProperties( propertiesCopy );
    setPresentation( graphNode.getPresentation().clone() );
  }

  @Override public String toString() {
    return id == null ? super.toString() : id;
  }

  @Override public boolean equals( Object o ) {
    if ( o == null ) {
      return false;
    }
    if ( !( o instanceof DataNode ) ) {
      return false;
    }
    if ( o == this ) {
      return true;
    }
    return ( (DataNode) o ).getId().equals( id );
  }

  /**
   * Search for the property with the given ID, case insensitive
   *
   * @param id the name of the property to look for
   * @return the property or null if nothing could be found.
   */
  public DataProperty findProperty( String id ) {
    for ( DataProperty property : properties ) {
      if ( property.getId().equalsIgnoreCase( id ) ) {
        return property;
      }
    }
    return null;
  }

  /**
   * Find a String property called name
   *
   * @return the name property string or if not available, the ID
   */
  public String getName() {
    DataProperty nameProperty = findProperty( "name" );
    if (nameProperty==null || nameProperty.getValue()==null) {
      return id;
    }
    return nameProperty.getValue().toString();
  }


  public String getNodeText() {
    String nodeText = getName()+ Const.CR;
    DataProperty typeProperty = findProperty( "type" );
    if (typeProperty!=null && typeProperty.getValue()!=null) {
      nodeText+=typeProperty.getValue().toString();
    }
    if (labels.size()>0) {
      nodeText += " (:" + labels.get( 0 ) + ")";
    }

    return nodeText;
  }

  public Point getLocation() {
    return new Point(presentation.getX(), presentation.getY());
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
   * Gets labels
   *
   * @return value of labels
   */
  public List<String> getLabels() {
    return labels;
  }

  /**
   * @param labels The labels to set
   */
  public void setLabels( List<String> labels ) {
    this.labels = labels;
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
   * Gets presentation
   *
   * @return value of presentation
   */
  public DataPresentation getPresentation() {
    return presentation;
  }

  /**
   * @param presentation The presentation to set
   */
  public void setPresentation( DataPresentation presentation ) {
    this.presentation = presentation;
  }

}

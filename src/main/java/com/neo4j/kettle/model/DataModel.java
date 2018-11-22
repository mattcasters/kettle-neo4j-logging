package com.neo4j.kettle.model;


import com.neo4j.kettle.logging.util.LoggingCore;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Display;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.pentaho.di.core.Const;
import org.pentaho.di.ui.core.gui.GUIResource;

import java.awt.geom.Line2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DataModel {
  private String name;

  private List<DataNode> nodes;

  private List<DataRelationship> relationships;

  public DataModel() {
    nodes = new ArrayList<>();
    relationships = new ArrayList<>();
  }

  public DataModel( String name, List<DataNode> nodes, List<DataRelationship> relationships ) {
    this.name = name;
    this.nodes = nodes;
    this.relationships = relationships;
  }

  @Override public boolean equals( Object object ) {
    if ( object == null ) {
      return false;
    }
    if ( !( object instanceof DataModel ) ) {
      return false;
    }
    if ( object == this ) {
      return true;
    }
    return ( (DataModel) object ).getName().equalsIgnoreCase( name );
  }

  public DataModel( DataModel source ) {
    this();
    replace( source );
  }

  public void replace( DataModel source ) {
    setName( source.getName() );

    // Copy nodes
    //
    nodes = new ArrayList<>();
    for ( DataNode node : source.getNodes() ) {
      nodes.add( new DataNode( node ) );
    }

    // replace relationships
    //
    relationships = new ArrayList<>();
    for ( DataRelationship relationship : source.getRelationships() ) {
      relationships.add( new DataRelationship( relationship ) );
    }
  }


  /**
   * Find a node with the given ID
   *
   * @param nodeId
   * @return The mode with the given ID or null if the node was not found
   */
  public DataNode findNode( String nodeId ) {
    for ( DataNode node : nodes ) {
      if ( node.getId().equals( nodeId ) ) {
        return node;
      }
    }
    return null;
  }


  /**
   * Find a relationship with the given ID
   *
   * @param relationshipId
   * @return The relationship with the given name or null if the relationship was not found
   */
  public DataRelationship findRelationship( String relationshipId ) {
    for ( DataRelationship relationship : relationships ) {
      if ( relationship.getId().equals( relationshipId ) ) {
        return relationship;
      }
    }
    return null;
  }

  /**
   * Find a relationship with source and target
   *
   * @param sourceId
   * @param targetId
   * @return the relationship or null if nothing was found.
   */
  public DataRelationship findRelationship( String sourceId, String targetId ) {
    for ( DataRelationship relationship : relationships ) {
      if ( relationship.getNodeSource().equals( sourceId ) &&
        relationship.getNodeTarget().equals( targetId ) ) {
        return relationship;
      }
      // Also match on the inverse
      if ( relationship.getNodeSource().equals( targetId) &&
        relationship.getNodeTarget().equals( sourceId) ) {
        return relationship;
      }

    }
    return null;
  }

  public DataModel( String modelName, StatementResult result ) {
    this();

    setName( modelName );

    while ( result.hasNext() ) {
      Record record = result.next();
      for ( String key : record.keys() ) {
        Value value = record.get( key );
        if ( "NODE".equals( value.type().name() ) ) {
          Node node = value.asNode();
          update( new DataNode( node ) );
        } else if ( "RELATIONSHIP".equals( value.type().name() ) ) {
          Relationship relationship = value.asRelationship();
          update( new DataRelationship( relationship ) );
        } else if ( "PATH".equals( value.type().name() ) ) {
          Path path = value.asPath();
          for ( Node node : path.nodes() ) {
            update( new DataNode( node ) );
          }
          for ( Relationship relationship : path.relationships() ) {
            update( new DataRelationship( relationship ) );
          }
        }
      }
    }
  }

  private void update( DataNode dataNode ) {
    int index = nodes.indexOf( dataNode );
    if ( index < 0 ) {
      nodes.add( dataNode );
    } else {
      nodes.set( index, dataNode );
    }
  }

  private void update( DataRelationship dataRelationship ) {
    int index = relationships.indexOf( dataRelationship );
    if ( index < 0 ) {
      relationships.add( dataRelationship );
    } else {
      relationships.set( index, dataRelationship );
    }
  }

  public DataNode findNodeWithProperty( String propertyId, Object value ) {
    for (DataNode node : nodes) {
      DataProperty property = node.findProperty( propertyId );
      if (property!=null) {
        if (property.getValue()!=null && property.getValue().equals( value )) {
          return node;
        }
      }
    }
    return null;
  }

  public List<DataRelationship> findRelationships(String label) {
    List<DataRelationship> rels = new ArrayList<>(  );
    for (DataRelationship relationship : relationships) {
      if (relationship.getLabel().equals( label )) {
        rels.add(relationship);
      }
    }
    return rels;
  }

  public DataNode findTopNode( String labelToFollow) {

    // Start from any relationship with given label
    //
    List<DataRelationship> rels = findRelationships( labelToFollow );
    // System.out.println("Found "+rels.size()+" relationships for "+labelToFollow);
    if ( rels.size()==0) {
      return null;
    }
    DataRelationship rel = rels.get(0);
    DataNode node = null;
    while (rel!=null) {
      node = findNode( rel.getNodeSource() );
      rel = findRelationshipsWithTarget( rels, node.getId() );
    }
    return node;
  }

  public DataRelationship findRelationshipsWithTarget(List<DataRelationship> rels, String targetId ) {
    for (DataRelationship relationship : rels) {
      if (relationship.getNodeTarget().equals( targetId )) {
        return relationship;
      }
    }
    return null;
  }

  public DataRelationship findRelationshipsWithSource(List<DataRelationship> rels, String sourceId ) {
    for (DataRelationship relationship : rels) {
      if (relationship.getNodeSource().equals( sourceId)) {
        return relationship;
      }
    }
    return null;
  }

  public interface BestScoreListener {
    public void newBestScoreFound( Scoring scoring, DataModel dataModel );
  }

  private List<BestScoreListener> bestScoreListeners = new ArrayList<>();

  /**
   * Gets bestScoreListeners
   *
   * @return value of bestScoreListeners
   */
  public List<BestScoreListener> getBestScoreListeners() {
    return bestScoreListeners;
  }

  /**
   * Layout the nodes on a main axis left-to-right
   * Put additional nodes below split
   *
   * @param startNode             Where to start on the left side.
   * @param mainRelationshipLabel
   */
  public void treeLayout( Display display, DataNode startNode, String mainRelationshipLabel, int leftMargin, int topMargin, int spacingX, int spacingY ) {

    List<Point> nodeSizes = calculateNodeSizes(display);

    DataNode previousNode = null;

    int currentX = leftMargin;
    int currentY = topMargin;

    List<DataNode> mainPath = getPath(startNode, mainRelationshipLabel);
    List<Point> mainSizes = new ArrayList<>( );
    int maxWidth = 0;
    for (DataNode pathNode : mainPath) {
      Point nodeSize = findNodeSize( nodeSizes, pathNode );
      if (maxWidth<nodeSize.x) {
        maxWidth = nodeSize.x;
      }
      mainSizes.add( nodeSize );
    }

    for (int p=0;p<mainPath.size();p++) {
      DataNode currentNode = mainPath.get( p );
      Point currentSize = mainSizes.get(p);

      // System.out.println( "Current node : "+( currentNode.getNodeText().replace( Const.CR, " - " ) ) );

      // Set the location of the current node
      //
      int nodeX = currentX+(maxWidth-currentSize.x)/2;
      currentNode.getPresentation().setLocation(nodeX, currentY);

      List<DataNode> nextNodes = findNextNodes( currentNode, mainRelationshipLabel );
      for (int i=0;i<nextNodes.size();i++) {
        DataNode nextNode = nextNodes.get(i);
        // System.out.println( " - Next node : "+(currentNode.getNodeText().replace( Const.CR, " - " ) ) );
        Point nextSize = findNodeSize( nodeSizes, nextNode );

        nextNode.getPresentation().setLocation( currentX + 2*spacingX, currentY+i*(spacingY) );
      }

      int rowsDown = nextNodes.isEmpty() ? 1 : nextNodes.size();
      currentY += spacingY*rowsDown;

      DataNode nextNode = findNextNode( currentNode, mainRelationshipLabel, previousNode );

      previousNode = currentNode;
    }
  }

  private List<DataNode> getPath( DataNode startNode, String mainRelationshipLabel ) {
    DataNode currentNode = startNode;
    DataNode previousNode = null;
    List<DataNode> path = new ArrayList<>(  );
    while (currentNode!=null) {
      path.add(currentNode);
      DataNode nextNode = findNextNode( currentNode, mainRelationshipLabel, previousNode );
      previousNode = currentNode;
      currentNode = nextNode;
    }
    return path;
  }

  private DataNode getOtherNode( DataRelationship nextRel, DataNode currentNode ) {
    if (nextRel.getNodeSource().equals( currentNode.getId() )) {
      return findNode(nextRel.getNodeTarget());
    }
    if (nextRel.getNodeTarget().equals( currentNode.getId() )) {
      return findNode( nextRel.getNodeSource() );
    }
    return null;
  }

  private Point findNodeSize( List<Point> nodeSizes, DataNode dataNode ) {
    int index = nodes.indexOf( dataNode );
    if (index<0) {
      return null;
    }
    return nodeSizes.get( index );
  }

  public int getFirstRelationshipLabelIndex(List<DataRelationship> rels, String mainRelationshipLabel) {
    for (int i=0;i<rels.size();i++) {
      DataRelationship relationship = rels.get(i);
      if (mainRelationshipLabel.equals( relationship.getLabel() )) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Find all next nodes from the given current node. Exclude the main relationship label given
   * @param currentNode
   * @param excludeLabel
   * @return
   */
  private List<DataNode> findNextNodes( DataNode currentNode, String excludeLabel ) {

    List<DataNode> nextNodes = new ArrayList<>();
    for (DataRelationship relationship : relationships) {

      if (!relationship.getLabel().equals( excludeLabel )) {
        if ( relationship.getNodeSource().equals( currentNode.getId() ) ) {
          nextNodes.add(findNode( relationship.getNodeTarget() ));
        }
        if ( relationship.getNodeTarget().equals( currentNode.getId() ) ) {
          nextNodes.add(findNode( relationship.getNodeSource() ));
        }
      }
    }
    return nextNodes;
  }

  public DataNode findNextNode( DataNode currentNode, String mainRelationshipLabel, DataNode excludeNode ) {
    List<DataRelationship> rels = findRelationships( currentNode );
    for (DataRelationship rel : rels) {
      if (mainRelationshipLabel.equals( rel.getLabel() )) {
        if (excludeNode==null || !(rel.getNodeSource().equals( excludeNode.getId()) || rel.getNodeTarget().equals( excludeNode.getId() ))) {
          // Don't return the same node, return the other
          //
          if ( rel.getNodeSource().equals( currentNode.getId() ) ) {
            return findNode( rel.getNodeTarget() );
          } else {
            return findNode( rel.getNodeSource() );
          }
        }
      }
    }
    return null;
  }

  /**
   * Get the list of relationships involved in the given node (source or target)
   *
   * @param dataNode
   * @return
   */
  public List<DataRelationship> findRelationships( DataNode dataNode ) {

    List<DataRelationship> found = new ArrayList<>();
    for (DataRelationship relationship : relationships) {
      if (relationship.getNodeSource().equals(dataNode.getId()) || relationship.getNodeTarget().equals( dataNode.getId() )) {
        found.add( relationship );
      }
    }
    return found;
  }


  /**
   * Do automatic layout of this data model
   *
   * @param bounds
   * @param cutOffTimeMs the cutoff time in ms
   * @return the chosen score.
   */
  public Scoring autoLayout( Display display, Rectangle bounds, long cutOffTimeMs ) {

    if ( nodes.size() == 0 ) {
      // Nothing to see here, move along!
      //
      return null;
    }

    long startTime = System.currentTimeMillis();

    // Height is usually the limiting factor
    // Take half of it
    //
    nodeCache = new HashMap<>();

    double optDistance = LoggingCore.calculateOptDistance( bounds, nodes.size() );
    int nrNodes = nodes.size();

    java.util.List<Point> nodesizes = calculateNodeSizes(display);

    // Generate list of random points in the bounding box
    //
    java.util.List<Point> bestCoordinates = generateRandomPoints( bounds, nrNodes ); ;
    Scoring bestScore = calculateGraphScore( bestCoordinates, bounds, optDistance, nodesizes );

    long iterations = 1;

    while ( ( System.currentTimeMillis() - startTime ) < cutOffTimeMs ) {

      // Change one random point
      //
      java.util.List<Point> testCoordinates = generateRandomPoints( bounds, nrNodes ); // modifyRandomPoints( bestCoordinates, bounds, (nodes.size()/2)+1 );
      Scoring testScore = calculateGraphScore( testCoordinates, bounds, optDistance, nodesizes );
      if ( testScore.score < bestScore.score ) {
        bestScore = testScore;
        bestCoordinates = testCoordinates;

        for ( BestScoreListener listener : bestScoreListeners ) {
          listener.newBestScoreFound( bestScore, this );
        }
      }

      iterations++;
    }

    // Modify the graph model after iterating
    //
    for ( int n = 0; n < nrNodes; n++ ) {
      Point point = bestCoordinates.get( n );
      DataPresentation presentation = nodes.get( n ).getPresentation();
      presentation.setX( point.x );
      presentation.setY( point.y );
    }

    bestScore.iterations = iterations;

    // System.out.println( ">>>>>>>>>>> Chosen distance=" + (long) optDistance + ", nrNodes=" + nrNodes + ", Final Score: " + bestScore );

    return bestScore;
  }

  private java.util.List<Point> calculateNodeSizes(Display display) {
    java.util.List<Point> sizes = new ArrayList<>();

    Image image = null;
    GC gc = null;
    try {
      image = new Image( display, 100, 100 );
      gc = new GC( image );
      gc.setFont( GUIResource.getInstance().getFontMediumBold() );
    } catch(Throwable e) {
      gc = null;
      image = null;
    }

    for ( DataNode node : nodes ) {
      Point textExtent = null;
      if (image!=null && gc!=null) {
        textExtent = gc.textExtent( node.getNodeText() );
      } else {
        textExtent = calculateServerSize(node.getNodeText());
      }

      int width = textExtent.x + 2 * 10; // 10 : margin
      int height = textExtent.y + 2 * 10;

      sizes.add( new Point( width, height ) );
    }
    if (gc!=null) {
      gc.dispose();
    }
    if (image!=null) {
      image.dispose();
    }

    return sizes;
  }

  private Point calculateServerSize( String nodeText ) {
    int characterWidth = 10;
    int characterHeight = 10;

    int maxLength = 0;
    String[] lines = nodeText.split( Const.CR );
    for (String line : lines) {
      if (line.length()>maxLength) {
        maxLength = line.length();
      }
    }
    return new Point(maxLength*characterWidth, lines.length*characterHeight);
  }

  private Map<String, Integer> nodeCache;

  private int lookupNode( String nodeId ) {
    Integer index = nodeCache.get( nodeId );
    if ( index == null ) {
      DataNode node = findNode( nodeId );
      index = nodes.indexOf( node );
      nodeCache.put( nodeId, index );
    }
    return index;
  }

  public List<Point> getNodeCoordinates() {
    List<Point> coordinates = new ArrayList<>();
    for ( DataNode node : nodes ) {
      coordinates.add( node.getLocation() );
    }
    return coordinates;
  }

  public Scoring calculateGraphScore( Display display, Rectangle bounds ) {
    double optDistance = LoggingCore.calculateOptDistance( bounds, nodes.size() );
    List<Point> coordinates = getNodeCoordinates();
    List<Point> nodeSizes = calculateNodeSizes(display);
    return calculateGraphScore( coordinates, bounds, optDistance, nodeSizes );
  }

  private Scoring calculateGraphScore( List<Point> coordinates, Rectangle bounds, double optDistance, List<Point> nodeSizes ) {
    Scoring scoring = new Scoring();

    Point center = new Point( bounds.width / 2, bounds.height / 2 );
    double idealRadius = LoggingCore.calculateRadius( bounds );

    for ( int n = 0; n < nodes.size(); n++ ) {
      DataNode node = nodes.get( n );
      Point nodePoint = coordinates.get( n );

      // Calculate distance score to other nodes in radius.
      //
      java.util.List<DataNode> otherNodes = findNodesInCircle( nodePoint.x, nodePoint.y, node, coordinates, optDistance * 1.5 );
      for ( int o = 0; o < otherNodes.size(); o++ ) {
        DataNode otherNode = otherNodes.get( o );
        Point otherNodePoint = coordinates.get( o );

        double distanceToOtherNode = calculateDistance( nodePoint, otherNodePoint );

        // We score difference with optimal distance
        //
        scoring.distanceToOthers += Math.abs( distanceToOtherNode - optDistance );
      }

      // All the nodes not in the circle are too far away
      //
      int nrOutsideCircle = nodes.size() - otherNodes.size() - 1;
      if ( nrOutsideCircle > 0 ) {
        scoring.distanceToOthers += nrOutsideCircle * 100;
      }

      // Add penalty for being far from the center!
      //
      double distanceToCenter = calculateDistance( center, nodePoint );

      scoring.distanceToCenter += 5 * Math.abs( idealRadius - distanceToCenter );
    }

    // Penalties for vertices misbehaving
    //
    for ( DataRelationship vertex : relationships ) {
      int fromIndex = lookupNode( vertex.getNodeSource() );
      Point vFrom = coordinates.get( fromIndex );
      int toIndex = lookupNode( vertex.getNodeTarget() );
      Point vTo = coordinates.get( toIndex );

      // Penalty for longer vertices is even higher
      //
      double vertexLength = calculateDistance( vFrom, vTo );
      scoring.vertexLength += 2 * Math.abs( vertexLength - optDistance );

      // Penalties for crossing vertices
      //
      for ( DataRelationship otherVertex : relationships ) {
        if ( !vertex.equals( otherVertex ) ) {
          int fromOtherIndex = lookupNode( otherVertex.getNodeSource() );
          Point oFrom = coordinates.get( fromOtherIndex );
          int toOtherIndex = lookupNode( otherVertex.getNodeTarget() );
          Point oTo = coordinates.get( toOtherIndex );

          if ( !( oFrom.equals( vFrom ) || oFrom.equals( vTo ) || oTo.equals( vFrom ) || oTo.equals( vTo ) ) ) {

            Line2D one = new Line2D.Double( vFrom.x, vFrom.y, vTo.x, vTo.y );
            Line2D two = new Line2D.Double( oFrom.x, oFrom.y, oTo.x, oTo.y );

            if ( one.intersectsLine( two ) ) {
              scoring.crossedVertices += 5000;
            }
          }
        }
      }
    }

    // Build label rectangles
    //
    java.util.List<Rectangle> labels = new ArrayList<>();
    for ( int s = 0; s < nodeSizes.size(); s++ ) {
      Point nodePoint = coordinates.get( s );
      Point size = nodeSizes.get( s );
      labels.add( new Rectangle( nodePoint.x, nodePoint.y, size.x, size.y ) );
    }

    // Pentalties for overlapping labels
    //
    for ( int a = 0; a < labels.size(); a++ ) {
      Rectangle labelA = labels.get( a );
      // Intersection with other label?
      //

      for ( int b = 0; b < labels.size(); b++ ) {
        if ( a != b ) {
          Rectangle labelB = labels.get( b );
          if ( labelA.intersects( labelB ) ) {
            scoring.overlappingLabels += 10000;
          }
        }
      }

      // Label outside of bounds?
      //
      if ( labelA.x + labelA.width > bounds.width ) {
        scoring.overlappingLabels += 20000;
      }
      if ( labelA.y + labelA.height > bounds.height ) {
        scoring.overlappingLabels += 20000;
      }


      // Intersection with any vertices?
      //
      for ( DataRelationship vertex : relationships ) {
        int fromIndex = lookupNode( vertex.getNodeSource() );
        Point vFrom = coordinates.get( fromIndex );
        int toIndex = lookupNode( vertex.getNodeTarget() );
        Point vTo = coordinates.get( toIndex );

        if ( labelA.intersects( vFrom.x, vFrom.y, vTo.x, vTo.y ) ) {
          scoring.vertexThroughLabels += 100000;
        }
      }

    }

    scoring.calculateTotal();

    scoring.optDistance = idealRadius;
    scoring.radius = idealRadius;

    return scoring;

  }

  private double calculateDistance( Point a, Point b ) {
    return calculateDistance( a.x, a.y, b.x, b.y );
  }

  private Point getNodePoint( DataNode node ) {
    return new Point( node.getPresentation().getX(), node.getPresentation().getY() );
  }

  private Random random = new Random();

  private java.util.List<Point> generateRandomPoints( Rectangle bounds, int size ) {
    java.util.List<Point> points = new ArrayList<>();
    for ( int i = 0; i < size; i++ ) {

      Point point = generateRandomPoint( bounds );
      points.add( point );
    }
    return points;
  }

  private Point generateRandomPoint( Rectangle bounds ) {
    int x = (int) ( ( random.nextDouble() * bounds.width * 0.9 ) + ( 0.05 * bounds.width ) );
    int y = (int) ( ( random.nextDouble() * bounds.height * 0.9 ) + ( 0.05 * bounds.height ) );
    return new Point( x, y );
  }


  private java.util.List<Point> modifyRandomPoints( java.util.List<Point> original, Rectangle bounds, int count ) {
    int size = original.size();

    java.util.List<Point> points = new ArrayList<>();
    for ( int i = 0; i < size; i++ ) {
      Point point = original.get( i );
      points.add( new Point( point.x, point.y ) );
    }

    // Modify a few random points in the list
    //
    for ( int i = 0; i < count; i++ ) {
      int index = (int) ( random.nextDouble() * size );
      Point point = generateRandomPoint( bounds );
      points.set( index, point );
    }
    return points;
  }

  /**
   * So we're doing one scan over all the nodes.
   * We're calculating all the nodes in a certain radius.
   * <p>
   * So looking in [x-radius, y-radius] to [x+radius, y+radius]
   *
   * @param centerNode The node at the center of circle
   * @param radius     The radius of the circle
   * @return The list of nodes
   */
  protected java.util.List<DataNode> findNodesInCircle( int xc, int yc, DataNode centerNode, List<Point> coordinates, double radius ) {

    java.util.List<DataNode> circleNodes = new ArrayList<>();

    for ( int n = 0; n < nodes.size(); n++ ) {
      DataNode node = nodes.get( n );

      // Don't add same node
      //
      if ( !node.equals( centerNode ) ) {
        Point p = coordinates.get( n );
        int xn = p.x;
        int yn = p.y;

        double distance = calculateDistance( xc, yc, xn, yn );

        // Check if the node is within the circle
        //
        if ( distance < radius ) {
          circleNodes.add( node );
        }
      }
    }

    // System.out.println("Nodes in circle of radius "+(long)radius+" around ("+xc+","+yc+") : "+circleNodes.size());

    return circleNodes;
  }

  private double calculateDistance( int xc, int yc, int xn, int yn ) {
    return Math.sqrt( ( xn - xc ) * ( xn - xc ) + ( yn - yc ) * ( yn - yc ) );
  }


  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets nodes
   *
   * @return value of nodes
   */
  public List<DataNode> getNodes() {
    return nodes;
  }

  /**
   * @param nodes The nodes to set
   */
  public void setNodes( List<DataNode> nodes ) {
    this.nodes = nodes;
  }

  /**
   * Gets relationships
   *
   * @return value of relationships
   */
  public List<DataRelationship> getRelationships() {
    return relationships;
  }

  /**
   * @param relationships The relationships to set
   */
  public void setRelationships( List<DataRelationship> relationships ) {
    this.relationships = relationships;
  }
}

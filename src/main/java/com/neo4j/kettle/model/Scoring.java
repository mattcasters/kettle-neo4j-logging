package com.neo4j.kettle.model;

public class Scoring {
  public double score;
  public double distanceToOthers;
  public double distanceToCenter;
  public double vertexLength;
  public double crossedVertices;
  public double overlappingLabels;
  public double vertexThroughLabels;

  public long iterations;
  public double radius;
  public double optDistance;

  public Scoring() {
    score = 0.0;
    distanceToOthers = 0.0;
    distanceToCenter = 0.0;
    vertexLength = 0.0;
    crossedVertices = 0.0;
    overlappingLabels = 0.0;
    vertexThroughLabels = 0.0;

    iterations = 0L;
    radius = 0.0;
    optDistance = 0.0;
  }

  public void calculateTotal() {
    score = distanceToOthers + distanceToCenter + vertexLength + crossedVertices + overlappingLabels + vertexThroughLabels;
  }

  @Override public String toString() {
    return "Score: " + (long) score
      + " [distances=" + (long) distanceToOthers
      + ", center=" + (long) distanceToCenter
      + ", vLengths=" + (long) vertexLength
      + ", X=" + (long) crossedVertices
      + ", overlaps=" + (long)overlappingLabels
      + ", labelX=" + (long)vertexThroughLabels
      + ", #=" + iterations
      + ", radius=" + (long)radius
      + ", optDist=" + (long)optDistance
      + "]";
  }
}
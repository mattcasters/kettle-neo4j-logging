package com.neo4j.kettle.model;

import org.eclipse.swt.graphics.Color;
import org.pentaho.di.ui.core.gui.GUIResource;

public class DataPresentation {

  private int x;

  private int y;

  private Color color;

  private Color borderColor;

  public DataPresentation() {
    this.color = GUIResource.getInstance().getColorWhite();
  }

  public DataPresentation( int x, int y ) {
    this();
    this.x = x;
    this.y = y;
  }

  public DataPresentation( int x, int y, Color color ) {
    this();
    this.x = x;
    this.y = y;
    this.color = color;
  }

  public DataPresentation clone() {
    return new DataPresentation( x, y, color );
  }

  public void setLocation(int x, int y) {
    this.x = x;
    this.y = y;
  }

  /**
   * Gets x
   *
   * @return value of x
   */
  public int getX() {
    return x;
  }

  /**
   * @param x The x to set
   */
  public void setX( int x ) {
    this.x = x;
  }

  /**
   * Gets y
   *
   * @return value of y
   */
  public int getY() {
    return y;
  }

  /**
   * @param y The y to set
   */
  public void setY( int y ) {
    this.y = y;
  }

  /**
   * Gets color
   *
   * @return value of color
   */
  public Color getColor() {
    return color;
  }

  /**
   * @param color The color to set
   */
  public void setColor( Color color ) {
    this.color = color;
  }

  /**
   * Gets borderColor
   *
   * @return value of borderColor
   */
  public Color getBorderColor() {
    return borderColor;
  }

  /**
   * @param borderColor The borderColor to set
   */
  public void setBorderColor( Color borderColor ) {
    this.borderColor = borderColor;
  }
}

package com.neo4j.kettle.spoon.history;

import com.neo4j.kettle.logging.util.LoggingCore;
import com.neo4j.kettle.logging.util.LoggingSession;
import com.neo4j.kettle.model.AreaOwner;
import com.neo4j.kettle.model.AreaType;
import com.neo4j.kettle.model.DataModel;
import com.neo4j.kettle.model.DataNode;
import com.neo4j.kettle.model.DataPresentation;
import com.neo4j.kettle.model.DataProperty;
import com.neo4j.kettle.model.DataRelationship;
import com.neo4j.kettle.model.Scoring;
import com.neo4j.kettle.shared.NeoConnection;
import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.TreeMemory;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.job.JobGraph;
import org.pentaho.di.ui.spoon.trans.TransGraph;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HistoryResultsDialog {
  private static Class<?> PKG = HistoryResultsDialog.class; // for i18n purposes, needed by Translator2!!

  private HistoryResults input;

  private Shell parent;
  private Shell shell;

  private boolean errorPath;

  private Tree wTree;
  private Canvas wCanvas;
  private Text wLogging;
  private Button wOpen;

  private PropsUI props;

  private int middle;
  private int margin;
  private HistoryResult linkedExecution;
  private String treeName;

  private NeoConnection connection;
  private Session session;
  private Color colorLightBlue;
  private Color colorLightRed;
  private Color colorRed;

  public HistoryResultsDialog( Shell parent, HistoryResults debugLevel ) {
    this(parent, debugLevel, false);
  }

  public HistoryResultsDialog( Shell parent, HistoryResults debugLevel, boolean errorPath ) {
    this.parent = parent;
    this.input = debugLevel;
    this.errorPath = errorPath;
    props = PropsUI.getInstance();
    linkedExecution = null;
    VariableSpace space = new Variables();
    space.initializeVariablesFrom( null );
    try {
      connection = LoggingCore.getConnection( Spoon.getInstance().getMetaStore(), space );
      session = LoggingSession.getInstance().getSession( connection );
    } catch(Exception e) {
      Spoon.getInstance().getLog().logError("Error getting session: ", e);
      connection = null;
      session = null;
    }
    mouseDownPoint = new Point( -1, -1 );
    colorLightBlue = new Color(parent.getDisplay(), 240,248,255); // aliceblue
    colorLightRed = new Color( parent.getDisplay(), 250,128,114 ); //   Salmon
    colorRed = new Color(parent.getDisplay(), 255,0,0); // Red
  }

  public void open() {
    Display display = parent.getDisplay();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageSlave() );

    middle = props.getMiddlePct();
    margin = Const.MARGIN + 2;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( "Execution history" );
    shell.setLayout( formLayout );
    
    SashForm leftRight = new SashForm(shell, SWT.HORIZONTAL);

    // LEFT RIGHT

    Composite left = new Composite( leftRight, SWT.NONE );
    left.setLayout( new FormLayout() );

    // LEFT

    // Full left side of LeftRight sash form
    //
    FormData fdLeft = new FormData(  );
    fdLeft.left = new FormAttachment( 0, 0 );
    fdLeft.right = new FormAttachment( 100, 0 );
    fdLeft.top = new FormAttachment( 0, 0 );
    fdLeft.bottom = new FormAttachment( 100, 0 );
    left.setLayoutData( fdLeft );


    // Split the left in top and bottom
    //

    SashForm topBottom = new SashForm( left, SWT.VERTICAL );

    // Sashform : full Left side

    FormData fdSashForm = new FormData();
    fdSashForm.left = new FormAttachment( 0, 0 );
    fdSashForm.right = new FormAttachment( 100, 0 );
    fdSashForm.top = new FormAttachment( 0, 0 );
    fdSashForm.bottom = new FormAttachment( 100, 0 );
    topBottom.setLayoutData( fdSashForm );

    Composite top = new Composite( topBottom, SWT.NONE );
    top.setLayout( new FormLayout() );

    // The topic
    //
    Label wlTopic = new Label( top, SWT.LEFT );
    // props.setLook( wlTopic );
    wlTopic.setText( "Topic: " + input.getTopic() );
    FormData fdlTopic = new FormData();
    fdlTopic.top = new FormAttachment( 0, margin );
    fdlTopic.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlTopic.right = new FormAttachment( 100, 0 );
    wlTopic.setLayoutData( fdlTopic );
    Control lastControl = wlTopic;
    
    // The subject
    //
    Label wlSubject = new Label( top, SWT.LEFT );
    // props.setLook( wlSubject );
    wlSubject.setText( "Subject: " + input.getSubjectName() + " (" + input.getSubjectType() + ")" );
    FormData fdlSubject = new FormData();
    fdlSubject.top = new FormAttachment( 0, margin );
    fdlSubject.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlSubject.right = new FormAttachment( 100, 0 );
    wlSubject.setLayoutData( fdlSubject );
    lastControl = wlSubject;

    // The parent
    //
    if (StringUtils.isNotEmpty(input.getParentName())) {
      Label wlParent = new Label( top, SWT.LEFT );
      // props.setLook( wlParent );
      wlParent.setText( "Parent: " + input.getParentName() + " (" + input.getParentType() + ")" );
      FormData fdlParent = new FormData();
      fdlParent.top = new FormAttachment( lastControl, margin );
      fdlParent.left = new FormAttachment( 0, 0 ); // First one in the left top corner
      fdlParent.right = new FormAttachment( middle, -margin );
      wlParent.setLayoutData( fdlParent );
      lastControl = wlParent;
    }

    wTree = new Tree( top, SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL );
    props.setLook( wTree );
    wTree.setHeaderVisible( true );
    treeName = "Execution History of " + input.getSubjectName() + "(" + input.getSubjectType() + ") / " + input.getParentName();
    TreeMemory.addTreeListener( wTree, treeName );

    {
      TreeColumn column = new TreeColumn( wTree, SWT.LEFT );
      column.setText( "#" );
      column.setWidth( 150 );
    }

    {
      TreeColumn column = new TreeColumn( wTree, SWT.LEFT );
      column.setText( "Name" );
      column.setWidth( 400 );
    }

    {
      TreeColumn column = new TreeColumn( wTree, SWT.LEFT );
      column.setText( "Type" );
      column.setWidth( 150 );
    }

    {
      TreeColumn column = new TreeColumn( wTree, SWT.LEFT );
      column.setText( "R/W/I/O/R" );
      column.setWidth( 300 );
    }

    {
      TreeColumn column = new TreeColumn( wTree, SWT.LEFT );
      column.setText( "errors" );
      column.setWidth( 100 );
    }

    {
      TreeColumn column = new TreeColumn( wTree, SWT.LEFT );
      column.setText( "date" );
      column.setWidth( 350 );
    }

    {
      TreeColumn column = new TreeColumn( wTree, SWT.LEFT );
      column.setText( "duration" );
      column.setWidth( 150 );
    }


    FormData fdTree = new FormData();
    fdTree.left = new FormAttachment( 0, 0 );
    fdTree.right = new FormAttachment( 100, 0 );
    fdTree.top = new FormAttachment( lastControl, margin * 2 );
    fdTree.bottom = new FormAttachment( 100, 0 );
    wTree.setLayoutData( fdTree );

    // BOTTOM

    Composite bottom = new Composite( topBottom, SWT.NONE );
    bottom.setLayout( new FormLayout() );

    wLogging = new Text( bottom, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL );
    wLogging.setEditable( false );
    wLogging.setFont( GUIResource.getInstance().getFontFixed() );
    props.setLook( wLogging );

    // Full LEFT / BOTTOM
    //
    FormData fdLogging = new FormData();
    fdLogging.left = new FormAttachment( 0, 0 );
    fdLogging.right = new FormAttachment( 100, 0 );
    fdLogging.top = new FormAttachment( 0, 0 );
    fdLogging.bottom = new FormAttachment( 100, 0 );
    wLogging.setLayoutData( fdLogging );

    // Buttons
    Button wClose = new Button( shell, SWT.PUSH );
    wClose.setText( BaseMessages.getString( PKG, "System.Button.Close" ) );
    wClose.addListener( SWT.Selection, e -> close() );

    // Buttons
    wOpen = new Button( shell, SWT.PUSH );
    wOpen.setText( "Go to transformation" );
    wOpen.addListener( SWT.Selection, this::openItem );
    wOpen.setEnabled( false );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wClose, wOpen }, margin, null );

    // full shell above buttons
    //
    FormData fdLeftRight = new FormData(  );
    fdLeftRight.left = new FormAttachment( 0, 0 );
    fdLeftRight.right = new FormAttachment( 100, 0 );
    fdLeftRight.top = new FormAttachment( 0, 0 );
    fdLeftRight.bottom = new FormAttachment( wClose, -margin*2 );
    leftRight.setLayoutData( fdLeftRight );

    topBottom.setWeights( new int[] { 60, 40 } );
    
    Composite right = new Composite( leftRight, SWT.NONE );
    right.setLayout( new FormLayout() );
    
    wCanvas = new Canvas(right, SWT.NONE);
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment( 0, 0 );
    fdCanvas.right = new FormAttachment( 100, 0 );
    fdCanvas.top = new FormAttachment( 0, 0 );
    fdCanvas.bottom = new FormAttachment( 100, 0 );
    wCanvas.setLayoutData( fdCanvas );
    
    wCanvas.addListener( SWT.Paint, this::paintControl );
    wCanvas.addListener( SWT.MouseDown, this::graphMouseDown );
    wCanvas.addListener( SWT.MouseUp, this::graphMouseUp );
    wCanvas.addListener( SWT.MouseMove, this::moveGraphObject );
    wCanvas.addListener( SWT.MouseDoubleClick, this::doubleClickGraphObject );

    leftRight.setWeights( new int[] {75, 25} );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        close();
      }
    } );
    wTree.addListener( SWT.Selection, this::handleItemSelection );
    wTree.addListener( SWT.DefaultSelection, this::openItem );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
  }

  private void doubleClickGraphObject( Event e ) {
    mouseDownPoint.x = -1;
    mouseDownPoint.y = -1;
    if (currentDataModel==null) {
      return;
    }

    AreaOwner areaOwner = AreaOwner.findArea( areaOwners, e.x, e.y );
    if ( areaOwner == null ) {
      return;
    }

    switch ( areaOwner.getAreaType() ) {
      case NODE:

        System.out.println( "CLICKED ON NODE" );

        // Which node?
        //
        DataNode dataNode = (DataNode) areaOwner.getSubject();
        if (dataNode!=null) {
          String nodeId = dataNode.getId();

          String id = (String)dataNode.findProperty( "id" ).getValue();
          String type = (String)dataNode.findProperty( "type" ).getValue();
          String name = (String)dataNode.findProperty( "name" ).getValue();

          linkedExecution = new HistoryResult();
          linkedExecution.setId(id);
          linkedExecution.setName( name );
          linkedExecution.setType( type );

          if ("TRANS".equals( type ) || "STEP".equals( type ) || "JOB".equals( type )) {
            System.out.println( "CLICKED ON NODE id : "+id+", type : "+type+", name : "+name );
            openItem( null );
          }
        }
        break;
      default:
        break;
    }
  }

  private java.util.List<AreaOwner> areaOwners;
  private DataModel currentDataModel = null;
  private Point mouseDownPoint;

  private void paintControl( Event event ) {

    GC gc = event.gc;
    Rectangle bounds = wCanvas.getBounds();
    areaOwners = new ArrayList<>();

    // Set background
    //
    gc.fillRectangle( bounds );

    gc.setForeground( GUIResource.getInstance().getColorBlack() );
    gc.setBackground( GUIResource.getInstance().getColorWhite() );
    gc.fillRectangle( 0, 0, event.width, event.height );

    int margin = 10;

    if (currentDataModel==null) {
      return;
    }

    // Draw the relationships
    //
    int lineWidth = gc.getLineWidth();
    gc.setLineWidth( 2 );
    for ( DataRelationship relationship : currentDataModel.getRelationships() ) {
      DataNode sourceNode = currentDataModel.findNode( relationship.getNodeSource() );
      DataNode targetNode = currentDataModel.findNode( relationship.getNodeTarget() );
      if ( sourceNode != null && targetNode != null ) {
        gc.setFont( GUIResource.getInstance().getFontMedium() );
        Point sourceExtent = gc.textExtent( sourceNode.getNodeText() );
        Point targetExtent = gc.textExtent( targetNode.getNodeText() );

        int fromX = sourceNode.getPresentation().getX() + margin + sourceExtent.x / 2;
        int fromY = sourceNode.getPresentation().getY() + margin + sourceExtent.y / 2;

        int toX = targetNode.getPresentation().getX() + margin + targetExtent.x / 2;
        int toY = targetNode.getPresentation().getY() + margin + targetExtent.y / 2;

        gc.setForeground( GUIResource.getInstance().getColorRed() );
        gc.drawLine( fromX, fromY, toX, toY );

        gc.setFont( GUIResource.getInstance().getFontMedium() );
        Point relExtent = gc.textExtent( Const.NVL(relationship.getLabel(), "") );

        int middleX = fromX + ( toX - fromX ) / 2 - relExtent.x / 2;
        int middleY = fromY + ( toY - fromY ) / 2 - relExtent.y / 2;

        gc.setForeground( GUIResource.getInstance().getColorBlack() );
        gc.drawText( Const.NVL(relationship.getLabel(), ""), middleX, middleY );
        areaOwners.add( new AreaOwner( middleX, middleY, relExtent.x, relExtent.y, AreaType.RELATIONSHIP_LABEL, relationship ) );
      }
    }
    gc.setLineWidth( lineWidth );

    // Draw all the nodes
    //
    gc.setFont( GUIResource.getInstance().getFontMediumBold() );
    for ( DataNode dataNode : currentDataModel.getNodes() ) {
      DataPresentation presentation = dataNode.getPresentation();

      String nodeText = dataNode.getNodeText();

      Point textExtent = gc.textExtent( nodeText );
      int x = presentation.getX();
      int y = presentation.getY();
      int width = textExtent.x + 2 * margin;
      int height = textExtent.y + 2 * margin;

      gc.setForeground( GUIResource.getInstance().getColorBackground() );
      gc.setBackground( presentation.getColor() );

      gc.fillRoundRectangle( x, y, width, height, margin, margin );

      if (presentation.getBorderColor()!=null) {
        gc.setForeground( presentation.getBorderColor() );
        gc.setLineWidth( 3 );
      } else {
        gc.setForeground( GUIResource.getInstance().getColorBlue() );
        gc.setLineWidth( 1 );
      }
      gc.drawRoundRectangle( x, y, width, height, height / 3, height / 3 );
      gc.setLineWidth( 1 );

      areaOwners.add( new AreaOwner( x, y, width, height, AreaType.NODE, dataNode ) );

      gc.setForeground( GUIResource.getInstance().getColorBlack() );
      gc.drawText( nodeText, x + margin, y + margin );
    }

    /*
      Scoring scoring = currentDataModel.calculateGraphScore( bounds );
      gc.setBackground( GUIResource.getInstance().getColorWhite() );
      gc.setForeground( GUIResource.getInstance().getColorDarkGray() );
      gc.drawText(scoring.toString(), 20, 20);
    */
  }

  private void graphMouseUp( Event e ) {
    mouseDownPoint.x = -1;
    mouseDownPoint.y = -1;
    // System.out.println("Up: ("+e.x+", "+e.y+")");
  }

  private void graphMouseDown( Event e ) {
    mouseDownPoint.x = e.x;
    mouseDownPoint.y = e.y;
    // System.out.println("Down: ("+e.x+", "+e.y+")");
  }

  private void moveGraphObject( Event e ) {
    if (currentDataModel==null) {
      return;
    }

    if ( mouseDownPoint.x > 0 && mouseDownPoint.y > 0 ) {
      // System.out.println("Move: ("+e.x+", "+e.y+")");
      // Mouse drag
      //
      AreaOwner areaOwner = AreaOwner.findArea( areaOwners, mouseDownPoint.x, mouseDownPoint.y );
      if ( areaOwner != null ) {
        int offsetX = mouseDownPoint.x - areaOwner.getX();
        int offsetY = mouseDownPoint.y - areaOwner.getY();
        // System.out.println("Offset: (+"+offsetX+", "+offsetY+")");

        switch ( areaOwner.getAreaType() ) {
          case NODE:
            DataNode graphNode = (DataNode) areaOwner.getSubject();
            DataPresentation p = graphNode.getPresentation();
            p.setX( e.x - offsetX );
            p.setY( e.y - offsetY );
            mouseDownPoint.x = e.x;
            mouseDownPoint.y = e.y;
            wCanvas.redraw();
            break;
          default:
            break;
        }
      } else {
        // Move all the objects around
        //
        int offsetX = mouseDownPoint.x-e.x;
        int offsetY = mouseDownPoint.y-e.y;

        for (DataNode graphNode : currentDataModel.getNodes()) {
          DataPresentation p = graphNode.getPresentation();
          p.setX(p.getX()-offsetX);
          p.setY(p.getY()-offsetY);
        }
        mouseDownPoint.x = e.x;
        mouseDownPoint.y = e.y;

        wCanvas.redraw();
      }
    }
  }

  private void openItem( Event event ) {
    if (linkedExecution==null) {
      return;
    }

    String nodeLabel = null;
    String relationship = null;

    if ("TRANS".equals(linkedExecution.getType())) {
      openTransformationOrJob("Transformation", "EXECUTION_OF_TRANSFORMATION");
    } else if ("JOB".equals( linkedExecution.getType() )){
      openTransformationOrJob("Job", "EXECUTION_OF_JOB");
    } else if ("STEP".equals( linkedExecution.getType() )){
      openStep();
    } else if ("JOBENTRY".equals( linkedExecution.getType() )){
      openJobEntry();
    }


  }

  private void openStep() {

    System.out.println("Open step : "+linkedExecution.getId()+", name : "+linkedExecution.getName()+", type: "+linkedExecution.getType());

    Map<String, Object> params = new HashMap<>();
    params.put( "subjectName", linkedExecution.getName() );
    params.put( "subjectType", linkedExecution.getType() );
    params.put( "subjectId",   linkedExecution.getId() );

    StringBuilder cypher = new StringBuilder();
    cypher.append( "MATCH(step:Execution { name : {subjectName}, type : {subjectType}, id : {subjectId}} )" ); // STEP
    cypher.append( "-[:EXECUTION_OF_STEP]->(stepMeta:Step { name : {subjectName}} )" ); // Step
    cypher.append( "-[:STEP_OF_TRANSFORMATION]->(transMeta:Transformation) " );
    cypher.append( "RETURN transMeta.filename, stepMeta.name " );

    System.out.println("Open step cypher : "+cypher.toString());

    StatementResult statementResult = session.run( cypher.toString(), params );
    if (!statementResult.hasNext()) {
      statementResult.consume();
      return; // No file found
    }
    Record record = statementResult.next();
    statementResult.consume();

    String filename = LoggingCore.getStringValue( record, 0 );
    String stepname = LoggingCore.getStringValue( record, 1 );

    System.out.println("Open filename : "+filename);
    System.out.println("Open stepname : "+stepname);

    Spoon spoon = Spoon.getInstance();
    if ( StringUtils.isNotEmpty(filename)) {
      close();
      spoon.openFile( filename, false );
      if (StringUtils.isNotEmpty( stepname )) {
        TransGraph transGraph = Spoon.getInstance().getActiveTransGraph();
        if (transGraph!=null) {
          System.out.println("Open step : "+stepname);
          TransMeta transMeta = transGraph.getTransMeta();
          StepMeta stepMeta = transMeta.findStep( stepname );
          if (stepMeta!=null) {
            transMeta.unselectAll();
            stepMeta.setSelected( true );
            spoon.editStep(transMeta, stepMeta);
          } else {
            System.out.println("step not found!");
          }
        }
      }
    }
  }

  private void openJobEntry() {

    System.out.println("Open job entry : "+linkedExecution.getId()+", name : "+linkedExecution.getName()+", type: "+linkedExecution.getType());

    Map<String, Object> params = new HashMap<>();
    params.put( "subjectName", linkedExecution.getName() );
    params.put( "subjectType", linkedExecution.getType() );
    params.put( "subjectId",   linkedExecution.getId() );

    StringBuilder cypher = new StringBuilder();
    cypher.append( "MATCH(jobEntry:Execution { name : {subjectName}, type : {subjectType}, id : {subjectId}} )" ); // JOBENTRY
    cypher.append( "-[:EXECUTION_OF_JOBENTRY]->(jobEntryMeta:JobEntry { name : {subjectName}} )" ); // JobEntry
    cypher.append( "-[:JOBENTRY_OF_JOB]->(jobMeta:Job) " ); // JobMeta
    cypher.append( "RETURN jobMeta.filename, jobEntryMeta.name " );

    System.out.println("Open job entry cypher : "+cypher.toString());

    StatementResult statementResult = session.run( cypher.toString(), params );
    if (!statementResult.hasNext()) {
      statementResult.consume();
      return; // No file found
    }
    Record record = statementResult.next();
    statementResult.consume();

    String filename = LoggingCore.getStringValue( record, 0 );
    String entryname = LoggingCore.getStringValue( record, 1 );

    System.out.println("Open filename : "+filename);
    System.out.println("Open stepname : "+entryname);

    Spoon spoon = Spoon.getInstance();
    if ( StringUtils.isNotEmpty(filename)) {
      close();
      spoon.openFile( filename, false );
      if (StringUtils.isNotEmpty( entryname )) {
        JobGraph jobGraph = Spoon.getInstance().getActiveJobGraph();
        if (jobGraph!=null) {
          System.out.println("Open job entry : "+entryname);
          JobMeta jobMeta = jobGraph.getJobMeta();
          JobEntryCopy jobEntryCopy = jobMeta.findJobEntry( entryname );
          if (jobEntryCopy!=null) {
            jobMeta.unselectAll();
            jobEntryCopy.setSelected( true );
            spoon.editJobEntry(jobMeta, jobEntryCopy);
          } else {
            System.out.println("job entry not found!");
          }
        }
      }
    }
  }

  private void openTransformationOrJob( String nodeLabel, String relationship ) {
    Map<String, Object> params = new HashMap<>();
    params.put( "subjectName", linkedExecution.getName() );
    params.put( "subjectType", linkedExecution.getType() );
    params.put( "subjectId",   linkedExecution.getId() );

    StringBuilder cypher = new StringBuilder();
    cypher.append( "MATCH(ex:Execution { name : {subjectName}, type : {subjectType}, id : {subjectId}}) " );
    cypher.append( "MATCH(tr:"+nodeLabel+" { name : {subjectName}}) " );
    cypher.append( "MATCH(ex)-[:"+relationship+"]->(tr) " );
    cypher.append( "RETURN tr.filename " );

    StatementResult statementResult = session.run( cypher.toString(), params );
    if (!statementResult.hasNext()) {
      statementResult.consume();
      return; // No file found
    }
    Record record = statementResult.next();
    String filename = LoggingCore.getStringValue( record, 0 );
    statementResult.consume();

    if ( StringUtils.isNotEmpty(filename)) {
      Spoon.getInstance().openFile( filename, false );
      close();
    }
  }

  private class TreeIndexes {
    int rootIndex;
    int pathIndex;
    int pathNodeIndex;

    public TreeIndexes( int rootIndex, int pathIndex, int pathNodeIndex ) {
      this.rootIndex = rootIndex;
      this.pathIndex = pathIndex;
      this.pathNodeIndex = pathNodeIndex;
    }
  }

  private void handleItemSelection( Event e ) {
    // Someone selected something in the tree.
    //
    // The selected tree item:
    //
    wOpen.setEnabled( false );
    TreeItem item = (TreeItem) e.item;

    handleItemSelection( item );
  }

  private void handleItemSelection(TreeItem item) {

    String[] path = ConstUI.getTreeStrings( item );

    TreeIndexes indexes = getTreeIndexes(item, path);

    if (indexes.rootIndex<0){
      return;
    }

    String cypher = null;
    HistoryResult historyResult = null;

    if ( path.length == 1 ) {
      // The root item
      //
      historyResult = input.getLastExecutions().get( indexes.rootIndex );
      cypher = showLogging( historyResult, false, 0, errorPath);

    }
    if ( path.length == 2 || path.length == 3 ) {
      // Clicked on a path root, show root = the last in the line
      //
      if (indexes.pathIndex<0) {
        indexes.pathIndex=0;
      }
      historyResult = input.getLastExecutions().get( indexes.rootIndex );
      if (historyResult.getShortestPaths().isEmpty()) {
        return;
      }
      List<HistoryResult> execPath = historyResult.getShortestPaths().get( indexes.pathIndex );
      if ( execPath.size() > 0 ) {
        HistoryResult execution = execPath.get( execPath.size() - 1 );
        cypher = showLogging( execution, true, indexes.pathIndex, errorPath);
        enableOpenButton(execution);
      }
    }
    if ( path.length == 4 ) {
      if (indexes.pathNodeIndex<0) {
        return;
      }
      historyResult = input.getLastExecutions().get( indexes.rootIndex );
      List<HistoryResult> execPath = historyResult.getShortestPaths().get( indexes.pathIndex );
      HistoryResult execution = execPath.get( indexes.pathNodeIndex );
      cypher = showLogging( execution, indexes.pathNodeIndex==execPath.size()-1, indexes.pathIndex, errorPath);
      enableOpenButton(execution);
    }

    System.out.println(">>>>>>>> handleItemSelection: session!=null ? "+(session!=null)+", cypher!=null ? "+(cypher!=null));

    if (session!=null && cypher!=null) {
      // Execute cypher and get DataModel
      //
      StatementResult result = session.run( cypher );
      currentDataModel = new DataModel( "Logging analyses", result );

      /*
      long cutOffTimeMs = 1000;

      // currentDataModel.getBestScoreListeners().add( this::updateDataModel );
      Scoring bestScore = currentDataModel.autoLayout( parent.getDisplay(), wCanvas.getBounds(), cutOffTimeMs );
      if (bestScore==null) {
        return;
      }

      Spoon.getInstance().getLog().logBasic("END LAYOUT TIME PER ITERATION (ns): "+(long)((double)(cutOffTimeMs*1000000/bestScore.iterations)) );
      */

      DataNode startNode = currentDataModel.findNodeWithProperty( "root", true);
      if (startNode==null) {
        startNode = currentDataModel.findTopNode("EXECUTES");
      }

      if (startNode!=null) {
        currentDataModel.treeLayout( parent.getDisplay(), startNode, "EXECUTES", 50, 50, 300, 200 );
      }

      // Set node colors
      //
      for (DataNode node : currentDataModel.getNodes()) {
        String label = node.getLabels().get(0);
        if ("Execution".equals( label )) {
          node.getPresentation().setColor( colorLightBlue );
        } else if ("Transformation".equals( label )) {
          node.getPresentation().setColor( colorLightRed  );
        } else if ("Step".equals( label )) {
          node.getPresentation().setColor( colorLightRed  );
        } else if ("Job".equals( label )) {
          node.getPresentation().setColor( colorLightRed );
        } else if ("JobEntry".equals( label )) {
          node.getPresentation().setColor( colorLightRed );
        }

        DataProperty errorsProperty = node.findProperty( "errors" );
        if (errorsProperty!=null) {
          if (errorsProperty.getValue()!=null && (errorsProperty.getValue() instanceof Long)) {
            Long errors = (Long) errorsProperty.getValue();
            if (errors>0) {
              node.getPresentation().setBorderColor( colorRed );
            }
          }
        }
      }

      wCanvas.redraw();
    }
  }

  private void updateDataModel( Scoring scoring, DataModel dataModel ) {
    wCanvas.update();
  }

  private TreeIndexes getTreeIndexes( TreeItem item, String[] path ) {
    TreeIndexes indexes = new TreeIndexes( -1, -1, -1 );

    // The path to the root
    //
    if ( path.length >= 1 ) {
      indexes.rootIndex = Const.toInt( path[ 0 ], 0 ) - 1;
    }
    if ( path.length >= 3 ) {
      indexes.pathIndex = Const.toInt( path[ 2 ], 0 ) - 1;
    }
    if ( path.length >= 4 ) {
      // Clicked on a path node.
      //
      indexes.pathNodeIndex = Const.toInt( path[ 3 ], 0 ) - 1;
    }

    return indexes;
  }

  private void enableOpenButton( HistoryResult execution ) {
    if ( LoggingObjectType.TRANS.name().equalsIgnoreCase(execution.getType())) {
      // A transformation
      //
      wOpen.setText( "Go to transformation" );
      wOpen.setEnabled( true );
      linkedExecution = execution;
    } else if ( LoggingObjectType.STEP.name().equalsIgnoreCase(execution.getType())) {
      // A transformation
      //
      wOpen.setText( "Go to step" );
      wOpen.setEnabled( true );
      linkedExecution = execution;
    } else if ( LoggingObjectType.JOB.name().equalsIgnoreCase(execution.getType())) {
      // A transformation
      //
      wOpen.setText( "Go to job" );
      wOpen.setEnabled( true );
      linkedExecution = execution;
    } else if ( LoggingObjectType.JOBENTRY.name().equalsIgnoreCase(execution.getType())) {
      // A transformation
      //
      wOpen.setText( "Go to job entry" );
      wOpen.setEnabled( true );
      linkedExecution = execution;
    } else {
      wOpen.setEnabled( false );
      linkedExecution = null;
    }
  }

  /**
   * Show Cypher and logging in the bottom text area
   *
   * @param hr the result
   * @param root is this a root result?
   * @param pathIndex
   * @return The most complex cypher possible
   */
  private String showLogging( HistoryResult hr, boolean root, int pathIndex, boolean errorPath) {
    String cypher = null;
    String metaCypher = null;

    StringBuilder log = new StringBuilder();
    log.append( "Name:      " + hr.getName() ).append( Const.CR );
    log.append( "Type:      " + hr.getType() ).append( Const.CR );
    log.append( "Date:      " + hr.getRegistrationDate() ).append( Const.CR );
    log.append( "ID:        " + hr.getId() ).append( Const.CR );
    log.append( "Errors:    " + toString( hr.getErrors() ) ).append( Const.CR );
    log.append( "Read:      " + toString( hr.getRead() ) ).append( Const.CR );
    log.append( "Written:   " + toString( hr.getWritten() ) ).append( Const.CR );
    log.append( "Input:     " + toString( hr.getInput() ) ).append( Const.CR );
    log.append( "Output:    " + toString( hr.getOutput() ) ).append( Const.CR );
    log.append( "Rejected:  " + toString( hr.getRejected() ) ).append( Const.CR );
    log.append( Const.CR );

    // Add Cypher to look up execution info
    //
    log.append( "Execution info cypher: " ).append( Const.CR );
    log.append("--------------------------------------------").append(Const.CR);
    cypher = hr.getExecutionInfoCommand();
    log.append( cypher );
    log.append(Const.CR);

    log.append( "Error path lookup: " ).append( Const.CR );
    log.append("--------------------------------------------").append(Const.CR);
    String errorCypher = hr.getErrorPathCommand();
    log.append( errorCypher);
    log.append(Const.CR);

    // Add Cypher to look up shortest path
    //
    if (!root && !(hr.isRoot()!=null && hr.isRoot())) {
      log.append( "Root path Cypher: " ).append( Const.CR );
      log.append("--------------------------------------------").append(Const.CR);
      cypher = hr.getShortestPathCommand();
      log.append( cypher );
      log.append( Const.CR );
    }
    // Let's calculate also the links to all the shortest metadata nodes
    //
    if (hr.getShortestPaths().size()>pathIndex) {
      log.append( "Shortest path including metadata: " ).append( Const.CR );
      log.append("--------------------------------------------").append(Const.CR);
      metaCypher = hr.getShortestPathWithMetadataCommand( pathIndex);
      cypher = metaCypher;
      log.append( cypher );
      System.out.println(">>>>> GOT SHORTEST PATH WITH METADATA!");
      log.append( Const.CR );
    }

    log.append( "Execution Log: " ).append( Const.CR );
    log.append("--------------------------------------------").append(Const.CR);
    log.append( hr.getLoggingText() ).append( Const.CR );

    wLogging.setText( log.toString() );

    if (errorPath) {
      cypher = errorCypher;

      /*if (metaCypher!=null) {
        cypher = metaCypher;
      }
      */
    }

    return cypher;
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
    colorLightBlue.dispose();
    colorLightRed.dispose();
    colorRed.dispose();
  }

  public void getData() {

    // Populate the tree...
    //
    for ( int a = 0; a < input.getLastExecutions().size(); a++ ) {
      HistoryResult execution = input.getLastExecutions().get( a );
      TreeItem parentItem = new TreeItem( wTree, SWT.NONE );
      int i = 0;
      parentItem.setText( i++, Integer.toString( a + 1 ) );
      parentItem.setText( i++, Const.NVL( execution.getName(), "" ) );
      parentItem.setText( i++, Const.NVL( execution.getType(), "" ) );
      String rwior = toString( execution.getRead() ) + "/" + toString( execution.getWritten() ) + "/" + toString( execution.getInput() ) + "/" + toString( execution.getOutput() ) + "/" + toString(
        execution.getRejected() );
      parentItem.setText( i++, rwior );
      parentItem.setText( i++, toString( execution.getErrors() ) );
      parentItem.setText( i++, Const.NVL( execution.getRegistrationDate(), "" ).replace( "T", " ") );
      parentItem.setText( i++, getDurationHMS(execution.getDurationMs()) );

      if (!(execution.isRoot()!=null && execution.isRoot()) || errorPath) {
        // Add the paths in fold out below that tree item
        //
        TreeItem pathsItem = new TreeItem( parentItem, SWT.NONE );
        pathsItem.setText( 0, "Paths" );

        List<List<HistoryResult>> shortestPaths = execution.getShortestPaths();
        for ( int p = shortestPaths.size() - 1; p >= 0; p-- ) {
          List<HistoryResult> shortestPath = shortestPaths.get( p );

          TreeItem pathItem = new TreeItem( pathsItem, SWT.NONE );
          pathItem.setText( 0, Integer.toString( p + 1 ) );

          for ( int e = 0; e < shortestPath.size(); e++ ) {
            HistoryResult exec = shortestPath.get( e );
            TreeItem execItem = new TreeItem( pathItem, SWT.NONE );
            int x = 0;
            execItem.setText( x++, Integer.toString( e + 1 ) );
            execItem.setText( x++, Const.NVL( exec.getName(), "" ) );
            execItem.setText( x++, Const.NVL( exec.getType(), "" ) );
            String erwior =
              toString( exec.getRead() ) + "/" + toString( exec.getWritten() ) + "/" + toString( exec.getInput() ) + "/" + toString( exec.getOutput() ) + "/" + toString( exec.getRejected() );
            execItem.setText( x++, erwior );
            execItem.setText( x++, toString( exec.getErrors() ) );
            execItem.setText( x++, Const.NVL( exec.getRegistrationDate(), "" ).replace( "T", " " ) );
            execItem.setText( x++, getDurationHMS(exec.getDurationMs()) );
            execItem.setExpanded( true );
          }
          if ( p == shortestPaths.size() - 1 ) {
            TreeMemory.getInstance().storeExpanded( treeName, pathItem, true );
          }
        }
        if (a==0) {
          TreeMemory.getInstance().storeExpanded( treeName, pathsItem, true );
        }
      }
      if (a==0) {
        TreeMemory.getInstance().storeExpanded( treeName, parentItem, true );
      }
    }
    TreeMemory.setExpandedFromMemory( wTree, treeName );

    if (wTree.getItemCount()>0) {
      TreeItem item = wTree.getItem( 0 );
      wTree.setSelection( item );
      handleItemSelection( item );
    }
  }

  private String toString( Long lng ) {
    if ( lng == null ) {
      return "";
    }
    return lng.toString();
  }

  private String getDurationHMS(Long durationMs) {
    if (durationMs==null) {
      return "";
    }
    double seconds = ((double)durationMs) / 1000;
    return getDurationHMS( seconds );
  }

  private String getDurationHMS(double seconds) {
    int day = (int) TimeUnit.SECONDS.toDays((long)seconds);
    long hours = TimeUnit.SECONDS.toHours((long)seconds) - (day *24);
    long minute = TimeUnit.SECONDS.toMinutes((long)seconds) - (TimeUnit.SECONDS.toHours((long)seconds)* 60);
    long second = TimeUnit.SECONDS.toSeconds((long)seconds) - (TimeUnit.SECONDS.toMinutes((long)seconds) *60);
    long ms = (long)((seconds - ((long)seconds))*1000);

    StringBuilder hms = new StringBuilder();
    if (day>0) {
      hms.append( day + "d " );
    }
    if (day>0 || hours>0) {
      hms.append(hours + "h ");
    }
    if (day>0 || hours>0 || minute>0) {
      hms.append(String.format( "%02d", minute ) + "' ");
    }
    hms.append(String.format( "%02d", second ) + ".");
    hms.append(String.format("%03d", ms)+"\"");

    return hms.toString();
  }

  public void close() {

    dispose();
  }

}

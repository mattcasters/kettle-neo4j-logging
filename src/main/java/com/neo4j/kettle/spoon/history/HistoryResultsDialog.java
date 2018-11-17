package com.neo4j.kettle.spoon.history;

import com.neo4j.kettle.logging.util.LoggingCore;
import com.neo4j.kettle.spoon.NeoLoggingHelper;
import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
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
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.TreeMemory;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HistoryResultsDialog extends Dialog {
  private static Class<?> PKG = HistoryResultsDialog.class; // for i18n purposes, needed by Translator2!!

  private HistoryResults input;

  private Shell shell;

  private Tree wTree;
  private Text wLogging;
  private Button wOpen;

  private PropsUI props;

  private int middle;
  private int margin;
  private HistoryResult linkedExecution;
  private boolean linkedTransformation;
  private String treeName;

  public HistoryResultsDialog( Shell par, HistoryResults debugLevel ) {
    super( par, SWT.NONE );
    this.input = debugLevel;
    props = PropsUI.getInstance();
    linkedExecution = null;
  }

  public void open() {
    Shell parent = getParent();
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

    SashForm sashForm = new SashForm( shell, SWT.VERTICAL );

    Composite top = new Composite( sashForm, SWT.NONE );
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

    Composite bottom = new Composite( sashForm, SWT.NONE );
    bottom.setLayout( new FormLayout() );

    wLogging = new Text( bottom, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL );
    wLogging.setEditable( false );
    wLogging.setFont( GUIResource.getInstance().getFontFixed() );
    props.setLook( wLogging );
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

    FormData fdSashForm = new FormData();
    fdSashForm.left = new FormAttachment( 0, 0 );
    fdSashForm.right = new FormAttachment( 100, 0 );
    fdSashForm.top = new FormAttachment( 0, 0 );
    fdSashForm.bottom = new FormAttachment( wClose, -margin * 2 );
    sashForm.setLayoutData( fdSashForm );

    sashForm.setWeights( new int[] { 50, 50 } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        close();
      }
    } );
    wTree.addListener( SWT.Selection, this::handleItemSelection );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    Display display = parent.getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
  }

  private void openItem( Event event ) {
    if (linkedExecution==null) {
      return;
    }

    String filename = null;
    String nodeLabel = null;
    String relationship = null;

    if (linkedTransformation) {
      nodeLabel = "Transformation";
      relationship = "EXECUTION_OF_TRANSFORMATION";
    } else {
      nodeLabel = "Job";
      relationship = "EXECUTION_OF_JOB";
    }

    Session session = null;
    try {
      session = NeoLoggingHelper.getInstance().getDriver().session();

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
        return; // No file found
      }
      Record record = statementResult.next();
      filename = LoggingCore.getStringValue( record, 0 );
    } finally {
      if ( session != null ) {
        session.close();
      }
    }

    if ( StringUtils.isNotEmpty(filename)) {
      Spoon.getInstance().openFile( filename, false );
      close();
    }

  }

  private void handleItemSelection( Event e ) {
    // Someone selected something in the tree.
    //
    // The selected tree item:
    //
    wOpen.setEnabled( false );
    TreeItem item = (TreeItem) e.item;

    int rootIndex = -1;
    int pathIndex = -1;
    int pathNodeIndex = -1;

    // The path to the root
    //
    String[] path = ConstUI.getTreeStrings( item );
    if ( path.length >= 1 ) {
      rootIndex = Const.toInt( path[ 0 ], 0 ) - 1;
    }
    if ( path.length >= 3 ) {
      pathIndex = Const.toInt( path[ 2 ], 0 ) - 1;
    }
    if ( path.length >= 4 ) {
      // Clicked on a path node.
      //
      pathNodeIndex = Const.toInt( path[ 3 ], 0 ) - 1;
    }

    if (rootIndex<0){
      return;
    }

    if ( path.length == 1 ) {
      // The root item
      //
      HistoryResult execution = input.getLastExecutions().get( rootIndex );
      showLogging( execution, false );
    }
    if ( path.length == 2 || path.length == 3 ) {
      // Clicked on a path root, show root = the last in the line
      //
      if (pathIndex<0) {
        pathIndex=0;
      }
      HistoryResult historyResult = input.getLastExecutions().get( rootIndex );
      if (historyResult.getShortestPaths().isEmpty()) {
        return;
      }
      List<HistoryResult> execPath = historyResult.getShortestPaths().get( pathIndex );
      if ( execPath.size() > 0 ) {
        HistoryResult execution = execPath.get( execPath.size() - 1 );
        showLogging( execution, true );
        enableOpenButton(execution);
      }
    }
    if ( path.length == 4 ) {
      if (pathNodeIndex<0) {
        return;
      }
      HistoryResult historyResult = input.getLastExecutions().get( rootIndex );
      List<HistoryResult> execPath = historyResult.getShortestPaths().get( pathIndex );
      HistoryResult execution = execPath.get( pathNodeIndex );
      showLogging( execution, pathNodeIndex==execPath.size()-1 );
      enableOpenButton(execution);
    }
  }

  private void enableOpenButton( HistoryResult execution ) {
    if ( LoggingObjectType.TRANS.name().equalsIgnoreCase(execution.getType())) {
      // A transformation
      //
      wOpen.setText( "Go to transformation" );
      wOpen.setEnabled( true );
      linkedExecution = execution;
      linkedTransformation = true;
    } else if ( LoggingObjectType.JOB.name().equalsIgnoreCase(execution.getType())) {
      // A transformation
      //
      wOpen.setText( "Go to job" );
      wOpen.setEnabled( true );
      linkedExecution = execution;
      linkedTransformation = false;
    }
  }

  private void showLogging( HistoryResult hr, boolean root ) {
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
    log.append( "MATCH(ex:Execution { name : \"" + hr.getName() + "\", type : \"" + hr.getType() + "\", id : \"" + hr.getId() + "\"}) " ).append(Const.CR);
    log.append( "RETURN ex " );
    log.append( Const.CR );
    log.append( Const.CR );

    // Add Cypher to look up shortest path
    //
    if (!root && !(hr.isRoot()!=null && hr.isRoot())) {
      log.append( "Root path Cypher: " ).append( Const.CR ).append(Const.CR);
      log.append( "MATCH(se:Execution { name : \"" + hr.getName() + "\", type : \"" + hr.getType() + "\", id : \"" + hr.getId() + "\"})" ).append(Const.CR);
      log.append( ", (je:Execution { root : true }) " ).append(Const.CR);
      log.append( "  , p=shortestpath((se)-[:EXECUTES*..20]-(je)) " ).append(Const.CR);
      log.append( "WHERE se.registrationDate IS NOT NULL " ).append(Const.CR);
      log.append( "RETURN p " ).append(Const.CR);
      log.append( "LIMIT 1 " ).append(Const.CR);
      log.append( Const.CR );
      log.append( Const.CR );
    }
    log.append( hr.getLoggingText() ).append( Const.CR );

    wLogging.setText( log.toString() );
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
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

      if (!(execution.isRoot()!=null && execution.isRoot())) {
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

package com.neo4j.kettle.logging.trans;

import com.neo4j.kettle.spoon.NeoLoggingHelper;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.ui.spoon.Spoon;

@ExtensionPoint(
  id = "EnvironmentChangedExtensionPoint",
  extensionPointId = "EnvironmentActivated",
  description = "Neo4j logging : resets connection and driver so the new environment can have its own settings"
)
public class EnvironmentChangedExtensionPoint implements ExtensionPointInterface {
  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {

    String environmentName = (String) object;

    // Only in Spoon...
    //
    if ( Spoon.getInstance()!=null) {
      NeoLoggingHelper helper = NeoLoggingHelper.getInstance();
      if (helper!=null) {
        helper.resetConnectionDriver();
      }
    }

  }
}

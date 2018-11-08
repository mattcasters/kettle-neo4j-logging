package com.neo4j.kettle.logging.util;

import com.neo4j.kettle.logging.Defaults;
import com.neo4j.kettle.shared.NeoConnection;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;

public class LoggingCore {

  public static final boolean isEnabled( VariableSpace space ) {
    String connectionName = space.getVariable( Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION );
    return StringUtils.isNotEmpty(connectionName);
  }

  public static final NeoConnection getConnection( IMetaStore metaStore, VariableSpace space) throws MetaStoreException {
    String connectionName = space.getVariable( Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION );
    if (StringUtils.isEmpty( connectionName )) {
      return null;
    }
    MetaStoreFactory<NeoConnection> factory = new MetaStoreFactory<NeoConnection>( NeoConnection.class, metaStore, Defaults.NAMESPACE );
    NeoConnection connection = factory.loadElement( connectionName );
    connection.initializeVariablesFrom( space );
    return connection;
  }

}

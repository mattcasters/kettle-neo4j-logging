package com.neo4j.kettle.logging.util;

import com.neo4j.kettle.shared.NeoConnection;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.pentaho.di.core.logging.LogChannel;

import java.util.HashMap;
import java.util.Map;

public class LoggingSession {

  private class DriverSession {
    public Driver driver;
    public Session session;
  }

  private Map<String, DriverSession> sessionMap;

  private static LoggingSession loggingSession;

  private LoggingSession() {
    sessionMap = new HashMap<>();
  }

  public static LoggingSession getInstance() {
    if ( loggingSession == null ) {
      loggingSession = new LoggingSession();
    }
    return loggingSession;
  }

  public Session getSession( NeoConnection connection ) {
    return obtainDriverSession( connection ).session;
  }

  public Driver getDriver( NeoConnection connection ) {
    return obtainDriverSession( connection ).driver;
  }

  public void close( NeoConnection connection ) {
    DriverSession driverSession = sessionMap.get( connection.getName() );
    if ( driverSession != null ) {
      if ( driverSession.driver != null ) {
        driverSession.driver.close();
      }
      if ( driverSession.session != null ) {
        driverSession.session.close();
      }
      sessionMap.remove( connection.getName() );
    }
  }

  public Session reconnect( NeoConnection connection ) {
    close( connection );
    return obtainDriverSession( connection ).session;
  }

  public DriverSession obtainDriverSession( NeoConnection connection ) {
    DriverSession driverSession = sessionMap.get( connection.getName() );
    if ( driverSession == null ) {
      driverSession = new DriverSession();

      driverSession.driver = connection.getDriver( LogChannel.GENERAL );
      driverSession.session = driverSession.driver.session();

      sessionMap.put( connection.getName(), driverSession );
    }
    return driverSession;
  }


}

package com.neo4j.kettle.logging.util;

import org.neo4j.driver.Result;

import java.text.ParseException;

public interface WorkLambda<T> {
  T getResultValue( Result result );
}

/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.configuration;

import java.sql.Connection;

/**
 * An oracle database connection (session) with selects executed on regular intervals to help to
 * keep long running sessions alive.
 *
 * @see java.sql.Connection;
 */
public class OracleHeartBeatConnection extends HeartBeatConnection {
  static final String SELECT_QUERY = "select 1 from dual";

  /** @param connection The database connection. */
  public OracleHeartBeatConnection(Connection connection) {
    super(connection, SELECT_QUERY);
  }

  /**
   * @param connection The database connection.
   * @param heartbeat_interval The interval to execute regular selects.
   */
  public OracleHeartBeatConnection(Connection connection, int heartbeat_interval) {
    super(connection, heartbeat_interval, SELECT_QUERY);
  }
}

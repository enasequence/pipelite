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

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.Test;

public class OracleHeartBeatConnectionTest {
  @Test
  public void test() throws ClassNotFoundException, SQLException {
    Properties p = new Properties();
    p.put("user", "era_reader");
    p.put("password", "reader");
    p.put("SetBigStringTryClob", "true");

    Class.forName("oracle.jdbc.driver.OracleDriver");

    Connection connection =
        DriverManager.getConnection(
            "jdbc:oracle:thin:@(DESCRIPTION ="
                + "(ADDRESS_LIST = "
                + "(ADDRESS = "
                + "(PROTOCOL = TCP)"
                + "(HOST = ora-dlvm5-008.ebi.ac.uk)"
                + "(PORT = 1521)"
                + ")"
                + ")"
                + "(CONNECT_DATA = "
                + "(SERVICE_NAME = VERADEVT)"
                + "(SERVER = SHARED)"
                + ")"
                + ")",
            p);

    connection.setAutoCommit(false);

    OracleHeartBeatConnection hc = new OracleHeartBeatConnection(connection, 4000);

    assertTrue(hc.isValid(0));

    hc.rollback();

    hc.close();
  }
}

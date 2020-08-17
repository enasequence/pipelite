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

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import pipelite.TestConfiguration;

import javax.sql.DataSource;
import javax.transaction.Transactional;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
public class HeartBeatConnectionTest {

  @Autowired DataSource dataSource;

  @Test
  @Transactional
  @Rollback
  public void test() throws SQLException {

    HeartBeatConnection hc =
        new HeartBeatConnection(
            DataSourceUtils.getConnection(dataSource),
            4000,
            OracleHeartBeatConnection.SELECT_QUERY);

    assertTrue(hc.isValid(0));
  }
}

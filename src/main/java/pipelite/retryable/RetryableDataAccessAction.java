/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.retryable;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.time.ZonedDateTime;
import lombok.extern.flogger.Flogger;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import pipelite.configuration.RetryableDataSourceConfiguration;
import pipelite.exception.PipeliteException;
import pipelite.time.Time;

@Flogger
public class RetryableDataAccessAction<T> {

  private final RetryableDataSourceConfiguration configuration;

  public RetryableDataAccessAction(RetryableDataSourceConfiguration configuration) {
    this.configuration = configuration;
  }

  public <T> T execute(RetryableAction<T, SQLException> action) throws SQLException {
    ZonedDateTime since = ZonedDateTime.now();
    while (true) {
      try {
        return action.get();
      } catch (TransientDataAccessException /* From Spring */
          | RecoverableDataAccessException /* From Spring */
          | SQLTransientException /* From JDBC/HikariCP */
          | SQLRecoverableException /* From JDBC/HikariCP */ ex) {
        log.atSevere().log("Recoverable data access exception: " + ex.getMessage());
        if (configuration.isRetryable(since)) {
          Time.wait(configuration.getDelay());
        } else {
          log.atSevere().log("Maximum data access retry time exceeded: " + ex.getMessage(), ex);
          throw ex;
        }
      } catch (DataAccessException ex) {
        // From Spring
        throw ex;
      } catch (SQLException ex) {
        // From JDBC/HikariCP
        throw ex;
      } catch (PipeliteException ex) {
        // From Pipelite
        throw ex;
      } catch (Throwable ex) {
        throw new PipeliteException("Unrecoverable data access exception: " + ex.getMessage(), ex);
      }
    }
  }
}

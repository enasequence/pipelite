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
package pipelite.lock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pipelite.process.instance.ProcessInstance;

@Slf4j
@RequiredArgsConstructor
public class ProcessInstanceOraclePackageLocker implements ProcessInstanceLocker {

  private final Connection connection;

  @Override
  public boolean lock(String launcherId, ProcessInstance processInstance) {
    try (PreparedStatement ps =
        connection.prepareStatement("{ call pipelite_lock_pkg.try_lock( ?, ?, ? ) }")) {
      ps.setString(1, processInstance.getPipelineName());
      ps.setString(2, processInstance.getProcessId());
      ps.setString(3, launcherId);
      ps.execute();
      return true;
    } catch (SQLException e) {
      log.error("SQLException", e);
      return false;
    }
  }

  @Override
  public boolean isLocked(ProcessInstance processInstance) {
    try (PreparedStatement ps =
        connection.prepareStatement("{ call pipelite_lock_pkg.is_locked( ?, ? ) }")) {
      ps.setString(1, processInstance.getPipelineName());
      ps.setString(2, processInstance.getProcessId());
      ps.execute();
      return true;
    } catch (SQLException e) {
      log.error("SQLException", e);
      return false;
    }
  }

  @Override
  public boolean unlock(String launcherId, ProcessInstance processInstance) {
    try (PreparedStatement ps =
        connection.prepareStatement("{ call pipelite_lock_pkg.unlock( ?, ?, ? ) }")) {
      ps.setString(1, processInstance.getPipelineName());
      ps.setString(2, processInstance.getProcessId());
      ps.setString(3, launcherId);
      ps.execute();
      return true;

    } catch (SQLException e) {
      log.error("SQLException", e);
      return false;
    }
  }

  @Override
  public void purge(String launcherId, String processName) {
    try (PreparedStatement ps =
        connection.prepareStatement("{ call pipelite_lock_pkg.purge_locks( ?, ? ) }")) {
      ps.setString(1, processName);
      ps.setString(2, launcherId);
      ps.execute();
    } catch (SQLException e) {
      log.error("SQLException", e);
    }
  }
}

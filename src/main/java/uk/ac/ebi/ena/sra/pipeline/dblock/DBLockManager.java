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
package uk.ac.ebi.ena.sra.pipeline.dblock;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import uk.ac.ebi.ena.sra.pipeline.filelock.FileLockInfo;
import uk.ac.ebi.ena.sra.pipeline.launcher.LauncherLockManager;
import uk.ac.ebi.ena.sra.pipeline.resource.ProcessResourceLock;
import uk.ac.ebi.ena.sra.pipeline.resource.ResourceLock;
import uk.ac.ebi.ena.sra.pipeline.resource.ResourceLocker;
import uk.ac.ebi.ena.sra.pipeline.resource.StageResourceLock;

public class DBLockManager implements LauncherLockManager, ResourceLocker {
  private final Connection connection;
  private final String pipeline_name;
  private final String allocator_name;
  private final Logger log = Logger.getLogger(this.getClass());
  ExecutorService e = Executors.newSingleThreadExecutor();
  private final AbstractPingPong pingpong;

  public DBLockManager(Connection connection, String pipeilne_name) throws InterruptedException {
    this.connection = connection;
    this.pipeline_name = pipeilne_name;
    String allocator_name = ManagementFactory.getRuntimeMXBean().getName();

    e.submit(
        pingpong =
            new AbstractPingPong(
                0, allocator_name.split("@")[1], Integer.valueOf(allocator_name.split("@")[0])) {
              private Pattern lock_pattern = Pattern.compile("^([\\d]+)@([^:]+):([\\d]{2,5})$");

              @Override
              public FileLockInfo parseFileLock(String request_line) {
                Matcher m = lock_pattern.matcher(request_line);
                if (m.matches()) {
                  log.info("To parse: " + request_line);
                  return new FileLockInfo(
                      null, Integer.parseInt(m.group(1)), m.group(2), Integer.parseInt(m.group(3)));
                }

                return null;
              }

              @Override
              public String formFileLock(FileLockInfo info) {
                return String.format("%d@%s:%d", info.pid, info.machine, info.port);
              }
            });

    this.allocator_name = pingpong.formFileLock(pingpong.getLockInfo());
  }

  public void purgeDead() throws InterruptedException {
    try (PreparedStatement ps =
        connection.prepareStatement("select * from table( pipelite_lock_pkg.get_allocators() )")) {
      ps.execute();
      try (ResultSet rs = ps.getResultSet()) {
        while (rs.next()) {
          String pipeline_name = rs.getString("pipeline_name");
          String allocator_name = rs.getString("allocator_name");
          if (!this.pipeline_name.equals(pipeline_name)
              || this.allocator_name.equals(allocator_name)) continue;

          FileLockInfo lock_info = pingpong.parseFileLock(allocator_name);
          if (null == lock_info) {
            log.info("cannot parse lock: " + allocator_name);
            continue;
          }

          try {
            int attempt = 3;
            int attempt_index = 0;
            while (!pingpong.pingLockOwner(lock_info)) {
              log.info(++attempt_index + " attempt failed to ping " + allocator_name);
              if (attempt-- == 0) {
                purge(allocator_name);
                break;
              }
              Thread.sleep((1 << attempt_index) * 1000);
            }
          } catch (IOException e) {
            log.info("Cannot purge lock " + allocator_name);
          }
        }
      }
    } catch (SQLException e) {
      log.info("ERROR: " + e.getMessage());
    }
  }

  public Connection getConnection() {
    return this.connection;
  }

  @Override
  public void close() throws Exception {
    purge(pingpong.formFileLock(pingpong.getLockInfo()));
    pingpong.stop();
  }

  @Override
  public boolean tryLock(String lock_id) {
    try (PreparedStatement ps =
        connection.prepareStatement("{ call pipelite_lock_pkg.try_lock( ?, ?, ? ) }")) {
      ps.setString(1, this.pipeline_name);

      if (null == lock_id) {
        ps.setNull(2, Types.VARCHAR);
      } else {
        ps.setString(2, lock_id);
      }

      ps.setString(3, allocator_name);

      ps.execute();
      return true;
    } catch (SQLException e) {
      log.error("ERROR: " + e.getMessage());
      return false;
    }
  }

  @Override
  public boolean isLocked(String lock_id) {
    try (PreparedStatement ps =
        connection.prepareStatement("{ call pipelite_lock_pkg.is_locked( ?, ?, ? ) }")) {
      ps.setString(1, this.pipeline_name);

      if (null == lock_id) {
        ps.setNull(2, Types.VARCHAR);
      } else {
        ps.setString(2, lock_id);
      }

      ps.setNull(3, Types.VARCHAR);
      ps.execute();
      return true;
    } catch (SQLException e) {
      return false;
    }
  }

  @Override
  public boolean isBeingHeld(String lock_id) {
    try (PreparedStatement ps =
        connection.prepareStatement("{ call pipelite_lock_pkg.is_locked( ?, ?, ? ) }")) {
      ps.setString(1, this.pipeline_name);

      if (null == lock_id) {
        ps.setNull(2, Types.VARCHAR);
      } else {
        ps.setString(2, lock_id);
      }

      ps.setString(3, allocator_name);
      ps.execute();
      return true;
    } catch (SQLException e) {
      return false;
    }
  }

  @Override
  public boolean unlock(String lock_id) {
    try (PreparedStatement ps =
        connection.prepareStatement("{ call pipelite_lock_pkg.unlock( ?, ?, ? ) }")) {
      ps.setString(1, this.pipeline_name);

      if (null == lock_id) {
        ps.setNull(2, Types.VARCHAR);
      } else {
        ps.setString(2, lock_id);
      }
      ps.setString(3, allocator_name);
      ps.execute();
      return true;

    } catch (SQLException e) {
      log.error("ERROR: " + e.getMessage());
      return false;
    }
  }

  @Override
  public boolean terminate(String lock_id) {
    try (PreparedStatement ps =
        connection.prepareStatement("{ call pipelite_lock_pkg.unlock( ?, ?, ? ) }")) {
      ps.setString(1, this.pipeline_name);

      if (null == lock_id) {
        ps.setNull(2, Types.VARCHAR);
      } else {
        ps.setString(2, lock_id);
      }

      ps.setNull(3, Types.VARCHAR);
      ps.execute();
      return true;

    } catch (SQLException e) {
      log.error("ERROR: " + e.getMessage());
      return false;
    }
  }

  @Override
  public void purge(String allocator_name) {
    try (PreparedStatement ps =
        connection.prepareStatement("{ call pipelite_lock_pkg.purge_locks( ?, ? ) }")) {
      ps.setString(1, this.pipeline_name);
      ps.setString(2, allocator_name);
      ps.execute();

    } catch (SQLException e) {
      log.error("ERROR: " + e.getMessage());
    }
  }

  @Override
  public boolean lock(StageResourceLock rl) {
    return tryLock(composeLock(rl));
  }

  @Override
  public boolean unlock(StageResourceLock rl) {
    return unlock(composeLock(rl));
  }

  @Override
  public boolean is_locked(StageResourceLock rl) {
    return isBeingHeld(composeLock(rl));
  }

  @Override
  public boolean lock(ProcessResourceLock rl) {
    return tryLock(composeLock(rl));
  }

  @Override
  public boolean unlock(ProcessResourceLock rl) {
    return unlock(composeLock(rl));
  }

  @Override
  public boolean is_locked(ProcessResourceLock rl) {
    return isBeingHeld(composeLock(rl));
  }

  private String composeLock(ResourceLock rl) {
    return rl.getLockId();
  }
}

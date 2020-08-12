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
package uk.ac.ebi.ena.sra.pipeline.filelock;

public class FileLockInfo {
  public final int port;
  public final String machine;
  public final int pid;
  public final String path;

  public FileLockInfo(String path, String machine_id, int port) {
    this.path = path;
    this.pid = Integer.parseInt(machine_id.split("@")[0]);
    this.machine = machine_id.split("@")[1];
    this.port = port;
  }

  public FileLockInfo(String path, int pid, String machine, int port) {
    this.path = path;
    this.pid = pid;
    this.machine = machine;
    this.port = port;
  }

  public String toString() {
    return String.format("%s [pid: %s, host: %s, port: %d]", path, pid, machine, port);
  }
}

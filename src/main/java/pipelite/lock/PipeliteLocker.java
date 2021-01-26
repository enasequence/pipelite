/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.lock;

import pipelite.entity.ServiceLockEntity;

/**
 * Manages locks. Service locks prevent services with the same name from being executed at the same
 * time while process locks prevent more than one instance of a process being executed at the same
 * time. Service locks may expire. If they do then the service lock and any associated process locks
 * will be removed. Lock expiry can be prevented by renewing the service lock.
 */
public interface PipeliteLocker extends AutoCloseable {

  /**
   * Returns the service name.
   *
   * @return the service name
   */
  String getServiceName();

  /**
   * Locks a process.
   *
   * @return false if the process could not be locked
   */
  boolean lockProcess(String pipelineName, String processId);

  /**
   * Unlocks a process.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   */
  void unlockProcess(String pipelineName, String processId);

  /**
   * Returns the service lock.
   *
   * @return the service lock
   */
  ServiceLockEntity getLock();
}

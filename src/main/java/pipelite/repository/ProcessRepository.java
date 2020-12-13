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
package pipelite.repository;

import java.util.stream.Stream;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ProcessEntityId;
import pipelite.process.ProcessState;

@Repository
public interface ProcessRepository extends CrudRepository<ProcessEntity, ProcessEntityId> {

  /**
   * Finds all processes.
   *
   * @return all processes
   */
  @Query(value = "SELECT * FROM PIPELITE_PROCESS", nativeQuery = true)
  Stream<ProcessEntity> findAllStream();

  /**
   * Finds all processes given process state.
   *
   * @param state the process state
   * @return all processes given process state
   */
  Stream<ProcessEntity> findAllByState(ProcessState state);

  /**
   * Finds all processes given process id.
   *
   * @param processId the process id
   * @return all processes given process id
   */
  Stream<ProcessEntity> findAllByProcessId(String processId);

  /**
   * Finds all processes given pipeline name.
   *
   * @param pipelineName the pipeline name
   * @return all processes given pipeline name
   */
  Stream<ProcessEntity> findAllByPipelineName(String pipelineName);

  /**
   * Finds all processes given pipeline name and process state.
   *
   * @param pipelineName the pipeline name
   * @param state the process state
   * @return all processes given pipeline name and process state
   */
  Stream<ProcessEntity> findAllByPipelineNameAndState(String pipelineName, ProcessState state);

  /**
   * Finds processes given pipeline name and process state in priority order.
   *
   * @param pipelineName the pipeline name
   * @param state the process state
   * @return the processes given pipeline name and process state in priority order
   */
  Stream<ProcessEntity> findAllByPipelineNameAndStateOrderByPriorityDesc(
      String pipelineName, ProcessState state);

  /**
   * Finds active processes except ones that are locked by other launchers.
   *
   * @param pipelineName the pipeline name
   * @param launcherName the launcher name
   * @return the active processes except ones that are locked by other launchers
   */
  @Query(
      value =
          "SELECT * FROM PIPELITE_PROCESS A WHERE PIPELINE_NAME = ?1 AND STATE = 'ACTIVE' AND NOT EXISTS (SELECT 1 FROM PIPELITE_PROCESS_LOCK B JOIN PIPELITE_LAUNCHER_LOCK C USING (LAUNCHER_ID) WHERE A.PIPELINE_NAME = B.PIPELINE_NAME AND A.PROCESS_ID = B.PROCESS_ID AND C.LAUNCHER_NAME IS NOT NULL) ORDER BY PRIORITY DESC",
      nativeQuery = true)
  Stream<ProcessEntity> findActiveOrderByPriorityDesc(String pipelineName, String launcherName);

  /**
   * Finds maximum process id for a pipeline.
   *
   * @param pipelineName the pipeline name
   * @return maximum process id for a pipeline
   */
  @Query(
      value = "SELECT MAX(PROCESS_ID) FROM PIPELITE_PROCESS A WHERE PIPELINE_NAME = ?1",
      nativeQuery = true)
  String findMaxProcessId(String pipelineName);
}

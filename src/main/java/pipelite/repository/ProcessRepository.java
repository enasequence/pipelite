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

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ProcessEntityId;
import pipelite.process.ProcessState;

@Repository
public interface ProcessRepository extends CrudRepository<ProcessEntity, ProcessEntityId> {

  List<ProcessEntity> findAllByPipelineNameAndState(String pipelineName, ProcessState state);

  List<ProcessEntity> findAllByPipelineNameAndStateOrderByPriorityDesc(
      String pipelineName, ProcessState state);

  /** Finds active processes except ones that are locked by other launchers. */
  @Query(
      value =
          "SELECT * FROM PIPELITE_PROCESS A WHERE PIPELINE_NAME = ?1 AND STATE = 'ACTIVE' AND NOT EXISTS (SELECT 1 FROM PIPELITE_PROCESS_LOCK B JOIN PIPELITE_LAUNCHER_LOCK C USING (LAUNCHER_ID) WHERE A.PIPELINE_NAME = B.PIPELINE_NAME AND A.PROCESS_ID = B.PROCESS_ID AND C.LAUNCHER_NAME <> ?2) ORDER BY PRIORITY DESC",
      nativeQuery = true)
  List<ProcessEntity> findActiveOrderByPriorityDesc(String pipelineName, String launcherName);
}

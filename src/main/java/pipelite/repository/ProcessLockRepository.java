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

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import pipelite.entity.ProcessLockEntity;
import pipelite.entity.ProcessLockEntityId;

import java.util.Optional;

@Repository
public interface ProcessLockRepository
    extends CrudRepository<ProcessLockEntity, ProcessLockEntityId> {
  Optional<ProcessLockEntity> findByLauncherIdAndPipelineNameAndProcessId(
      Long launcherId, String pipelineName, String processId);

  Optional<ProcessLockEntity> findByPipelineNameAndProcessId(String pipelineName, String processId);

  void deleteByPipelineName(String pipelineName);

  void deleteByLauncherId(Long launcherId);
}

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
package pipelite.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.process.ProcessFactory;

@Service
public class ProcessFactoryService {

  private final Map<String, ProcessFactory> map = new HashMap<>();

  public ProcessFactoryService(@Autowired List<ProcessFactory> factories) {
    for (ProcessFactory factory : factories) {
      if (map.containsKey(factory.getPipelineName())) {
        throw new ProcessFactoryServiceException(
            "Non-unique pipeline: " + factory.getPipelineName());
      }
      map.put(factory.getPipelineName(), factory);
    }
  }

  /**
   * Creates a process factory.
   *
   * @param pipelineName the pipeline name. A pipeline is identified by its name.
   * @return the process factory for the pipeline.
   * @throws ProcessFactoryServiceException if the pipeline is not supported by this factory or if
   *     the pipeline name is null or empty.
   */
  public ProcessFactory create(String pipelineName) {
    if (pipelineName == null || pipelineName.trim().isEmpty()) {
      throw new ProcessFactoryServiceException("Missing pipeline name");
    }
    if (!map.containsKey(pipelineName)) {
      throw new ProcessFactoryServiceException("Unknown pipeline: " + pipelineName);
    }
    return map.get(pipelineName);
  }
}

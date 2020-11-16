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
import pipelite.process.ProcessSource;

@Service
public class ProcessSourceService {

  private final Map<String, ProcessSource> map = new HashMap<>();

  public ProcessSourceService(@Autowired List<ProcessSource> sources) {
    for (ProcessSource source : sources) {
      if (map.containsKey(source.getPipelineName())) {
        throw new ProcessSourceServiceException("Non-unique pipeline: " + source.getPipelineName());
      }
      map.put(source.getPipelineName(), source);
    }
  }

  /**
   * Creates a process source.
   *
   * @param pipelineName the pipeline name. A pipeline is identified by its name.
   * @return the process source for the pipeline.
   * @throws ProcessFactoryServiceException if the pipeline name is null or empty.
   */
  public ProcessSource create(String pipelineName) {
    if (pipelineName == null || pipelineName.trim().isEmpty()) {
      throw new ProcessSourceServiceException("Missing pipeline name");
    }
    return map.get(pipelineName);
  }
}

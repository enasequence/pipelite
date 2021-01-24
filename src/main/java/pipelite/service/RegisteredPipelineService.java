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
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.Pipeline;
import pipelite.exception.PipeliteException;

@Service
@Flogger
public class RegisteredPipelineService {

  private final Map<String, Pipeline> map = new HashMap<>();

  public RegisteredPipelineService(@Autowired List<Pipeline> pipelines) {
    for (Pipeline pipeline : pipelines) {
      if (map.containsKey(pipeline.getPipelineName())) {
        throw new PipeliteException("Non-unique pipeline: " + pipeline.getPipelineName());
      }
      map.put(pipeline.getPipelineName(), pipeline);
    }
  }

  /**
   * Returns the registered pipeline names.
   *
   * @return the registered pipeline names
   */
  public List<String> getPipelineNames() {
    return map.keySet().stream().collect(Collectors.toList());
  }

  /**
   * Returns a registered pipeline.
   *
   * @param pipelineName the pipeline name. A pipeline is identified by its name.
   * @return the registered pipeline.
   * @throws PipeliteException if the pipeline was not found
   */
  public Pipeline getPipeline(String pipelineName) {
    if (pipelineName == null || pipelineName.trim().isEmpty()) {
      throw new PipeliteException("Missing pipeline name");
    }
    if (!map.containsKey(pipelineName)) {
      log.atSevere().log("Unknown pipeline: " + pipelineName);
      throw new PipeliteException("Unknown pipeline: " + pipelineName);
    }
    return map.get(pipelineName);
  }
}

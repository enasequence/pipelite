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
package pipelite.process;

import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.service.ProcessFactoryService;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Flogger
public class ProcessFactoryCache {

  private final ProcessFactoryService processFactoryService;
  private final Map<String, ProcessFactory> processFactoryCache = new ConcurrentHashMap<>();

  public ProcessFactoryCache(ProcessFactoryService processFactoryService) {
    Assert.notNull(processFactoryService, "Missing process factory service");
    this.processFactoryService = processFactoryService;
  }

  public ProcessFactory getProcessFactory(String pipelineName) {
    if (processFactoryCache.containsKey(pipelineName)) {
      return processFactoryCache.get(pipelineName);
    }
    processFactoryCache.putIfAbsent(pipelineName, processFactoryService.create(pipelineName));
    return processFactoryCache.get(pipelineName);
  }
}

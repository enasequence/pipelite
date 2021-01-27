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
package pipelite.launcher;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.Schedule;
import pipelite.service.RegisteredPipelineService;

@Flogger
public class ScheduleCache {

  private final RegisteredPipelineService service;
  private final Map<String, Schedule> cache = new ConcurrentHashMap<>();

  public ScheduleCache(RegisteredPipelineService service) {
    Assert.notNull(service, "Missing registered pipeline service");
    this.service = service;
  }

  public Schedule getSchedule(String pipelineName) {
    if (cache.containsKey(pipelineName)) {
      return cache.get(pipelineName);
    }
    Schedule schedule = service.getRegisteredPipeline(pipelineName, Schedule.class);
    if (schedule == null) {
      return null;
    }
    cache.putIfAbsent(pipelineName, schedule);
    return cache.get(pipelineName);
  }
}

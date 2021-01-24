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
import pipelite.Scheduler;
import pipelite.exception.PipeliteException;

@Service
@Flogger
public class RegisteredSchedulerService {

  private final Map<String, Scheduler> map = new HashMap<>();

  public RegisteredSchedulerService(@Autowired List<Scheduler> schedulers) {
    for (Scheduler scheduler : schedulers) {
      if (map.containsKey(scheduler.getSchedulerName())) {
        throw new PipeliteException("Non-unique schedule: " + scheduler.getSchedulerName());
      }
      map.put(scheduler.getSchedulerName(), scheduler);
    }
  }

  /**
   * Returns the registered scheduler names.
   *
   * @return the registered scheduler names
   */
  public List<String> getSchedulerNames() {
    return map.keySet().stream().collect(Collectors.toList());
  }
}

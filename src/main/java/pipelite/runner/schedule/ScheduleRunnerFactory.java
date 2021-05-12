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
package pipelite.runner.schedule;

import java.util.List;
import pipelite.Schedule;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.metrics.PipeliteMetrics;
import pipelite.runner.process.ProcessRunner;
import pipelite.service.PipeliteServices;

public class ScheduleRunnerFactory {

  private ScheduleRunnerFactory() {}

  public static ScheduleRunner create(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      List<Schedule> schedules) {
    boolean lockProcess = false;
    return new ScheduleRunner(
        pipeliteConfiguration,
        pipeliteServices,
        pipeliteMetrics,
        schedules,
        (pipelineName1, process1) ->
            new ProcessRunner(
                pipeliteConfiguration,
                pipeliteServices,
                pipeliteMetrics,
                pipelineName1,
                process1,
                lockProcess));
  }
}

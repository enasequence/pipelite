/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Service;
import pipelite.runner.pipeline.PipelineRunner;
import pipelite.runner.schedule.ScheduleRunner;

@Service
public class RunnerService {

  /** Registered schedule runners. */
  private ScheduleRunner scheduleRunner;

  /** Registered pipeline runners. */
  private final List<PipelineRunner> pipelineRunners = new ArrayList<>();

  public ScheduleRunner getScheduleRunner() {
    return scheduleRunner;
  }

  public boolean isScheduleRunner() {
    return scheduleRunner != null;
  }

  public void clearScheduleRunner() {
    this.scheduleRunner = null;
  }

  public void setScheduleRunner(ScheduleRunner scheduleRunner) {
    this.scheduleRunner = scheduleRunner;
  }

  public List<PipelineRunner> getPipelineRunners() {
    return pipelineRunners;
  }

  public Optional<PipelineRunner> getPipelineRunner(String pipelineName) {
    return pipelineRunners.stream()
        .filter(e -> e.getPipelineName().equals(pipelineName))
        .findFirst();
  }

  public void clearPipelineRunners() {
    this.pipelineRunners.clear();
  }

  public void addPipelineRunner(PipelineRunner pipelineRunner) {
    this.pipelineRunners.add(pipelineRunner);
  }
}

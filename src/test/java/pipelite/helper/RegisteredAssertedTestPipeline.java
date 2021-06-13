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
package pipelite.helper;

import pipelite.metrics.PipeliteMetrics;
import pipelite.service.ProcessService;
import pipelite.service.ScheduleService;
import pipelite.service.StageService;

public abstract class RegisteredAssertedTestPipeline extends RegisteredConfiguredTestPipeline {

  public abstract void assertCompletedScheduleEntity(
      ScheduleService scheduleService, String serviceName, int expectedProcessCnt);

  public abstract void assertCompletedProcessEntities(
      ProcessService processService, int expectedProcessCnt);

  public abstract void assertCompletedMetrics(PipeliteMetrics metrics, int expectedProcessCnt);

  public abstract void assertSubmittedStageEntities(
      StageService stageService, int expectedProcessCnt);

  public abstract void assertCompletedStageEntities(
      StageService stageService, int expectedProcessCnt);
}

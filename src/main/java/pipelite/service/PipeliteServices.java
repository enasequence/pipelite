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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PipeliteServices {
  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final StageService stageService;
  private final MailService mailService;
  private final PipeliteLockerService pipeliteLockerService;
  private final RegisteredPipelineService registeredPipelineService;
  private final InternalErrorService internalErrorService;
  private final HealthCheckService healthCheckService;
  private final RunnerService runnerService;

  public PipeliteServices(
      @Autowired ScheduleService scheduleService,
      @Autowired ProcessService processService,
      @Autowired StageService stageService,
      @Autowired MailService mailService,
      @Autowired PipeliteLockerService pipeliteLockerService,
      @Autowired RegisteredPipelineService registeredPipelineService,
      @Autowired InternalErrorService internalErrorService,
      @Autowired HealthCheckService healthCheckService,
      @Autowired RunnerService runnerService) {
    this.scheduleService = scheduleService;
    this.processService = processService;
    this.stageService = stageService;
    this.mailService = mailService;
    this.pipeliteLockerService = pipeliteLockerService;
    this.registeredPipelineService = registeredPipelineService;
    this.internalErrorService = internalErrorService;
    this.healthCheckService = healthCheckService;
    this.runnerService = runnerService;
  }

  public ScheduleService schedule() {
    return scheduleService;
  }

  public ProcessService process() {
    return processService;
  }

  public StageService stage() {
    return stageService;
  }

  public MailService mail() {
    return mailService;
  }

  public PipeliteLockerService locker() {
    return pipeliteLockerService;
  }

  public RegisteredPipelineService registeredPipeline() {
    return registeredPipelineService;
  }

  public InternalErrorService internalError() {
    return internalErrorService;
  }

  public HealthCheckService healthCheck() {
    return healthCheckService;
  }

  public RunnerService runner() {
    return runnerService;
  }
}

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

import org.springframework.util.Assert;
import pipelite.lock.PipeliteLocker;
import pipelite.service.*;

public class PipeliteServices {

  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final StageService stageService;
  private final MailService mailService;
  private final PipeliteLocker lockerService;
  private final RegisteredPipelineService registeredPipelineService;
  private final InternalErrorService internalErrorService;
  private final HealthCheckService healthCheckService;

  public PipeliteServices(
      ScheduleService scheduleService,
      ProcessService processService,
      StageService stageService,
      MailService mailService,
      PipeliteLocker lockerService,
      RegisteredPipelineService registeredPipelineService,
      InternalErrorService internalErrorService,
      HealthCheckService healthCheckService) {
    Assert.notNull(scheduleService, "Missing schedule service");
    Assert.notNull(processService, "Missing process service");
    Assert.notNull(stageService, "Missing stage service");
    Assert.notNull(mailService, "Missing mail service");
    Assert.notNull(lockerService, "Missing locker service");
    Assert.notNull(registeredPipelineService, "Missing registered pipeline service");
    Assert.notNull(healthCheckService, "Missing health check service");
    this.scheduleService = scheduleService;
    this.processService = processService;
    this.stageService = stageService;
    this.mailService = mailService;
    this.lockerService = lockerService;
    this.registeredPipelineService = registeredPipelineService;
    this.internalErrorService = internalErrorService;
    this.healthCheckService = healthCheckService;
  }

  public PipeliteServices(
      ScheduleService scheduleService,
      ProcessService processService,
      StageService stageService,
      MailService mailService,
      PipeliteLockerService lockerService,
      RegisteredPipelineService registeredPipelineService,
      InternalErrorService internalErrorService,
      HealthCheckService healthCheck) {
    this(
        scheduleService,
        processService,
        stageService,
        mailService,
        lockerService.getPipeliteLocker(),
        registeredPipelineService,
        internalErrorService,
        healthCheck);
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

  public PipeliteLocker locker() {
    return lockerService;
  }

  public RegisteredPipelineService registeredPipeline() {
    return registeredPipelineService;
  }

  public InternalErrorService internalError() {
    return internalErrorService;
  }

  public HealthCheckService healthCheckService() {
    return healthCheckService;
  }
}

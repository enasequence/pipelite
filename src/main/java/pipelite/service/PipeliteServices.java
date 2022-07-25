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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.metrics.PipeliteMetrics;

@Component
public class PipeliteServices {
  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final StageService stageService;
  private final DescribeJobsService describeJobsService;
  private final MailService mailService;
  private final PipeliteLockerService pipeliteLockerService;
  private final RegisteredPipelineService registeredPipelineService;
  private final InternalErrorService internalErrorService;
  private final DataSourceHealthCheckService healthCheckService;
  private final RunnerService runnerService;
  private final PipeliteExecutorService pipeliteExecutorService;
  private final PipeliteMetrics pipeliteMetrics;

  public PipeliteServices(
      @Autowired ScheduleService scheduleService,
      @Autowired ProcessService processService,
      @Autowired StageService stageService,
      @Autowired DescribeJobsService describeJobsService,
      @Autowired MailService mailService,
      @Autowired PipeliteLockerService pipeliteLockerService,
      @Autowired RegisteredPipelineService registeredPipelineService,
      @Autowired InternalErrorService internalErrorService,
      @Autowired DataSourceHealthCheckService healthCheckService,
      @Autowired RunnerService runnerService,
      @Autowired PipeliteExecutorService pipeliteExecutorService,
      @Autowired PipeliteMetrics pipeliteMetrics) {
    this.scheduleService = scheduleService;
    this.processService = processService;
    this.stageService = stageService;
    this.describeJobsService = describeJobsService;
    this.mailService = mailService;
    this.pipeliteLockerService = pipeliteLockerService;
    this.registeredPipelineService = registeredPipelineService;
    this.internalErrorService = internalErrorService;
    this.healthCheckService = healthCheckService;
    this.runnerService = runnerService;
    this.pipeliteExecutorService = pipeliteExecutorService;
    this.pipeliteMetrics = pipeliteMetrics;
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

  public DescribeJobsService jobs() {
    return describeJobsService;
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

  public DataSourceHealthCheckService healthCheck() {
    return healthCheckService;
  }

  public RunnerService runner() {
    return runnerService;
  }

  public PipeliteExecutorService executor() {
    return pipeliteExecutorService;
  }

  public PipeliteMetrics metrics() {
    return pipeliteMetrics;
  }
}

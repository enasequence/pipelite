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

import static org.assertj.core.api.Assertions.assertThat;

import pipelite.helper.entity.ProcessEntityTestHelper;
import pipelite.helper.entity.ScheduleEntityTestHelper;
import pipelite.helper.metrics.MetricsTestHelper;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.ProcessService;
import pipelite.service.ScheduleService;
import pipelite.service.StageService;

public abstract class RegisteredTestPipeline<T extends RegisteredTestPipeline>
    extends RegisteredAssertedTestPipeline {

  private final TestType testType;
  private final int immediateRetries;
  private final int maximumRetries;
  private final AssertSubmittedStageEntity assertSubmittedStageEntity;
  private final AssertCompletedStageEntity assertCompletedStageEntity;

  protected interface AssertSubmittedStageEntity<T> {
    void assertSubmittedStageEntity(
        StageService stageService,
        String pipelineName,
        String processId,
        String stageName,
        T registeredTestPipeline);
  }

  protected interface AssertCompletedStageEntity<T> {
    void assertCompletedStageEntity(
        StageService stageService,
        String pipelineName,
        String processId,
        String stageName,
        T registeredTestPipeline);
  }

  public RegisteredTestPipeline(
      TestType testType,
      int immediateRetries,
      int maximumRetries,
      AssertSubmittedStageEntity<T> assertSubmittedStageEntity,
      AssertCompletedStageEntity<T> assertCompletedStageEntity) {
    this.testType = testType;
    this.immediateRetries = immediateRetries;
    this.maximumRetries = maximumRetries;
    this.assertSubmittedStageEntity = assertSubmittedStageEntity;
    this.assertCompletedStageEntity = assertCompletedStageEntity;
  }

  public TestType testType() {
    return testType;
  }

  public int immediateRetries() {
    return immediateRetries;
  }

  public int maximumRetries() {
    return maximumRetries;
  }

  public AssertSubmittedStageEntity assertSubmittedStageEntity() {
    return assertSubmittedStageEntity;
  }

  public AssertCompletedStageEntity assertCompletedStageEntity() {
    return assertCompletedStageEntity;
  }

  @Override
  public final void assertCompletedScheduleEntity(
      ScheduleService scheduleService, String serviceName, int expectedProcessCnt) {
    ScheduleEntityTestHelper.assertCompletedSchduleEntity(
        scheduleService,
        serviceName,
        pipelineName(),
        expectedProcessCnt,
        configuredProcessIds(),
        testType());
  }

  @Override
  public final void assertCompletedProcessEntities(
      ProcessService processService, int expectedProcessCnt) {
    assertThat(expectedProcessCnt).isEqualTo(configuredProcessCount());
    for (String processId : configuredProcessIds()) {
      ProcessEntityTestHelper.assertCompletedProcessEntity(
          processService, pipelineName(), processId, testType());
    }
  }

  @Override
  public final void assertCompletedMetrics(PipeliteMetrics metrics, int expectedProcessCnt) {
    assertThat(expectedProcessCnt).isEqualTo(configuredProcessCount());
    MetricsTestHelper.assertCompletedMetrics(
        testType(),
        metrics,
        pipelineName(),
        expectedProcessCnt,
        immediateRetries(),
        maximumRetries());
  }
}
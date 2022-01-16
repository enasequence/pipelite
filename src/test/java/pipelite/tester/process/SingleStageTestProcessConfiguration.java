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
package pipelite.tester.process;

import static org.assertj.core.api.Assertions.assertThat;

import pipelite.UniqueStringGenerator;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.ProcessService;
import pipelite.service.ScheduleService;
import pipelite.service.StageService;
import pipelite.tester.TestType;
import pipelite.tester.entity.ProcessEntityAsserter;
import pipelite.tester.entity.ScheduleEntityAsserter;
import pipelite.tester.metrics.MetricsTestAsserter;

public abstract class SingleStageTestProcessConfiguration extends TestProcessConfiguration {

  private final TestType testType;
  private final AssertSubmittedStageEntity assertSubmittedStageEntity;
  private final AssertCompletedStageEntity assertCompletedStageEntity;
  private final String stageName =
      UniqueStringGenerator.randomStageName(SingleStageTestProcessConfiguration.class);

  protected interface AssertSubmittedStageEntity {
    void assertSubmittedStageEntity(
        StageService stageService, String pipelineName, String processId, String stageName);
  }

  protected interface AssertCompletedStageEntity {
    void assertCompletedStageEntity(
        StageService stageService, String pipelineName, String processId, String stageName);
  }

  public SingleStageTestProcessConfiguration(
      TestType testType,
      AssertSubmittedStageEntity assertSubmittedStageEntity,
      AssertCompletedStageEntity assertCompletedStageEntity) {
    this.testType = testType;
    this.assertSubmittedStageEntity = assertSubmittedStageEntity;
    this.assertCompletedStageEntity = assertCompletedStageEntity;
  }

  @Override
  protected void register(String processId) {
    testType.register(pipelineName(), processId, stageName);
  }

  public TestType testType() {
    return testType;
  }

  public int immediateRetries() {
    return testType.immediateRetries();
  }

  public int maximumRetries() {
    return testType.maximumRetries();
  }

  public String stageName() {
    return stageName;
  }

  public AssertSubmittedStageEntity assertSubmittedStageEntity() {
    return assertSubmittedStageEntity;
  }

  public AssertCompletedStageEntity assertCompletedStageEntity() {
    return assertCompletedStageEntity;
  }

  public void assertCompleted(
      ProcessService processService,
      StageService stageService,
      PipeliteMetrics metrics,
      int processCnt) {
    assertThat(configuredProcessIds().size()).isEqualTo(processCnt);
    assertCompletedMetrics(metrics, processCnt);
    assertCompletedProcessEntities(processService, processCnt);
    assertCompletedStageEntities(stageService, processCnt);
    assertThat(testType.failedAsserts()).isEmpty();
  }

  public final void assertCompletedScheduleEntity(
      ScheduleService scheduleService, String serviceName, int expectedProcessCnt) {
    ScheduleEntityAsserter.assertCompletedScheduleEntity(
        scheduleService,
        testType,
        serviceName,
        pipelineName(),
        expectedProcessCnt,
        configuredProcessIds());
  }

  public final void assertCompletedProcessEntities(
      ProcessService processService, int expectedProcessCnt) {
    assertThat(expectedProcessCnt).isEqualTo(configuredProcessCount());
    for (String processId : configuredProcessIds()) {
      ProcessEntityAsserter.assertCompletedProcessEntity(
          processService, testType, pipelineName(), processId);
    }
  }

  public final void assertCompletedMetrics(PipeliteMetrics metrics, int expectedProcessCnt) {
    assertThat(expectedProcessCnt).isEqualTo(configuredProcessCount());
    MetricsTestAsserter.assertCompletedMetrics(
        testType(), metrics, pipelineName(), expectedProcessCnt);
  }

  public final void assertSubmittedStageEntities(
      StageService stageService, int expectedProcessCnt) {
    assertThat(expectedProcessCnt).isEqualTo(configuredProcessCount());
    for (String processId : configuredProcessIds()) {
      assertSubmittedStageEntity()
          .assertSubmittedStageEntity(stageService, pipelineName(), processId, stageName());
    }
  }

  public final void assertCompletedStageEntities(
      StageService stageService, int expectedProcessCnt) {
    assertThat(expectedProcessCnt).isEqualTo(configuredProcessCount());
    for (String processId : configuredProcessIds()) {
      assertCompletedStageEntity()
          .assertCompletedStageEntity(stageService, pipelineName(), processId, stageName());
    }
  }
}

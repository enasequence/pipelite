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

import pipelite.UniqueStringGenerator;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.StageService;

public abstract class SingleStageTestProcessFactory extends CreateProcessPipelineTestHelper {

  private final TestType testType;
  private final int parallelism;
  private final int immediateRetries;
  private final int maximumRetries;
  private final String stageName = "STAGE";

  public SingleStageTestProcessFactory(
      TestType testType,
      int processCnt,
      int parallelism,
      int immediateRetries,
      int maximumRetries) {
    super(processCnt);
    this.testType = testType;
    this.parallelism = parallelism;
    this.immediateRetries = immediateRetries;
    this.maximumRetries = maximumRetries;
  }

  public pipelite.process.Process create() {
    String processId = UniqueStringGenerator.randomProcessId(SingleStageTestProcessFactory.class);
    ProcessBuilder processBuilder = new ProcessBuilder(processId);
    configureProcess(processBuilder);
    return processBuilder.build();
  }

  public TestType testType() {
    return testType;
  }

  @Override
  protected final int testConfigureParallelism() {
    return parallelism;
  }

  public int parallelism() {
    return parallelism;
  }

  public int immediateRetries() {
    return immediateRetries;
  }

  public int maximumRetries() {
    return maximumRetries;
  }

  public String stageName() {
    return stageName;
  }

  public void assertCompletedProcessEntity(ProcessService processService, String processId) {
    ProcessEntityTestHelper.assertCompletedProcessEntity(
        processService, pipelineName(), processId, testType());
  }

  public abstract void assertSubmittedStageEntity(StageService stageService, String processId);

  public abstract void assertCompletedStageEntity(StageService stageService, String processId);

  public void assertCompletedMetrics(PipeliteMetrics metrics) {
    MetricsTestHelper.assertCompletedMetrics(
        testType(), metrics, pipelineName(), processCnt(), immediateRetries(), maximumRetries());
  }
}

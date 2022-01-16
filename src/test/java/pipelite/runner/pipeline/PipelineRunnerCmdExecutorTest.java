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
package pipelite.runner.pipeline;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.service.StageService;
import pipelite.tester.TestTypePipelineRunner;
import pipelite.tester.process.SingleStageCmdTestProcessConfiguration;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerCmdExecutorTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test"})
@DirtiesContext
public class PipelineRunnerCmdExecutorTest {

  private static final int PARALLELISM = 10;
  private static final int PROCESS_CNT = 20;

  @Autowired TestTypePipelineRunner testRunner;

  // For TestType.spyStageService
  @SpyBean private StageService stageServiceSpy;

  @Test
  public void runPipelines() {
    testRunner.runPipelines(
        stageServiceSpy,
        PARALLELISM,
        PROCESS_CNT,
        testType -> new SingleStageCmdTestProcessConfiguration(testType));
  }
}

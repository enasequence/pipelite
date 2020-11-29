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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import pipelite.PipeliteTestConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.launcher.PipeliteSchedulerOracleTest;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.StageExecutionResultType;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = PipeliteTestConfiguration.class,
    properties = {
      "pipelite.mail.host=outgoing.ebi.ac.uk",
      "pipelite.mail.port=587",
      "pipelite.mail.from=pipelite-noreply@ebi.ac.uk",
      "pipelite.mail.to=rasko@ebi.ac.uk"
    })
@ContextConfiguration(initializers = PipeliteSchedulerOracleTest.TestContextInitializer.class)
@ActiveProfiles(value = {"hsql-test", "pipelite-test"})
public class MailServiceTest {

  @Autowired MailService mailService;

  @Test
  public void sendProcessExecutionMessage() {
    Process process =
        new ProcessBuilder("PROCESS_ID")
            .execute("STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .build();
    StageEntity stageEntity =
        StageEntity.createExecution("PIPELINE_NAME", "PROCESS_ID", process.getStages().get(0));
    process.getStages().get(0).setStageEntity(stageEntity);
    ProcessEntity processEntity =
        ProcessEntity.createExecution(
            "PIPELINE_NAME", "PROCESS_ID", ProcessEntity.DEFAULT_PRIORITY);
    process.setProcessEntity(processEntity);
    assertThat(mailService.getProcessExecutionSubject("PIPELINE_NAME", process))
        .isEqualTo("pipelite process (PENDING): PIPELINE_NAME/PROCESS_ID");
    assertThat(mailService.getExecutionBody(process, "SUBJECT"))
        .isEqualTo(
            "SUBJECT\n"
                + "\n"
                + "Process:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \"PROCESS_ID\",\n"
                + "  \"pipelineName\" : \"PIPELINE_NAME\",\n"
                + "  \"state\" : \"PENDING\",\n"
                + "  \"executionCount\" : 0,\n"
                + "  \"priority\" : 5\n"
                + "}\n"
                + "\n"
                + "Stages:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \"PROCESS_ID\",\n"
                + "  \"pipelineName\" : \"PIPELINE_NAME\",\n"
                + "  \"stageName\" : \"STAGE1\",\n"
                + "  \"executionCount\" : 0\n"
                + "}\n");
    // mailService.sendProcessExecutionMessage("PIPELINE_NAME", process);
  }

  @Test
  public void sendStageExecutionMessage() {
    Process process =
        new ProcessBuilder("PROCESS_ID")
            .execute("STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .build();
    StageEntity stageEntity =
        StageEntity.createExecution("PIPELINE_NAME", "PROCESS_ID", process.getStages().get(0));
    process.getStages().get(0).setStageEntity(stageEntity);
    ProcessEntity processEntity =
        ProcessEntity.createExecution(
            "PIPELINE_NAME", "PROCESS_ID", ProcessEntity.DEFAULT_PRIORITY);
    process.setProcessEntity(processEntity);
    assertThat(
            mailService.getStageExecutionSubject(
                "PIPELINE_NAME", process, process.getStages().get(0)))
        .isEqualTo("pipelite stage (PENDING): PIPELINE_NAME/PROCESS_ID/STAGE1");
    assertThat(mailService.getExecutionBody(process, "SUBJECT"))
        .isEqualTo(
            "SUBJECT\n"
                + "\n"
                + "Process:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \"PROCESS_ID\",\n"
                + "  \"pipelineName\" : \"PIPELINE_NAME\",\n"
                + "  \"state\" : \"PENDING\",\n"
                + "  \"executionCount\" : 0,\n"
                + "  \"priority\" : 5\n"
                + "}\n"
                + "\n"
                + "Stages:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \"PROCESS_ID\",\n"
                + "  \"pipelineName\" : \"PIPELINE_NAME\",\n"
                + "  \"stageName\" : \"STAGE1\",\n"
                + "  \"executionCount\" : 0\n"
                + "}\n");
    // mailService.sendStageExecutionMessage("PIPELINE_NAME", process, process.getStages().get(0));
  }
}

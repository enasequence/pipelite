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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteIdCreator;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.entity.StageLogEntity;
import pipelite.entity.field.StageState;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=MailServiceTest"})
@DirtiesContext
@ActiveProfiles("test")
public class MailServiceTest {

  @Autowired MailService mailService;
  @Autowired StageService stageService;

  @Test
  public void sendProcessExecutionMessage() {
    Process process =
        new ProcessBuilder("PROCESS_ID").execute("STAGE1").withSyncTestExecutor().build();
    Stage stage = process.getStage("STAGE1").get();
    stage.setStageEntity(StageEntity.createExecution("PIPELINE_NAME", "PROCESS_ID", "STAGE1"));
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName("PIPELINE_NAME");
    processEntity.setProcessId("PROCESS_ID");
    processEntity.setProcessState(ProcessState.PENDING);
    processEntity.setPriority(5);
    process.setProcessEntity(processEntity);
    assertThat(mailService.getProcessExecutionSubject(process))
        .isEqualTo("Pipelite process (PENDING): PIPELINE_NAME/PROCESS_ID");
    assertThat(mailService.getExecutionBody(process, "SUBJECT"))
        .isEqualTo(
            "SUBJECT\n"
                + "\n"
                + "Process:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \"PROCESS_ID\",\n"
                + "  \"pipelineName\" : \"PIPELINE_NAME\",\n"
                + "  \"processState\" : \"PENDING\",\n"
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
                + "  \"stageState\" : \"PENDING\",\n"
                + "  \"executionCount\" : 0\n"
                + "}\n");
  }

  @Test
  public void sendStageExecutionMessage() {
    Process process =
        new ProcessBuilder("PROCESS_ID").execute("STAGE1").withSyncTestExecutor().build();
    Stage stage = process.getStage("STAGE1").get();
    stage.setStageEntity(
        StageEntity.createExecution("PIPELINE_NAME", "PROCESS_ID", stage.getStageName()));
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName("PIPELINE_NAME");
    processEntity.setProcessId("PROCESS_ID");
    processEntity.setProcessState(ProcessState.PENDING);
    processEntity.setPriority(5);
    process.setProcessEntity(processEntity);
    process.setProcessEntity(processEntity);
    assertThat(mailService.getStageExecutionSubject(process, stage))
        .isEqualTo("Pipelite stage (PENDING): PIPELINE_NAME/PROCESS_ID/STAGE1");
    assertThat(mailService.getExecutionBody(process, "SUBJECT"))
        .isEqualTo(
            "SUBJECT\n"
                + "\n"
                + "Process:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \"PROCESS_ID\",\n"
                + "  \"pipelineName\" : \"PIPELINE_NAME\",\n"
                + "  \"processState\" : \"PENDING\",\n"
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
                + "  \"stageState\" : \"PENDING\",\n"
                + "  \"executionCount\" : 0\n"
                + "}\n");
  }

  @Test
  public void sendFailedStageExecutionMessageWithLog() {
    String pipelineName = PipeliteIdCreator.pipelineName();
    String processId = PipeliteIdCreator.processId();

    Process process =
        new ProcessBuilder(processId).execute("STAGE1").withSyncTestExecutor().build();
    Stage stage = process.getStage("STAGE1").get();
    stage.setStageEntity(
        StageEntity.createExecution(pipelineName, processId, stage.getStageName()));
    stage.getStageEntity().setStageState(StageState.ERROR);

    StageLogEntity stageLogEntity = new StageLogEntity();
    stageLogEntity.setPipelineName(pipelineName);
    stageLogEntity.setProcessId(processId);
    stageLogEntity.setStageName("STAGE1");
    stageLogEntity.setStageLog("TEST");
    stageService.saveStageLog(stageLogEntity);
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName(pipelineName);
    processEntity.setProcessId(processId);
    processEntity.setProcessState(ProcessState.PENDING);
    processEntity.setPriority(5);
    process.setProcessEntity(processEntity);
    process.setProcessEntity(processEntity);
    assertThat(mailService.getStageExecutionSubject(process, stage))
        .isEqualTo("Pipelite stage (ERROR): " + pipelineName + "/" + processId + "/STAGE1");
    assertThat(mailService.getExecutionBody(process, "SUBJECT"))
        .isEqualTo(
            "SUBJECT\n"
                + "\n"
                + "Process:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \""
                + processId
                + "\",\n"
                + "  \"pipelineName\" : \""
                + pipelineName
                + "\",\n"
                + "  \"processState\" : \"PENDING\",\n"
                + "  \"executionCount\" : 0,\n"
                + "  \"priority\" : 5\n"
                + "}\n"
                + "\n"
                + "Stages:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \""
                + processId
                + "\",\n"
                + "  \"pipelineName\" : \""
                + pipelineName
                + "\",\n"
                + "  \"stageName\" : \"STAGE1\",\n"
                + "  \"stageState\" : \"ERROR\",\n"
                + "  \"executionCount\" : 0\n"
                + "}\n"
                + "\n"
                + "Error logs:\n"
                + "---------------\n"
                + "\n"
                + "Stage: STAGE1\n"
                + "===============\n"
                + "TEST\n");
  }

  @Test
  public void sendFailedStageExecutionMessageWithEmptyLog() {
    String pipelineName = PipeliteIdCreator.pipelineName();
    String processId = PipeliteIdCreator.processId();

    Process process =
        new ProcessBuilder(processId).execute("STAGE1").withSyncTestExecutor().build();
    Stage stage = process.getStage("STAGE1").get();
    stage.setStageEntity(
        StageEntity.createExecution(pipelineName, processId, stage.getStageName()));
    stage.getStageEntity().setStageState(StageState.ERROR);

    StageLogEntity stageLogEntity = new StageLogEntity();
    stageLogEntity.setPipelineName(pipelineName);
    stageLogEntity.setProcessId(processId);
    stageLogEntity.setStageName("STAGE1");
    stageLogEntity.setStageLog("");
    stageService.saveStageLog(stageLogEntity);
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName(pipelineName);
    processEntity.setProcessId(processId);
    processEntity.setProcessState(ProcessState.PENDING);
    processEntity.setPriority(5);
    process.setProcessEntity(processEntity);
    process.setProcessEntity(processEntity);
    assertThat(mailService.getStageExecutionSubject(process, stage))
        .isEqualTo("Pipelite stage (ERROR): " + pipelineName + "/" + processId + "/STAGE1");
    assertThat(mailService.getExecutionBody(process, "SUBJECT"))
        .isEqualTo(
            "SUBJECT\n"
                + "\n"
                + "Process:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \""
                + processId
                + "\",\n"
                + "  \"pipelineName\" : \""
                + pipelineName
                + "\",\n"
                + "  \"processState\" : \"PENDING\",\n"
                + "  \"executionCount\" : 0,\n"
                + "  \"priority\" : 5\n"
                + "}\n"
                + "\n"
                + "Stages:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \""
                + processId
                + "\",\n"
                + "  \"pipelineName\" : \""
                + pipelineName
                + "\",\n"
                + "  \"stageName\" : \"STAGE1\",\n"
                + "  \"stageState\" : \"ERROR\",\n"
                + "  \"executionCount\" : 0\n"
                + "}\n"
                + "\n"
                + "Error logs:\n"
                + "---------------\n");
  }

  @Test
  public void sendFailedStageExecutionMessageWithNullLog() {
    String pipelineName = PipeliteIdCreator.pipelineName();
    String processId = PipeliteIdCreator.processId();

    Process process =
        new ProcessBuilder(processId).execute("STAGE1").withSyncTestExecutor().build();
    Stage stage = process.getStage("STAGE1").get();
    stage.setStageEntity(
        StageEntity.createExecution(pipelineName, processId, stage.getStageName()));
    stage.getStageEntity().setStageState(StageState.ERROR);

    StageLogEntity stageLogEntity = new StageLogEntity();
    stageLogEntity.setPipelineName(pipelineName);
    stageLogEntity.setProcessId(processId);
    stageLogEntity.setStageName("STAGE1");
    stageLogEntity.setStageLog(null);
    stageService.saveStageLog(stageLogEntity);
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName(pipelineName);
    processEntity.setProcessId(processId);
    processEntity.setProcessState(ProcessState.PENDING);
    processEntity.setPriority(5);
    process.setProcessEntity(processEntity);
    process.setProcessEntity(processEntity);
    assertThat(mailService.getStageExecutionSubject(process, stage))
        .isEqualTo("Pipelite stage (ERROR): " + pipelineName + "/" + processId + "/STAGE1");
    assertThat(mailService.getExecutionBody(process, "SUBJECT"))
        .isEqualTo(
            "SUBJECT\n"
                + "\n"
                + "Process:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \""
                + processId
                + "\",\n"
                + "  \"pipelineName\" : \""
                + pipelineName
                + "\",\n"
                + "  \"processState\" : \"PENDING\",\n"
                + "  \"executionCount\" : 0,\n"
                + "  \"priority\" : 5\n"
                + "}\n"
                + "\n"
                + "Stages:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \""
                + processId
                + "\",\n"
                + "  \"pipelineName\" : \""
                + pipelineName
                + "\",\n"
                + "  \"stageName\" : \"STAGE1\",\n"
                + "  \"stageState\" : \"ERROR\",\n"
                + "  \"executionCount\" : 0\n"
                + "}\n"
                + "\n"
                + "Error logs:\n"
                + "---------------\n");
  }

  @Test
  public void sendFailedStageExecutionMessageWithNoLog() {
    String pipelineName = PipeliteIdCreator.pipelineName();
    String processId = PipeliteIdCreator.processId();

    Process process =
        new ProcessBuilder(processId).execute("STAGE1").withSyncTestExecutor().build();
    Stage stage = process.getStage("STAGE1").get();
    stage.setStageEntity(
        StageEntity.createExecution(pipelineName, processId, stage.getStageName()));
    stage.getStageEntity().setStageState(StageState.ERROR);
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName(pipelineName);
    processEntity.setProcessId(processId);
    processEntity.setProcessState(ProcessState.PENDING);
    processEntity.setPriority(5);
    process.setProcessEntity(processEntity);
    process.setProcessEntity(processEntity);
    assertThat(mailService.getStageExecutionSubject(process, stage))
        .isEqualTo("Pipelite stage (ERROR): " + pipelineName + "/" + processId + "/STAGE1");
    assertThat(mailService.getExecutionBody(process, "SUBJECT"))
        .isEqualTo(
            "SUBJECT\n"
                + "\n"
                + "Process:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \""
                + processId
                + "\",\n"
                + "  \"pipelineName\" : \""
                + pipelineName
                + "\",\n"
                + "  \"processState\" : \"PENDING\",\n"
                + "  \"executionCount\" : 0,\n"
                + "  \"priority\" : 5\n"
                + "}\n"
                + "\n"
                + "Stages:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \""
                + processId
                + "\",\n"
                + "  \"pipelineName\" : \""
                + pipelineName
                + "\",\n"
                + "  \"stageName\" : \"STAGE1\",\n"
                + "  \"stageState\" : \"ERROR\",\n"
                + "  \"executionCount\" : 0\n"
                + "}\n"
                + "\n"
                + "Error logs:\n"
                + "---------------\n");
  }
}

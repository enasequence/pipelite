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
package pipelite.process;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import pipelite.PipeliteIdCreator;
import pipelite.exception.PipeliteProcessStagesException;
import pipelite.process.builder.ProcessBuilder;
import pipelite.runner.stage.DependencyResolver;
import pipelite.stage.Stage;

public class ProcessBuilderTest {

  private static final String PROCESS_ID = PipeliteIdCreator.processId();

  @Test
  public void test() {
    String stageName1 = "STAGE1";
    String stageName2 = "STAGE2";
    String stageName3 = "STAGE3";
    String stageName4 = "STAGE4";
    String stageName5 = "STAGE5";

    Process process =
        new ProcessBuilder(PROCESS_ID)
            .execute(stageName1)
            .withSyncTestExecutor()
            .executeAfterPrevious(stageName2)
            .withSyncTestExecutor()
            .executeAfterPrevious(stageName3)
            .withSyncTestExecutor()
            .executeAfterFirst(stageName4)
            .withSyncTestExecutor()
            .executeAfter(stageName5, Arrays.asList(stageName1, stageName2))
            .withSyncTestExecutor()
            .build();

    Stage stage1 = process.getStage(stageName1).get();
    Stage stage2 = process.getStage(stageName2).get();
    Stage stage3 = process.getStage(stageName3).get();
    Stage stage4 = process.getStage(stageName4).get();
    Stage stage5 = process.getStage(stageName5).get();

    assertThat(process).isNotNull();
    assertThat(process.getProcessId()).isEqualTo(PROCESS_ID);
    assertThat(process.getStages()).hasSize(5);

    assertThat(DependencyResolver.getDependsOnStages(process, stage1)).isEmpty();
    assertThat(DependencyResolver.getDependsOnStages(process, stage2).size()).isOne();
    assertThat(DependencyResolver.getDependsOnStages(process, stage3).size()).isEqualTo(2);
    assertThat(DependencyResolver.getDependsOnStages(process, stage4).size()).isOne();
    assertThat(DependencyResolver.getDependsOnStages(process, stage5).size()).isEqualTo(2);

    assertThat(DependencyResolver.getDependsOnStages(process, stage2).contains(stage1)).isTrue();
    assertThat(DependencyResolver.getDependsOnStages(process, stage3).contains(stage1)).isTrue();
    assertThat(DependencyResolver.getDependsOnStages(process, stage3).contains(stage2)).isTrue();
    assertThat(DependencyResolver.getDependsOnStages(process, stage4).contains(stage1)).isTrue();
    assertThat(DependencyResolver.getDependsOnStages(process, stage5).contains(stage1)).isTrue();
    assertThat(DependencyResolver.getDependsOnStages(process, stage5).contains(stage2)).isTrue();

    assertThat(DependencyResolver.getDependentStages(process, stage1).size()).isEqualTo(4);
    assertThat(DependencyResolver.getDependentStages(process, stage2).size()).isEqualTo(2);
    assertThat(DependencyResolver.getDependentStages(process, stage3)).isEmpty();
    assertThat(DependencyResolver.getDependentStages(process, stage4)).isEmpty();
    assertThat(DependencyResolver.getDependentStages(process, stage5)).isEmpty();

    assertThat(DependencyResolver.getDependentStages(process, stage1).contains(stage2)).isTrue();
    assertThat(DependencyResolver.getDependentStages(process, stage1).contains(stage3)).isTrue();
    assertThat(DependencyResolver.getDependentStages(process, stage1).contains(stage4)).isTrue();
    assertThat(DependencyResolver.getDependentStages(process, stage1).contains(stage5)).isTrue();
    assertThat(DependencyResolver.getDependentStages(process, stage2).contains(stage3)).isTrue();
    assertThat(DependencyResolver.getDependentStages(process, stage2).contains(stage5)).isTrue();
  }

  @Test
  public void testNonStages() {
    assertThatThrownBy(() -> new ProcessBuilder("TEST").build())
        .isInstanceOf(PipeliteProcessStagesException.class)
        .hasMessageContaining("Process has no stages");
  }

  @Test
  public void testNonUniqueStages() {
    String stageName1 = "STAGE1";
    String stageName2 = "STAGE1";
    assertThatThrownBy(
            () ->
                new ProcessBuilder("TEST")
                    .execute(stageName1)
                    .withSyncTestExecutor()
                    .executeAfterPrevious(stageName2)
                    .withSyncTestExecutor()
                    .build())
        .isInstanceOf(PipeliteProcessStagesException.class)
        .hasMessageContaining("Process has non-unique stage names: STAGE1");
  }

  @Test
  public void testNonExistingStages() {
    String stageName1 = "STAGE1";
    String stageName2 = "STAGE2";
    assertThatThrownBy(
            () ->
                new ProcessBuilder("TEST")
                    .execute(stageName1)
                    .withSyncTestExecutor()
                    .executeAfter(stageName2, Arrays.asList(stageName1, "INVALID"))
                    .withSyncTestExecutor()
                    .build())
        .isInstanceOf(PipeliteProcessStagesException.class)
        .hasMessageContaining("Process has non-existing stage dependencies: INVALID");
  }

  @Test
  public void testCyclicStages() {
    String stageName1 = "STAGE1";
    String stageName2 = "STAGE2";
    String stageName3 = "STAGE3";
    assertThatThrownBy(
            () ->
                new ProcessBuilder("TEST")
                    .execute(stageName1)
                    .withSyncTestExecutor()
                    .executeAfter(stageName2, Arrays.asList(stageName3))
                    .withSyncTestExecutor()
                    .executeAfter(stageName3, Arrays.asList(stageName2))
                    .withSyncTestExecutor()
                    .build())
        .isInstanceOf(PipeliteProcessStagesException.class)
        .hasMessageContaining("Process has cyclic stage dependencies: STAGE3,STAGE2");
  }

  @Test
  public void testSelfReferencingStages() {
    String stageName1 = "STAGE1";
    String stageName2 = "STAGE2";
    assertThatThrownBy(
            () ->
                new ProcessBuilder("TEST")
                    .execute(stageName1)
                    .withSyncTestExecutor()
                    .executeAfter(stageName2, Arrays.asList(stageName2))
                    .withSyncTestExecutor()
                    .build())
        .isInstanceOf(PipeliteProcessStagesException.class)
        .hasMessageContaining("Process has self-referencing stage dependencies: STAGE2");
  }
}

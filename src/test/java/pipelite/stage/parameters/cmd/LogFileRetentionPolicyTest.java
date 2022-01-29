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
package pipelite.stage.parameters.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import pipelite.stage.executor.StageExecutorResult;

public class LogFileRetentionPolicyTest {

  @Test
  public void test() {
    assertThat(
            LogFileRetentionPolicy.isDelete(
                LogFileRetentionPolicy.DELETE_ALWAYS, StageExecutorResult.submitted()))
        .isFalse();
    assertThat(
            LogFileRetentionPolicy.isDelete(
                LogFileRetentionPolicy.DELETE_ALWAYS, StageExecutorResult.active()))
        .isFalse();
    assertThat(
            LogFileRetentionPolicy.isDelete(
                LogFileRetentionPolicy.DELETE_ALWAYS, StageExecutorResult.error()))
        .isTrue();
    assertThat(
            LogFileRetentionPolicy.isDelete(
                LogFileRetentionPolicy.DELETE_ALWAYS, StageExecutorResult.success()))
        .isTrue();

    assertThat(
            LogFileRetentionPolicy.isDelete(
                LogFileRetentionPolicy.DELETE_SUCCESS, StageExecutorResult.submitted()))
        .isFalse();
    assertThat(
            LogFileRetentionPolicy.isDelete(
                LogFileRetentionPolicy.DELETE_SUCCESS, StageExecutorResult.active()))
        .isFalse();
    assertThat(
            LogFileRetentionPolicy.isDelete(
                LogFileRetentionPolicy.DELETE_SUCCESS, StageExecutorResult.error()))
        .isFalse();
    assertThat(
            LogFileRetentionPolicy.isDelete(
                LogFileRetentionPolicy.DELETE_SUCCESS, StageExecutorResult.success()))
        .isTrue();

    assertThat(
            LogFileRetentionPolicy.isDelete(
                LogFileRetentionPolicy.DELETE_NEVER, StageExecutorResult.submitted()))
        .isFalse();
    assertThat(
            LogFileRetentionPolicy.isDelete(
                LogFileRetentionPolicy.DELETE_NEVER, StageExecutorResult.active()))
        .isFalse();
    assertThat(
            LogFileRetentionPolicy.isDelete(
                LogFileRetentionPolicy.DELETE_NEVER, StageExecutorResult.error()))
        .isFalse();
    assertThat(
            LogFileRetentionPolicy.isDelete(
                LogFileRetentionPolicy.DELETE_NEVER, StageExecutorResult.success()))
        .isFalse();
  }
}

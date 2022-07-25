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
package pipelite.runner.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ScheduleRunnerNextProcessIdTest {

  @Test
  public void nextProcessId() {
    assertThat(ScheduleRunner.nextProcessId(null)).isEqualTo("1");
    assertThat(ScheduleRunner.nextProcessId("0")).isEqualTo("1");
    assertThat(ScheduleRunner.nextProcessId("1")).isEqualTo("2");
    assertThat(ScheduleRunner.nextProcessId("9")).isEqualTo("10");
    assertThat(ScheduleRunner.nextProcessId("10")).isEqualTo("11");
    assertThat(ScheduleRunner.nextProcessId("29")).isEqualTo("30");
    assertThat(ScheduleRunner.nextProcessId("134232")).isEqualTo("134233");
  }
}

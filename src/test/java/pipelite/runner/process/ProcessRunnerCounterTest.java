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
package pipelite.runner.process;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

public class ProcessRunnerCounterTest {

  @Test
  public void test() {
    ProcessRunnerCounter counter = new ProcessRunnerCounter();

    Duration since = Duration.ofDays(1);

    assertThat(counter.getCount(since)).isZero();

    counter.increment(1);
    assertThat(counter.getCount(since)).isEqualTo(1);

    counter.increment(2);
    assertThat(counter.getCount(since)).isEqualTo(3);

    counter.increment(3);
    assertThat(counter.getCount(since)).isEqualTo(6);

    counter.purge(since);

    assertThat(counter.getCount(since)).isEqualTo(6);

    counter.purge(Duration.ofDays(0));

    assertThat(counter.getCount(since)).isEqualTo(0);
  }
}

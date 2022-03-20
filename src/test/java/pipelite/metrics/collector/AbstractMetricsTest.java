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
package pipelite.metrics.collector;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class AbstractMetricsTest {

  private static class TestMetrics extends AbstractMetrics {
    public TestMetrics() {
      super(".prefix..prefix.prefix");
    }
  }

  @Test
  public void test() {
    TestMetrics testMetrics = new TestMetrics();
    assertThat(testMetrics.name("..hello..world..")).isEqualTo("prefix.prefix.prefix.hello.world");
    assertThat(testMetrics.name("hello.world")).isEqualTo("prefix.prefix.prefix.hello.world");
  }
}

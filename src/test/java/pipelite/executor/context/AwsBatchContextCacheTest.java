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
package pipelite.executor.context;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pipelite.executor.AwsBatchExecutor;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.parameters.AwsBatchExecutorParameters;

@Tag("aws-test")
public class AwsBatchContextCacheTest {

  private AwsBatchExecutor executor(String region) {
    AwsBatchExecutorParameters params = AwsBatchExecutorParameters.builder().region(region).build();
    AwsBatchExecutor executor = StageExecutor.createAwsBatchExecutor();
    executor.setExecutorParams(params);
    return executor;
  }

  @Test
  @Disabled
  public void test() {
    AwsBatchContextCache cache = new AwsBatchContextCache();
    assertThat(cache.getSharedContext(executor("region1")))
        .isSameAs(cache.getSharedContext(executor("region1")));
    assertThat(cache.getSharedContext(executor("region2")))
        .isSameAs(cache.getSharedContext(executor("region2")));
    assertThat(cache.getSharedContext(executor("region1")))
        .isNotSameAs(cache.getSharedContext(executor("region2")));
    assertThat(cache.getSharedContext(executor(null)))
        .isSameAs(cache.getSharedContext(executor(null)));
    assertThat(cache.getSharedContext(executor(null)))
        .isNotSameAs(cache.getSharedContext(executor("region1")));
  }
}

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
package pipelite.executor.describe.context;

import com.amazonaws.services.batch.AWSBatch;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import pipelite.executor.AwsBatchExecutor;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public final class AwsBatchExecutorContext extends DefaultExecutorContext<DefaultRequestContext> {

  public AwsBatchExecutorContext(AWSBatch awsBatch) {
    super("AWSBatch", requests -> AwsBatchExecutor.pollJobs(awsBatch, requests), null);
  }
}

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

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import lombok.extern.flogger.Flogger;
import pipelite.executor.AwsBatchExecutor;

/** Cache of contexts for operations that can be shared between AWSBatch executors. */
@Flogger
public class AwsBatchContextCache
    extends SharedExecutorContextCache<AwsBatchExecutor, AwsBatchContextId, AwsBatchContext> {

  @Override
  protected AwsBatchContextId createSharedContextId(AwsBatchExecutor executor) {
    return new AwsBatchContextId(executor.getExecutorParams().getRegion());
  }

  @Override
  protected AwsBatchContext createSharedContext(AwsBatchExecutor executor) {
    AWSBatchClientBuilder awsBuilder = AWSBatchClientBuilder.standard();
    String region = executor.getExecutorParams().getRegion();
    if (region != null) {
      awsBuilder.setRegion(region);
    }
    AWSBatch awsBatch = awsBuilder.build();
    return new AwsBatchContext(awsBatch);
  }
}

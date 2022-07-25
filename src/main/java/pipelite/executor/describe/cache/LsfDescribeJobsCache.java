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
package pipelite.executor.describe.cache;

import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.AbstractLsfExecutor;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.describe.context.LsfCacheContext;
import pipelite.executor.describe.context.LsfExecutorContext;
import pipelite.executor.describe.context.LsfRequestContext;
import pipelite.service.InternalErrorService;
import pipelite.stage.parameters.AbstractLsfExecutorParameters;

@Component
@Flogger
public class LsfDescribeJobsCache
    extends DescribeJobsCache<
        LsfRequestContext,
        LsfExecutorContext,
        LsfCacheContext,
        AbstractLsfExecutor<AbstractLsfExecutorParameters>> {

  public LsfDescribeJobsCache(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired InternalErrorService internalErrorService) {
    super(serviceConfiguration, internalErrorService, 100);
  }

  @Override
  public LsfExecutorContext getExecutorContext(
      AbstractLsfExecutor<AbstractLsfExecutorParameters> executor) {
    return new LsfExecutorContext(CmdRunner.create(executor.getExecutorParams()));
  }

  @Override
  public LsfCacheContext getCacheContext(
      AbstractLsfExecutor<AbstractLsfExecutorParameters> executor) {
    return new LsfCacheContext(executor.getExecutorParams().getHost());
  }
}

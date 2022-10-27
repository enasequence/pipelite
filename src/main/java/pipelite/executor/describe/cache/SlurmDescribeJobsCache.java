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
import pipelite.executor.AbstractSlurmExecutor;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.describe.context.cache.SlurmCacheContext;
import pipelite.executor.describe.context.executor.SlurmExecutorContext;
import pipelite.executor.describe.context.request.SlurmRequestContext;
import pipelite.executor.describe.poll.SlurmExecutorPollJobs;
import pipelite.executor.describe.recover.SlurmExecutorRecoverJob;
import pipelite.service.InternalErrorService;
import pipelite.stage.parameters.AbstractSlurmExecutorParameters;

@Component
@Flogger
public class SlurmDescribeJobsCache
    extends DescribeJobsCache<
        SlurmRequestContext,
        SlurmExecutorContext,
        SlurmCacheContext,
        AbstractSlurmExecutor<AbstractSlurmExecutorParameters>> {
  private final SlurmExecutorPollJobs pollJobs;
  private final SlurmExecutorRecoverJob recoverJob;

  public SlurmDescribeJobsCache(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired InternalErrorService internalErrorService,
      @Autowired SlurmExecutorPollJobs pollJobs,
      @Autowired SlurmExecutorRecoverJob recoverJob) {
    super(serviceConfiguration, internalErrorService, 100);
    this.pollJobs = pollJobs;
    this.recoverJob = recoverJob;
  }

  @Override
  public SlurmExecutorContext getExecutorContext(
      AbstractSlurmExecutor<AbstractSlurmExecutorParameters> executor) {
    return new SlurmExecutorContext(
        CmdRunner.create(executor.getExecutorParams()), pollJobs, recoverJob);
  }

  @Override
  public SlurmCacheContext getCacheContext(
      AbstractSlurmExecutor<AbstractSlurmExecutorParameters> executor) {
    return new SlurmCacheContext(executor.getExecutorParams().getHost());
  }
}

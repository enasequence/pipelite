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
import pipelite.executor.KubernetesExecutor;
import pipelite.executor.describe.context.cache.KubernetesCacheContext;
import pipelite.executor.describe.context.executor.KubernetesExecutorContext;
import pipelite.executor.describe.context.request.DefaultRequestContext;
import pipelite.executor.describe.poll.KubernetesExecutorPollJobs;
import pipelite.service.InternalErrorService;
import pipelite.stage.parameters.KubernetesExecutorParameters;

@Component
@Flogger
public class KubernetesDescribeJobsCache
    extends DescribeJobsCache<
        DefaultRequestContext,
        KubernetesExecutorContext,
        KubernetesCacheContext,
        KubernetesExecutor> {

  private final KubernetesExecutorPollJobs pollJobs;

  public KubernetesDescribeJobsCache(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired InternalErrorService internalErrorService,
      @Autowired KubernetesExecutorPollJobs pollJobs) {
    super(serviceConfiguration, internalErrorService, 100);
    this.pollJobs = pollJobs;
  }

  @Override
  public KubernetesExecutorContext getExecutorContext(KubernetesExecutor executor) {
    KubernetesExecutorParameters params = executor.getExecutorParams();
    return new KubernetesExecutorContext(
        KubernetesExecutor.client(params.getContext()), params.getNamespace(), pollJobs);
  }

  @Override
  public KubernetesCacheContext getCacheContext(KubernetesExecutor executor) {
    KubernetesExecutorParameters params = executor.getExecutorParams();
    return new KubernetesCacheContext(params.getContext(), params.getNamespace());
  }
}

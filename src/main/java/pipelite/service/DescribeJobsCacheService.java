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
package pipelite.service;

import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.describe.cache.AwsBatchDescribeJobsCache;
import pipelite.executor.describe.cache.KubernetesDescribeJobsCache;
import pipelite.executor.describe.cache.LsfDescribeJobsCache;

@Service
@Flogger
public class DescribeJobsCacheService {

  private final LsfDescribeJobsCache lsfDescribeJobsCache;
  private final AwsBatchDescribeJobsCache awsBatchDescribeJobsCache;
  private final KubernetesDescribeJobsCache kubernetesDescribeJobsCache;

  public DescribeJobsCacheService(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired InternalErrorService internalErrorService) {

    this.lsfDescribeJobsCache =
        new LsfDescribeJobsCache(serviceConfiguration, internalErrorService);
    this.awsBatchDescribeJobsCache =
        new AwsBatchDescribeJobsCache(serviceConfiguration, internalErrorService);
    this.kubernetesDescribeJobsCache =
        new KubernetesDescribeJobsCache(serviceConfiguration, internalErrorService);
  }

  public LsfDescribeJobsCache lsfDescribeJobsCache() {
    return lsfDescribeJobsCache;
  }

  public AwsBatchDescribeJobsCache awsBatchDescribeJobsCache() {
    return awsBatchDescribeJobsCache;
  }

  public KubernetesDescribeJobsCache kubernetesDescribeJobsCache() {
    return kubernetesDescribeJobsCache;
  }
}

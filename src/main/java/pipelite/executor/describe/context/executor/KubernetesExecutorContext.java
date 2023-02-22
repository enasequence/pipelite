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
package pipelite.executor.describe.context.executor;

import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.NonFinal;
import pipelite.executor.describe.DescribeJobsRequests;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.request.DefaultRequestContext;
import pipelite.executor.describe.poll.KubernetesExecutorPollJobs;

@Value
@Accessors(fluent = true)
@NonFinal
@EqualsAndHashCode(callSuper = true)
public class KubernetesExecutorContext extends DefaultExecutorContext<DefaultRequestContext> {

  private final KubernetesClient client;
  private final String namespace;
  private final KubernetesExecutorPollJobs pollJobs;

  public KubernetesExecutorContext(
      KubernetesClient client, String namespace, KubernetesExecutorPollJobs pollJobs) {
    super("Kubernetes");
    this.client = client;
    this.namespace = namespace;
    this.pollJobs = pollJobs;
  }

  @Override
  public DescribeJobsResults<DefaultRequestContext> pollJobs(
      DescribeJobsRequests<DefaultRequestContext> requests) {
    return pollJobs.pollJobs(this, requests);
  }
}

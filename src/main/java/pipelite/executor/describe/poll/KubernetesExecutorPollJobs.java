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
package pipelite.executor.describe.poll;

import static pipelite.executor.KubernetesExecutor.lastContainerToFinish;
import static pipelite.executor.KubernetesExecutor.lastPodToStart;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import lombok.extern.flogger.Flogger;
import org.springframework.stereotype.Component;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.DescribeJobsRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.executor.KubernetesExecutorContext;
import pipelite.executor.describe.context.request.DefaultRequestContext;
import pipelite.retryable.RetryableExternalAction;

@Component
@Flogger
public class KubernetesExecutorPollJobs
    implements PollJobs<KubernetesExecutorContext, DefaultRequestContext> {

  @Override
  public DescribeJobsResults<DefaultRequestContext> pollJobs(
      KubernetesExecutorContext executorContext,
      DescribeJobsRequests<DefaultRequestContext> requests) {
    DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();
    Set<String> jobIds = new HashSet<>();
    try {
      KubernetesClient client = executorContext.client();
      String namespace = executorContext.namespace();
      JobList jobList =
          RetryableExternalAction.execute(
              () -> client.batch().v1().jobs().inNamespace(namespace).list());
      for (Job job : jobList.getItems()) {
        String jobId = job.getMetadata().getName();
        jobIds.add(jobId);
        DefaultRequestContext request = requests.get(jobId);
        if (request != null) {
          results.add(
              extractJobResult(
                  request, job.getStatus(), () -> extratExitCode(namespace, jobId, client)));
        }
      }
      for (String jobId : requests.jobIds()) {
        if (!jobIds.contains(jobId)) {
          // The job has been lost.
          results.add(DescribeJobsResult.builder(requests, jobId).lostError().build());
        }
      }
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }
    return results;
  }

  public static DescribeJobsResult<DefaultRequestContext> extractJobResult(
      DefaultRequestContext request, JobStatus jobStatus, Supplier<Integer> exitCodeCallback) {
    // https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1JobStatus.md
    // Only one pod per job.

    DescribeJobsResult.Builder<DefaultRequestContext> result = DescribeJobsResult.builder(request);

    // The completion time is only set when the job finishes successfully.
    if (jobStatus.getCompletionTime() != null) {
      result.success();
    }

    if (!result.isCompleted()) {
      // When a Job fails, one of the conditions will have type 'Failed' and status true.
      for (JobCondition jobCondition : jobStatus.getConditions()) {
        if ("Failed".equalsIgnoreCase(jobCondition.getType())
            && "true".equalsIgnoreCase(jobCondition.getStatus())) {
          result.executionError(exitCodeCallback.get());
        }
      }
    }

    if (!result.isCompleted()) {
      // When a Job completes, one of the conditions will have type 'Complete' and status true.
      for (JobCondition jobCondition : jobStatus.getConditions()) {
        if ("Complete".equalsIgnoreCase(jobCondition.getType())
            && "true".equalsIgnoreCase(jobCondition.getStatus())) {
          result.success();
        }
      }
    }

    if (!result.isCompleted()) {
      result.active();
    }

    return result.build();
  }

  private static int extratExitCode(String namespace, String jobId, KubernetesClient client) {
    try {
      List<Pod> pods =
          client.pods().inNamespace(namespace).withLabel("job-name", jobId).list().getItems();
      Pod pod = lastPodToStart(pods);
      if (pod == null || pod.getStatus() == null) {
        throw new PipeliteException(
            "Could not get pod status for completed Kubernetes job: " + jobId);
      }
      List<ContainerStatus> containerStatuses = pod.getStatus().getContainerStatuses();
      ContainerStatus containerStatus = lastContainerToFinish(containerStatuses);

      if (containerStatus == null
          || containerStatus.getState() == null
          || containerStatus.getState().getTerminated() == null
          || containerStatus.getState().getTerminated().getExitCode() == null) {
        throw new PipeliteException(
            "Could not get container status for completed Kubernetes job: " + jobId);
      }
      return containerStatus.getState().getTerminated().getExitCode();
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }
  }
}

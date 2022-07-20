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
package pipelite.executor;

import com.google.common.flogger.FluentLogger;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.DefaultRequestContext;
import pipelite.executor.describe.context.KubernetesExecutorContext;
import pipelite.log.LogKey;
import pipelite.retryable.RetryableExternalAction;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.KubernetesExecutorParameters;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

// https://github.com/fabric8io
// https://github.com/fabric8io/kubernetes-client/blob/master/README.md
// https://github.com/fabric8io/kubernetes-client/blob/master/kubernetes-examples/src/main/java/io/fabric8/kubernetes/examples/JobExample.java

/** Executes a Job using Kubernetes. */
@Flogger
@Getter
@Setter
public class KubernetesExecutor
    extends AsyncExecutor<
        KubernetesExecutorParameters, DefaultRequestContext, KubernetesExecutorContext>
    implements JsonSerializableExecutor {

  private static final int KUBERNETES_TTL_SECONDS_AFTER_FINISHED =
      (int) java.time.Duration.ofHours(8).getSeconds();

  /**
   * The image. Set during executor creation. Serialize in database to continue execution after
   * service restart.
   */
  private String image;

  /**
   * The image arguments. Set during executor creation. Serialize in database to continue execution
   * after service restart.
   */
  private List<String> imageArgs;

  /**
   * The Kubernetes context. Set during submit. Serialize in database to continue execution after
   * service restart.
   */
  private String context;

  /**
   * The Kubernetes namespace. Set during submit. Serialize in database to continue execution after
   * service restart.
   */
  private String namespace;

  @Override
  protected DefaultRequestContext prepareRequestContext() {
    return new DefaultRequestContext(getJobId());
  }

  @Override
  protected DescribeJobs<DefaultRequestContext, KubernetesExecutorContext> prepareDescribeJobs(
      PipeliteServices pipeliteServices) {
    return pipeliteServices.jobs().kubernetes().getDescribeJobs(this);
  }

  @Override
  protected SubmitJobResult submitJob() {
    KubernetesExecutorParameters executorParams = getExecutorParams();
    context = executorParams.getContext();
    namespace = executorParams.getNamespace() != null ? executorParams.getNamespace() : "default";
    String jobId = kubernetesJobName();
    logContext(log.atFine(), getRequest()).log("Submitting Kubernetes job " + jobId);

    // Map<String, String> labelMap = new HashMap<>();
    // labelMap.put(..., ...);

    Map<String, Quantity> requestsMap = new HashMap<>();
    requestsMap.put("cpu", executorParams.getMemory());
    requestsMap.put("memory", executorParams.getCpu());

    Map<String, Quantity> limitsMap = new HashMap<>();
    limitsMap.put("cpu", executorParams.getMemoryLimit());
    limitsMap.put("memory", executorParams.getCpuLimit());

    try (KubernetesClient client = client(context)) {
      Job job =
          new JobBuilder()
              .withApiVersion("batch/v1")
              .withNewMetadata()
              .withName(jobId)
              // .withLabels(...)
              // .withAnnotations(...)
              .endMetadata()
              .withNewSpec()
              .withBackoffLimit(1)
              .withTtlSecondsAfterFinished(KUBERNETES_TTL_SECONDS_AFTER_FINISHED)
              .withNewTemplate()
              .withNewSpec()
              .addNewContainer()
              .withName(jobId)
              .withImage(image)
              .withArgs(imageArgs)
              .withNewResources()
              .withRequests(requestsMap)
              .withLimits(limitsMap)
              .endResources()
              .endContainer()
              .withRestartPolicy("Never")
              .endSpec()
              .endTemplate()
              // .withBackoffLimit()
              .endSpec()
              .build();

      RetryableExternalAction.execute(
          () -> client.batch().v1().jobs().inNamespace(namespace).create(job));

      logContext(log.atInfo(), getRequest()).log("Submitted Kubernetes job " + jobId);
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }
    return new SubmitJobResult(jobId);
  }

  @Override
  protected void endJob() {
    StageExecutorResult result = this.getJobCompletedResult();
    try (KubernetesClient client = client(context)) {
      String jobId = getJobId();
      if (isSaveLogFile(result)) {
        List<Pod> pods =
            client.pods().inNamespace(namespace).withLabel("job-name", jobId).list().getItems();
        Pod pod = lastPodToStart(pods);
        String log =
            client
                .pods()
                .inNamespace(namespace)
                .withName(pod.getMetadata().getName())
                .tailingLines(getExecutorParams().getLogLines())
                .getLog();
        result.setStageLog(log);
      }
      terminateJob(client);
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }
  }

  @Override
  public void terminateJob() {
    String jobId = getJobId();
    if (jobId == null) {
      return;
    }
    try (KubernetesClient client = client(context)) {
      terminateJob(client);
      getDescribeJobs().removeRequest(getRequestContext());
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }
  }

  public static DescribeJobsResults<DefaultRequestContext> pollJobs(
      KubernetesClient client,
      String namespace,
      DescribeJobsPollRequests<DefaultRequestContext> requests) {
    DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();
    Set<String> kubernetesJobIds = new HashSet<>();
    try {
      JobList jobList =
          RetryableExternalAction.execute(
              () -> client.batch().v1().jobs().inNamespace(namespace).list());
      for (Job job : jobList.getItems()) {
        String kubernetesJobId = job.getMetadata().getName();
        kubernetesJobIds.add(kubernetesJobId);
        if (requests.requests.get(kubernetesJobId) != null) {
          results.add(
              new DescribeJobsResult<>(
                  requests,
                  kubernetesJobId,
                  extractJobResult(namespace, kubernetesJobId, client, job.getStatus())));
        }
      }
      for (String jobId : requests.jobIds) {
        if (!kubernetesJobIds.contains(jobId)) {
          // Consider jobs that can't be found as failed.
          results.add(new DescribeJobsResult<>(requests, jobId, StageExecutorResult.error()));
        }
      }
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }
    return results;
  }

  static StageExecutorResult extractJobResult(
      String namespace,
      String jobId,
      KubernetesClient client,
      io.fabric8.kubernetes.api.model.batch.v1.JobStatus jobStatus) {
    // Only one pod per job.

    StageExecutorResult result = extractJobResultFromStatus(jobStatus);
    if (result.isActive()) {
      return result;
    }

    // Get exit code.
    Integer exitCode;
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
      exitCode = containerStatus.getState().getTerminated().getExitCode();
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }

    result.addAttribute(
        StageExecutorResultAttribute.EXIT_CODE, exitCode != null ? String.valueOf(exitCode) : "");
    result.addAttribute(StageExecutorResultAttribute.JOB_ID, jobId);
    return result;
  }

  static StageExecutorResult extractJobResultFromStatus(
      io.fabric8.kubernetes.api.model.batch.v1.JobStatus jobStatus) {
    // https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1JobStatus.md

    // The completion time is only set when the job finishes successfully.
    if (jobStatus.getCompletionTime() != null) {
      return StageExecutorResult.success();
    }

    // When a Job fails, one of the conditions will have type 'Failed' and status true.
    for (JobCondition jobCondition : jobStatus.getConditions()) {
      if ("Failed".equalsIgnoreCase(jobCondition.getType())
          && "true".equalsIgnoreCase(jobCondition.getStatus())) {
        return StageExecutorResult.error();
      }
    }

    // When a Job completes, one of the conditions will have type 'Complete' and status true.
    for (JobCondition jobCondition : jobStatus.getConditions()) {
      if ("Complete".equalsIgnoreCase(jobCondition.getType())
          && "true".equalsIgnoreCase(jobCondition.getStatus())) {
        return StageExecutorResult.success();
      }
    }

    return StageExecutorResult.active();
  }

  public static KubernetesClient client(String context) {
    return context != null
        ? new DefaultKubernetesClient(Config.autoConfigure(context))
        : new DefaultKubernetesClient();
  }

  static String kubernetesJobName() {
    // Job name requirements
    // =====================
    // must contain no more than 253 characters
    // must contain only lowercase alphanumeric characters, '-' or '.'
    // must begin and end with an alphanumeric character [a-z0-9A-Z]

    // Job name is added as a label to the pods.

    // Label requirements
    // ==================
    // must contain no more than 63 characters
    // must contain only alphanumeric characters, '-' or '_'.
    // must begin and end with an alphanumeric character [a-z0-9A-Z]

    return "pipelite-" + UUID.randomUUID();
  }

  private void terminateJob(KubernetesClient client) {
    String jobId = getJobId();
    if (jobId == null) {
      return;
    }
    log.atFine().log("Terminating Kubernetes job " + jobId);
    ScalableResource<Job> job = client.batch().v1().jobs().inNamespace(namespace).withName(jobId);
    RetryableExternalAction.execute(() -> job.delete());
  }

  private static Pod lastPodToStart(List<Pod> pods) {
    return pods.stream()
        .max(Comparator.comparing(p -> toLocalDateTime(p.getStatus().getStartTime())))
        .orElse(null);
  }

  private static ContainerStatus lastContainerToFinish(List<ContainerStatus> containerStatuses) {
    return containerStatuses.stream()
        .max(
            Comparator.comparing(
                s -> toLocalDateTime(s.getState().getTerminated().getFinishedAt())))
        .orElse(null);
  }

  static LocalDateTime toLocalDateTime(String str) {
    // Example: 2022-01-08T21:56:16Z
    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    return LocalDateTime.parse(str, formatter);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, StageExecutorRequest request) {
    return log.with(LogKey.PIPELINE_NAME, request.getPipelineName())
        .with(LogKey.PROCESS_ID, request.getProcessId())
        .with(LogKey.STAGE_NAME, request.getStage().getStageName());
  }
}

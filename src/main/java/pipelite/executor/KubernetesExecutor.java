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
import io.fabric8.kubernetes.api.model.batch.v1.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.cache.KubernetesDescribeJobsCache;
import pipelite.executor.task.RetryTask;
import pipelite.log.LogKey;
import pipelite.service.StageService;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.KubernetesExecutorParameters;

// https://github.com/fabric8io
// https://github.com/fabric8io/kubernetes-client/blob/master/README.md
// https://github.com/fabric8io/kubernetes-client/blob/master/kubernetes-examples/src/main/java/io/fabric8/kubernetes/examples/JobExample.java

/** Executes a Job using Kubernetes. */
@Flogger
@Getter
@Setter
public class KubernetesExecutor
    extends AbstractAsyncExecutor<KubernetesExecutorParameters, KubernetesDescribeJobsCache>
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

  // Json deserialization requires a no argument constructor.
  public KubernetesExecutor() {}

  @Override
  protected KubernetesDescribeJobsCache initDescribeJobsCache(StageService stageService) {
    return stageService.getKubernetesDescribeJobsCache();
  }

  private DescribeJobs<String, KubernetesDescribeJobsCache.ExecutorContext> describeJobs() {
    return getDescribeJobsCache().getDescribeJobs(this);
  }

  @Override
  protected void prepareAsyncSubmit(StageExecutorRequest request) {
    // Reset to allow execution retry.
    context = null;
    namespace = null;
  }

  @Override
  protected StageExecutorResult submit(StageExecutorRequest request) {
    KubernetesExecutorParameters executorParams = getExecutorParams();
    context = executorParams.getContext();
    namespace = executorParams.getNamespace() != null ? executorParams.getNamespace() : "default";
    setJobId(createJobId());
    String jobId = getJobId();
    logContext(log.atFine(), request).log("Submitting Kubernetes job " + jobId);

    // Map<String, String> labelMap = new HashMap<>();
    // labelMap.put(..., ...);

    Map<String, Quantity> requestsMap = new HashMap<>();
    requestsMap.put("cpu", executorParams.getMemory());
    requestsMap.put("memory", executorParams.getCpu());

    Map<String, Quantity> limitsMap = new HashMap<>();
    limitsMap.put("cpu", executorParams.getMemoryLimit());
    limitsMap.put("memory", executorParams.getCpuLimit());

    try (KubernetesClient client = kubernetesClient(context)) {
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

      RetryTask.DEFAULT.execute(r -> client.batch().v1().jobs().inNamespace(namespace).create(job));

      logContext(log.atInfo(), request).log("Submitted Kubernetes job " + jobId);
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }

    return StageExecutorResult.submitted();
  }

  @Override
  protected StageExecutorResult poll(StageExecutorRequest request) {
    String jobId = getJobId();
    logContext(log.atFine(), request).log("Polling Kubernetes job result " + jobId);
    StageExecutorResult result =
        describeJobs().getResult(jobId, getExecutorParams().getPermanentErrors());
    if (result.isActive()) {
      return StageExecutorResult.active();
    }

    try (KubernetesClient client = kubernetesClient(context)) {
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
    return result;
  }

  @Override
  public void terminate() {
    String jobId = getJobId();
    if (jobId == null) {
      return;
    }
    try (KubernetesClient client = kubernetesClient(context)) {
      terminateJob(client);
      describeJobs().removeRequest(jobId);
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }
  }

  public static Map<String, StageExecutorResult> describeJobs(
      List<String> requests, KubernetesDescribeJobsCache.ExecutorContext executorContext) {
    log.atFine().log("Describing Kubernetes job results");

    Map<String, StageExecutorResult> results = new HashMap<>();
    Set<String> jobIds = new HashSet();
    String namespace = executorContext.getNamespace();
    try {
      KubernetesClient client = executorContext.getKubernetesClient();
      JobList jobList =
          RetryTask.DEFAULT.execute(r -> client.batch().v1().jobs().inNamespace(namespace).list());
      for (Job job : jobList.getItems()) {
        String jobId = job.getMetadata().getName();
        jobIds.add(jobId);
        results.put(jobId, describeJobsResult(namespace, jobId, client, job.getStatus()));
      }
      for (String jobId : requests) {
        if (!jobIds.contains(jobId)) {
          // Consider jobs that can't be found as failed.
          results.put(jobId, StageExecutorResult.error());
        }
      }
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }
    return results;
  }

  static StageExecutorResult describeJobsResult(
      String namespace, String jobId, KubernetesClient client, JobStatus jobStatus) {
    // Only one pod per job.

    StageExecutorResult result = describeJobsResultFromStatus(jobStatus);
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

  static StageExecutorResult describeJobsResultFromStatus(JobStatus jobStatus) {
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

  public static KubernetesClient kubernetesClient(String context) {
    return context != null
        ? new DefaultKubernetesClient(Config.autoConfigure(context))
        : new DefaultKubernetesClient();
  }

  static String createJobId() {
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
    RetryTask.DEFAULT.execute(r -> job.delete());
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

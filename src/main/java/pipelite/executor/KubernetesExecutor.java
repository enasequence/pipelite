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
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
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
import pipelite.executor.describe.cache.KubernetesDescribeJobsCache;
import pipelite.executor.task.RetryTask;
import pipelite.log.LogKey;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.KubernetesExecutorParameters;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.lang.Math.max;

// https://github.com/fabric8io
// https://github.com/fabric8io/kubernetes-client/blob/master/README.md
// https://github.com/fabric8io/kubernetes-client/blob/master/kubernetes-examples/src/main/java/io/fabric8/kubernetes/examples/JobExample.java

/** Executes a Job using Kubernetes. */
@Flogger
@Getter
@Setter
public class KubernetesExecutor extends AbstractAsyncExecutor<KubernetesExecutorParameters>
    implements JsonSerializableExecutor {

  private static final int KUBERNETES_TTL_SECONDS_AFTER_FINISHED =
      (int) java.time.Duration.ofHours(8).getSeconds();

  /** The image. Set during executor creation. */
  private String image;

  /** The image arguments. Set during executor creation. */
  private List<String> imageArgs;

  /** The Kubernetes context. Set during submit. */
  private String context;

  /** The Kubernetes namespace. Set during submit. */
  private String namespace;

  /** The Kubernetes job name. Set during submit. */
  private String jobName;

  // Json deserialization requires a no argument constructor.
  public KubernetesExecutor() {}

  private DescribeJobs<String, KubernetesDescribeJobsCache.ExecutorContext> describeJobs() {
    return describeJobsCache.kubernetes.getDescribeJobs(this);
  }

  @Override
  protected void prepareSubmit(StageExecutorRequest request) {
    context = null;
    namespace = null;
    jobName = null;
  }

  @Override
  protected StageExecutorResult submit(StageExecutorRequest request) {
    KubernetesExecutorParameters executorParams = getExecutorParams();
    context = executorParams.getContext();
    namespace = kubernetesNamespace(executorParams);
    jobName = kubernetesJobName();

    logContext(log.atFine(), request).log("Submitting Kubernetes job " + jobName);

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
              .withName(jobName)
              // .withLabels(...)
              // .withAnnotations(...)
              .endMetadata()
              .withNewSpec()
              .withBackoffLimit(1)
              .withTtlSecondsAfterFinished(KUBERNETES_TTL_SECONDS_AFTER_FINISHED)
              .withNewTemplate()
              .withNewSpec()
              .addNewContainer()
              .withName(jobName)
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

      // TODO: make fine
      logContext(log.atInfo(), request).log("Submitted Kubernetes job " + jobName);

    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }

    return StageExecutorResult.submitted();
  }

  @Override
  protected StageExecutorResult poll(StageExecutorRequest request) {
    logContext(log.atFine(), request).log("Checking Kubernetes job result.");
    StageExecutorResult result =
        describeJobs().getResult(jobName, getExecutorParams().getPermanentErrors());
    if (result.isActive()) {
      return StageExecutorResult.active();
    }

    log.atFine().log(
        "Completed Kubernetes job in poll "
            + jobName
            + " with state "
            + result.getExecutorState().name());

    try (KubernetesClient client = kubernetesClient(context)) {
      // TODO: user LogReader
      if (getExecutorParams().isSaveLog()) {
        String log = client.batch().v1().jobs().inNamespace(namespace).withName(jobName).getLog();
        log = log.substring(max(0, log.length() - getExecutorParams().getLogBytes()));
        result.setStageLog(log);
      }

      log.atFine().log("Terminating Kubernetes job in poll " + jobName);
      kubernetesTerminateJob(client);
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }
    return result;
  }

  @Override
  public void terminate() {
    if (jobName == null) {
      return;
    }
    try (KubernetesClient client = kubernetesClient(context)) {
      log.atFine().log("Terminating Kubernetes job in terminate " + jobName);
      kubernetesTerminateJob(client);
      describeJobs().removeRequest(jobName);
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }
  }

  public static Map<String, StageExecutorResult> describeJobs(
      List<String> requests, KubernetesDescribeJobsCache.ExecutorContext executorContext) {

    Map<String, StageExecutorResult> results = new HashMap<>();
    Set<String> jobNames = new HashSet();
    String namespace = executorContext.getNamespace();
    try {
      KubernetesClient client = executorContext.getKubernetesClient();
      JobList jobList =
          RetryTask.DEFAULT.execute(r -> client.batch().v1().jobs().inNamespace(namespace).list());
      for (Job job : jobList.getItems()) {
        String jobName = job.getMetadata().getName();
        jobNames.add(jobName);
        results.put(jobName, describeJobsResult(namespace, jobName, client, job.getStatus()));
      }
      for (String jobName : requests) {
        if (!jobNames.contains(jobName)) {
          // Consider jobs that can't be found as failed.
          results.put(jobName, StageExecutorResult.error());
        }
      }
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }
    return results;
  }

  static StageExecutorResult describeJobsResult(
      String namespace, String jobName, KubernetesClient client, JobStatus jobStatus) {
    // Only one pod per job.

    StageExecutorResult result = describeJobsResultFromStatus(jobStatus);
    if (result.isActive()) {
      return result;
    }

    // Get exit code.
    Integer exitCode = -1;
    LocalDateTime lastFinishedAt = null;
    try {
      PodList podList = client.pods().inNamespace(namespace).withLabel("job-name", jobName).list();
      for (Pod pod : podList.getItems()) {
        for (ContainerStatus containerStatus : pod.getStatus().getContainerStatuses()) {
          // Example: 2022-01-08T21:56:16Z
          String finishedAtStr = containerStatus.getState().getTerminated().getFinishedAt();
          DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
          LocalDateTime finishedAt = LocalDateTime.parse(finishedAtStr, formatter);
          if (lastFinishedAt == null || lastFinishedAt.isBefore(finishedAt)) {
            lastFinishedAt = finishedAt;
            exitCode = max(exitCode, containerStatus.getState().getTerminated().getExitCode());
          }
        }
      }
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }

    result.addAttribute(
        StageExecutorResultAttribute.EXIT_CODE, exitCode > -1 ? String.valueOf(exitCode) : "");
    result.addAttribute(StageExecutorResultAttribute.JOB_ID, jobName);
    return result;
  }

  static StageExecutorResult describeJobsResultFromStatus(JobStatus jobStatus) {
    if (jobStatus.getFailed() != null && jobStatus.getFailed() > 0) {
      return StageExecutorResult.error();
    } else if (jobStatus.getSucceeded() != null && jobStatus.getSucceeded() > 0) {
      return StageExecutorResult.success();
    } else {
      return StageExecutorResult.active();
    }
  }

  public static KubernetesClient kubernetesClient(String context) {
    return context != null
        ? new DefaultKubernetesClient(Config.autoConfigure(context))
        : new DefaultKubernetesClient();
  }

  static String kubernetesNamespace(KubernetesExecutorParameters executorParams) {
    return executorParams.getNamespace() != null ? executorParams.getNamespace() : "default";
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

  private void kubernetesTerminateJob(KubernetesClient client) {
    if (jobName == null) {
      return;
    }
    ScalableResource<Job> job = client.batch().v1().jobs().inNamespace(namespace).withName(jobName);
    RetryTask.DEFAULT.execute(r -> job.delete());
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, StageExecutorRequest request) {
    return log.with(LogKey.PIPELINE_NAME, request.getPipelineName())
        .with(LogKey.PROCESS_ID, request.getProcessId())
        .with(LogKey.STAGE_NAME, request.getStage().getStageName());
  }
}

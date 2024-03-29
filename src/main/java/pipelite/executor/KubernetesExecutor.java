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
package pipelite.executor;

import com.google.common.flogger.FluentLogger;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
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
import pipelite.executor.describe.context.executor.KubernetesExecutorContext;
import pipelite.executor.describe.context.request.DefaultRequestContext;
import pipelite.log.LogKey;
import pipelite.retryable.Retry;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.KubernetesExecutorParameters;

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

  public KubernetesExecutor() {
    super("Kubernetes");
  }

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

      Retry.DEFAULT.execute(() -> client.batch().v1().jobs().inNamespace(namespace).create(job));

      logContext(log.atInfo(), getRequest()).log("Submitted Kubernetes job " + jobId);
    } catch (KubernetesClientException e) {
      throw new PipeliteException("Kubernetes error", e);
    }
    return new SubmitJobResult(jobId, null);
  }

  @Override
  protected void endJob() {
    StageExecutorResult result = getStageExecutorResult();
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
        result.stdOut(log);
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
    Retry.DEFAULT.execute(() -> job.delete());
  }

  public static Pod lastPodToStart(List<Pod> pods) {
    return pods.stream()
        .max(Comparator.comparing(p -> toLocalDateTime(p.getStatus().getStartTime())))
        .orElse(null);
  }

  public static ContainerStatus lastContainerToFinish(List<ContainerStatus> containerStatuses) {
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

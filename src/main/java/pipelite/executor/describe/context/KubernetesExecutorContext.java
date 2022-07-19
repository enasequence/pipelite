package pipelite.executor.describe.context;

import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import pipelite.executor.KubernetesExecutor;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public class KubernetesExecutorContext extends DefaultExecutorContext<DefaultRequestContext> {

  public KubernetesExecutorContext(KubernetesClient client, String namespace) {
    super("Kubernetes", requests -> KubernetesExecutor.pollJobs(client, namespace, requests), null);
  }
}

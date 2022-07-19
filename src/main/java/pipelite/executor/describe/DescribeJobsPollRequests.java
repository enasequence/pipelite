package pipelite.executor.describe;

import pipelite.executor.describe.context.DefaultRequestContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DescribeJobsPollRequests<RequestContext extends DefaultRequestContext> {
  public final List<String> jobIds;
  /** Jobs indexed by job id. */
  public final Map<String, RequestContext> requests = new HashMap<>();

  public DescribeJobsPollRequests(List<RequestContext> requests) {
    this.jobIds = requests.stream().map(r -> r.getJobId()).collect(Collectors.toList());
    requests.forEach(r -> this.requests.put(r.getJobId(), r));
  }
}

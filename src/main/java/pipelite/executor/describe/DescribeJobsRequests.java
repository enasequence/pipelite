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
package pipelite.executor.describe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import pipelite.executor.describe.context.request.DefaultRequestContext;

public class DescribeJobsRequests<RequestContext extends DefaultRequestContext> {
  private final List<String> jobIds = new ArrayList<>();

  /** Jobs indexed by job id. */
  private final HashMap<String, RequestContext> requests = new HashMap<>();

  public DescribeJobsRequests(List<RequestContext> requests) {
    requests.forEach(
        r -> {
          this.jobIds.add(r.jobId());
          this.requests.put(r.jobId(), r);
        });
  }

  /**
   * Returns the requests.
   *
   * @return the requests.
   */
  public Collection<RequestContext> get() {
    return requests.values();
  }

  /**
   * Returns the request for the given job id.
   *
   * @param jobId the job id.
   * @return request for the given job id.
   */
  public RequestContext get(String jobId) {
    return requests.get(jobId);
  }

  /**
   * Returns the number or requests.
   *
   * @return the number of requests.
   */
  public int size() {
    return requests.size();
  }

  public Collection<String> jobIds() {
    return jobIds;
  }
}

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import pipelite.executor.describe.context.DefaultRequestContext;

public class DescribeJobsPollRequests<RequestContext extends DefaultRequestContext> {
  public final List<String> jobIds;
  /** Jobs indexed by job id. */
  public final Map<String, RequestContext> requests = new HashMap<>();

  public DescribeJobsPollRequests(List<RequestContext> requests) {
    this.jobIds = requests.stream().map(r -> r.getJobId()).collect(Collectors.toList());
    requests.forEach(r -> this.requests.put(r.getJobId(), r));
  }
}

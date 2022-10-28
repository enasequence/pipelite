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
import java.util.List;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.context.request.DefaultRequestContext;

/** Job results. */
public class DescribeJobsResults<RequestContext extends DefaultRequestContext> {
  /** Jobs found by the executor backend. */
  public final List<DescribeJobsResult<RequestContext>> found = new ArrayList<>();

  /** Jobs lost by the executor backend. */
  public final List<DescribeJobsResult<RequestContext>> lost = new ArrayList<>();

  /** Adds a job result. */
  public void add(DescribeJobsResult<RequestContext> result) {
    if (result == null) {
      return;
    }
    if (result.result == null) {
      throw new PipeliteException("Missing job result");
    }
    if (result.result.isLostError()) {
      lost.add(result); // Job result is not available.
    } else {
      found.add(result); // Job result is available.
    }
  }
}

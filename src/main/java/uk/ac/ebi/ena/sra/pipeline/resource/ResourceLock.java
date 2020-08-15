/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.resource;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class ResourceLock {
  private final String[] parts;
  private final String pipeline_name;
  protected final String separator = "/";

  public String getSeparator() {
    return this.separator;
  }

  public ResourceLock(String pipeline_name, String... parts) {
    this.parts = parts;
    this.pipeline_name = pipeline_name;
  }

  public String[] getParts() {
    return parts;
  }

    public String getLockId() {
    return Stream.of(parts).collect(Collectors.joining(getSeparator()));
  }

  public String getLockOwner() {
    return pipeline_name;
  }

  @Override
  public int hashCode() {
    return getParts().hashCode();
  }

  @Override
  public boolean equals(Object another) {
    if (this == another) return true;

    if (null == another) return false;

    if (getClass() != another.getClass()) return false;

    return (getLockId().equals(((ResourceLock) another).getLockId()))
        && (getLockOwner().equals(((ResourceLock) another).getLockOwner()));
  }

  @Override
  public String toString() {
    return String.format("%2$s: %1$s", getLockId(), getLockOwner());
  }
}

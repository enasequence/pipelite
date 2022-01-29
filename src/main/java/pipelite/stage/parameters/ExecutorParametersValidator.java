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
package pipelite.stage.parameters;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import pipelite.exception.PipeliteException;

public class ExecutorParametersValidator {
  protected static <T> T validateNotNull(T value, String name) {
    if (value == null) {
      throw new PipeliteException("Missing " + name + " parameter");
    }
    return value;
  }

  public static Path validatePath(String path, String name) {
    try {
      return Paths.get(path).normalize();
    } catch (Exception e) {
      throw new PipeliteException("Invalid " + name + " parameter path: " + path);
    }
  }

  public static URL validateUrl(String url, String name) {
    validateNotNull(url, name);
    try {
      // Try to create a URL.
      new URL(url).openConnection().connect();
      return new URL(url);
    } catch (Exception e1) {
      // Try to create a resource URL.
      try {
        // Remove leading '/'.
        url = url.replaceFirst("^/+", "");
        URL resourceUrl = ExecutorParameters.class.getClassLoader().getResource(url);
        if (resourceUrl == null) {
          throw new PipeliteException("Invalid " + name + " parameter url: " + url);
        }
        return resourceUrl;
      } catch (Exception e2) {
        throw new PipeliteException("Invalid " + name + " parameter url: " + url);
      }
    }
  }
}

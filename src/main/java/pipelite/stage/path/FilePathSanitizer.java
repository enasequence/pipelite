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
package pipelite.stage.path;

/**
 * Sanitizes a file or directory name by removing all non-word characters. A word character is a
 * character a-z, A-Z, 0-9, including _ (underscore ). If the file name is file path with directory
 * separator then they will be sanitized as well.
 */
public class FilePathSanitizer {

  private FilePathSanitizer() {}

  /**
   * Sanitizes a file or directory name by replacing '-' with '_' and removing all other non-word
   * characters. A word character is a character a-z, A-Z, 0-9, including '_' (underscore). If the
   * file name is file path with directory separator then they will be sanitized as well.
   *
   * @name file or directory name
   * @return the sanitized name
   */
  public static String sanitize(String name) {
    return name.replaceAll("-", "_").replaceAll("\\W+", "");
  }
}

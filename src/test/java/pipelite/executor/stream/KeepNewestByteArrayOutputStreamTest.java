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
package pipelite.executor.stream;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class KeepNewestByteArrayOutputStreamTest {

  @Test
  public void test() {
    KeepNewestByteArrayOutputStream strm = new KeepNewestByteArrayOutputStream(5);
    strm.write(new byte[] {'a'}, 0, 1);
    assertThat(strm.toString()).isEqualTo("a");
    strm.write(new byte[] {'b', 'c'}, 0, 2);
    assertThat(strm.toString()).isEqualTo("abc");
    strm.write(new byte[] {'d'}, 0, 1);
    assertThat(strm.toString()).isEqualTo("abcd");
    strm.write(new byte[] {'e'}, 0, 1);
    assertThat(strm.toString()).isEqualTo("abcde");
    strm.write(new byte[] {'f'}, 0, 1);
    assertThat(strm.toString()).isEqualTo("bcdef");
    strm.write(new byte[] {'1', '2', '3', '4'}, 0, 4);
    assertThat(strm.toString()).isEqualTo("f1234");
  }
}

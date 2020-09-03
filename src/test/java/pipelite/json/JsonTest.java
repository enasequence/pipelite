package pipelite.json;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonTest {

  @Test
  public void testEmpty() {
    Object object = new Object();
    assertThat(Json.serializeThrowIfError(object)).isEqualTo("{ }");
    assertThat(Json.serializeNullIfErrorOrEmpty(object)).isNull();

    HashMap<String, String> map = new HashMap<>();
    assertThat(Json.serializeThrowIfError(map)).isEqualTo("{ }");
    assertThat(Json.serializeNullIfErrorOrEmpty(map)).isNull();
    map.put("test", null);
    assertThat(Json.serializeThrowIfError(map)).isEqualTo("{ }");
    assertThat(Json.serializeNullIfErrorOrEmpty(map)).isNull();
  }

  @Test
  public void testNotEmpty() {
    HashMap<String, String> map = new HashMap<>();
    map.put("test", "test");
    assertThat(Json.serializeThrowIfError(map)).isEqualTo("{\n" + "  \"test\" : \"test\"\n" + "}");
    assertThat(Json.serializeNullIfErrorOrEmpty(map))
        .isEqualTo("{\n" + "  \"test\" : \"test\"\n" + "}");
  }
}

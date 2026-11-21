package org.seattleoba.lambda.dagger;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dagger.Module;
import dagger.Provides;
import javax.inject.Singleton;

/** Dagger module that provides the shared Jackson {@link ObjectMapper}. */
@Module
public class JacksonModule {
  /**
   * Provides an object mapper that ignores unknown JSON properties.
   *
   * @return the mapper
   */
  @Provides
  @Singleton
  public ObjectMapper providesObjectMapper() {
    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return objectMapper;
  }
}

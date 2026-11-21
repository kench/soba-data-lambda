package org.seattleoba.lambda.dagger;

import dagger.Component;
import javax.inject.Singleton;
import org.seattleoba.lambda.requesthandler.BevyTicketDynamodbEventRequestHandler;

/** Dagger component for {@link org.seattleoba.lambda.DynamoDbEventRequestHandler}. */
@Singleton
@Component(modules = {AwsModule.class, JacksonModule.class})
public interface BevyTicketDynamodbEventHandlerComponent {
  /**
   * Returns the request handler.
   *
   * @return the handler
   */
  BevyTicketDynamodbEventRequestHandler requestHandler();
}

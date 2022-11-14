/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.directory.notification.server;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A WebSocketHandler that sends messages from a broker to websockets opened by clients, interleaving with pings to keep connections open.
 * <p>
 * Spring Cloud Stream gets the consumeNotification bean and calls it with the
 * flux from the broker. We call publish and connect to subscribe immediately to the flux
 * and multicast the messages to all connected websockets and to discard the messages when
 * no websockets are connected.
 *
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Jon Harper <jon.harper at rte-france.com>
 */
@Component
public class DirectoryNotificationWebSocketHandler implements WebSocketHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryNotificationWebSocketHandler.class);
    private static final String CATEGORY_BROKER_INPUT = DirectoryNotificationWebSocketHandler.class.getName() + ".messages.input-broker";
    private static final String CATEGORY_WS_OUTPUT = DirectoryNotificationWebSocketHandler.class.getName() + ".messages.output-websocket";
    static final String FILTER_UPDATE_TYPE = "updateType";
    static final String FILTER_STUDY_UUID = "studyUuid";
    static final String HEADER_USER_ID = "userId";
    static final String HEADER_DIRECTORY_UUID = "directoryUuid";
    static final String HEADER_IS_PUBLIC_DIRECTORY = "isPublicDirectory";
    static final String HEADER_UPDATE_TYPE = "updateType";
    static final String HEADER_TIMESTAMP = "timestamp";
    static final String HEADER_ERROR = "error";
    static final String HEADER_ELEMENT_NAME = "elementName";
    static final String HEADER_IS_ROOT_DIRECTORY = "isRootDirectory";
    static final String HEADER_NOTIFICATION_TYPE = "notificationType";
    static final String HEADER_STUDY_UUID = "studyUuid";

    private ObjectMapper jacksonObjectMapper;

    private int heartbeatInterval;

    public DirectoryNotificationWebSocketHandler(ObjectMapper jacksonObjectMapper, @Value("${notification.websocket.heartbeat.interval:30}") int heartbeatInterval) {
        this.jacksonObjectMapper = jacksonObjectMapper;
        this.heartbeatInterval = heartbeatInterval;
    }

    Flux<Message<String>> flux;

    @Bean
    public Consumer<Flux<Message<String>>> consumeNotification() {
        return f -> {
            ConnectableFlux<Message<String>> c = f.log(CATEGORY_BROKER_INPUT, Level.FINE).publish();
            this.flux = c;
            c.connect();
            // Force connect 1 fake subscriber to consumme messages as they come.
            // Otherwise, reactorcore buffers some messages (not until the connectable flux had
            // at least one subscriber. Is there a better way ?
            c.subscribe();
        };
    }

    /**
     * map from the broker flux to the filtered flux for one websocket client, extracting only relevant fields.
     */
    private Flux<WebSocketMessage> notificationFlux(WebSocketSession webSocketSession,
                                                    String userId) {
        return flux.transform(f -> {
            Flux<Message<String>> res = f;
            if (userId != null) {
                res = res.filter(m -> {
                    if (m.getHeaders().get(HEADER_ERROR) != null && !userId.equals(m.getHeaders().get(HEADER_USER_ID))) {
                        return false;
                    }
                    var headerIsPublicDirectory = m.getHeaders().get(HEADER_IS_PUBLIC_DIRECTORY, Boolean.class);
                    return userId.equals(m.getHeaders().get(HEADER_USER_ID)) || (headerIsPublicDirectory != null && headerIsPublicDirectory);
                });
            }
            return res;
        }).filter(message -> {
            String filterUpdateType = (String) webSocketSession.getAttributes().get(FILTER_UPDATE_TYPE);
            String filterStudyUuid = (String) webSocketSession.getAttributes().get(FILTER_STUDY_UUID);
            if (filterUpdateType != null && !filterUpdateType.equals(message.getHeaders().get(HEADER_UPDATE_TYPE))) {
                return false;
            }
            return filterStudyUuid == null || filterStudyUuid.equals(message.getHeaders().get(HEADER_STUDY_UUID));
        }).map(m -> {
            try {
                return jacksonObjectMapper.writeValueAsString(Map.of(
                        "payload", m.getPayload(),
                        "headers", toResultHeader(m.getHeaders())));
            } catch (JsonProcessingException e) {
                throw new DirectoryNotificationServerRuntimeException(e.toString());
            }
        }).log(CATEGORY_WS_OUTPUT, Level.FINE).map(webSocketSession::textMessage);
    }

    private static Map<String, Object> toResultHeader(Map<String, Object> messageHeader) {
        var resHeader = new HashMap<String, Object>();
        resHeader.put(HEADER_TIMESTAMP, messageHeader.get(HEADER_TIMESTAMP));
        resHeader.put(HEADER_UPDATE_TYPE, messageHeader.get(HEADER_UPDATE_TYPE));

        if (messageHeader.get(HEADER_DIRECTORY_UUID) != null) {
            resHeader.put(HEADER_DIRECTORY_UUID, messageHeader.get(HEADER_DIRECTORY_UUID));
        }
        if (messageHeader.get(HEADER_ERROR) != null) {
            resHeader.put(HEADER_ERROR, messageHeader.get(HEADER_ERROR));
        }
        if (messageHeader.get(HEADER_IS_ROOT_DIRECTORY) != null) {
            resHeader.put(HEADER_IS_ROOT_DIRECTORY, messageHeader.get(HEADER_IS_ROOT_DIRECTORY));
        }
        if (messageHeader.get(HEADER_NOTIFICATION_TYPE) != null) {
            resHeader.put(HEADER_NOTIFICATION_TYPE, messageHeader.get(HEADER_NOTIFICATION_TYPE));
        }
        if (messageHeader.get(HEADER_ELEMENT_NAME) != null) {
            resHeader.put(HEADER_ELEMENT_NAME, messageHeader.get(HEADER_ELEMENT_NAME));
        }
        if (messageHeader.get(HEADER_STUDY_UUID) != null) {
            resHeader.put(HEADER_STUDY_UUID, messageHeader.get(HEADER_STUDY_UUID));
        }
        return resHeader;
    }

    /**
     * A heartbeat flux sending websockets pings
     */
    private Flux<WebSocketMessage> heartbeatFlux(WebSocketSession webSocketSession) {
        return Flux.interval(Duration.ofSeconds(heartbeatInterval)).map(n -> webSocketSession
                .pingMessage(dbf -> dbf.wrap((webSocketSession.getId() + "-" + n).getBytes(StandardCharsets.UTF_8))));
    }

    public Flux<WebSocketMessage> receive(WebSocketSession webSocketSession) {
        return webSocketSession.receive()
                .doOnNext(webSocketMessage -> {
                    try {
                        String wsPayload = webSocketMessage.getPayloadAsText();
                        //if it's not the heartbeat
                        if (!(wsPayload.startsWith(webSocketSession.getId() + "-"))) {
                            LOGGER.debug("Message received : {} by session {}", wsPayload, webSocketSession.getId());
                            Filters receivedFilters = jacksonObjectMapper.readValue(webSocketMessage.getPayloadAsText(), Filters.class);
                            //because null is not allowed in ConcurrentHashMap and will cause the websocket to close
                            if (receivedFilters.getUpdateType() != null) {
                                webSocketSession.getAttributes().put(FILTER_UPDATE_TYPE, receivedFilters.getUpdateType());
                            }
                            if (receivedFilters.getStudyUuid() != null) {
                                webSocketSession.getAttributes().put(FILTER_STUDY_UUID, receivedFilters.getStudyUuid());
                            }
                        }
                    } catch (JsonProcessingException e) {
                        LOGGER.error("Unprocessable message received");
                    }
                });
    }

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        String userId = webSocketSession.getHandshakeInfo().getHeaders().getFirst(HEADER_USER_ID);
        return webSocketSession
                .send(notificationFlux(webSocketSession, userId)
                        .mergeWith(heartbeatFlux(webSocketSession)))
                .and(receive(webSocketSession));
    }
}

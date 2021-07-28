/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.directory.notification.server;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import static org.gridsuite.directory.notification.server.DirectoryNotificationWebSocketHandler.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Jon Harper <jon.harper at rte-france.com>
 */
public class DirectoryNotificationWebSocketHandlerTest {

    private ObjectMapper objectMapper;
    private WebSocketSession ws;
    private HandshakeInfo handshakeinfo;
    private Flux<Message<String>> flux;

    @Before
    public void setup() {
        objectMapper = new ObjectMapper();
        var dataBufferFactory = new DefaultDataBufferFactory();

        ws = Mockito.mock(WebSocketSession.class);
        handshakeinfo = Mockito.mock(HandshakeInfo.class);

        when(ws.getHandshakeInfo()).thenReturn(handshakeinfo);
        when(ws.receive()).thenReturn(Flux.empty());
        when(ws.send(any())).thenReturn(Mono.empty());
        when(ws.textMessage(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String str = (String) args[0];
            return new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(str.getBytes()));
        });
        when(ws.pingMessage(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Function<DataBufferFactory, DataBuffer> f = (Function<DataBufferFactory, DataBuffer>) args[0];
            return new WebSocketMessage(WebSocketMessage.Type.PING, f.apply(dataBufferFactory));
        });
        when(ws.getId()).thenReturn("testsession");

    }

    private void setUpUriComponentBuilder(String connectedUserId) {
        setUpUriComponentBuilder(connectedUserId, null, null);
    }

    private void setUpUriComponentBuilder(String connectedUserId, String filterStudyUuid, String filterUpdateType) {
        UriComponentsBuilder uriComponentBuilder = UriComponentsBuilder.fromUriString("http://localhost:1234/notify");

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HEADER_USER_ID, connectedUserId);
        when(handshakeinfo.getHeaders()).thenReturn(httpHeaders);

        if (filterStudyUuid != null) {
            uriComponentBuilder.queryParam(QUERY_STUDY_UUID, filterStudyUuid);
        }
        if (filterUpdateType != null) {
            uriComponentBuilder.queryParam(QUERY_UPDATE_TYPE, filterUpdateType);
        }

        when(handshakeinfo.getUri()).thenReturn(uriComponentBuilder.build().toUri());
    }

    private void withFilters(String filterStudyUuid, String filterUpdateType) {
        String connectedUserId = "userId";
        String otherUserId = "userId2";
        setUpUriComponentBuilder(connectedUserId, filterStudyUuid, filterUpdateType);

        var notificationWebSocketHandler = new DirectoryNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE);
        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        notificationWebSocketHandler.consumeNotification().accept(flux);
        var sink = atomicRef.get();
        notificationWebSocketHandler.handle(ws);

        List<GenericMessage<String>> refMessages = Stream.<Map<String, Object>>of(
                Map.of(HEADER_STUDY_UUID, "foo", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "baz", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "foo", HEADER_UPDATE_TYPE, "rab"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "rab"),
                Map.of(HEADER_STUDY_UUID, "baz", HEADER_UPDATE_TYPE, "rab"),
                Map.of(HEADER_STUDY_UUID, "foo", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "baz", HEADER_UPDATE_TYPE, "oof"),

                Map.of(HEADER_STUDY_UUID, "foo bar/bar", HEADER_UPDATE_TYPE, "foobar"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "studies", HEADER_ERROR, "error_message"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "rab", HEADER_SUBSTATIONS_IDS, "s1"),

                Map.of(HEADER_STUDY_UUID, "public_" + connectedUserId, HEADER_UPDATE_TYPE, "oof", HEADER_USER_ID, connectedUserId, HEADER_IS_PUBLIC_STUDY, true),
                Map.of(HEADER_STUDY_UUID, "private_" + connectedUserId, HEADER_UPDATE_TYPE, "oof", HEADER_USER_ID, connectedUserId, HEADER_IS_PUBLIC_STUDY, false),
                Map.of(HEADER_STUDY_UUID, "public_" + otherUserId, HEADER_UPDATE_TYPE, "rab", HEADER_USER_ID, otherUserId, HEADER_IS_PUBLIC_STUDY, true),
                Map.of(HEADER_STUDY_UUID, "private_" + otherUserId, HEADER_UPDATE_TYPE, "rab", HEADER_USER_ID, otherUserId, HEADER_IS_PUBLIC_STUDY, false),
                Map.of(HEADER_STUDY_UUID, "public_" + otherUserId, HEADER_UPDATE_TYPE, "rab", HEADER_USER_ID, otherUserId, HEADER_IS_PUBLIC_STUDY, true, HEADER_ERROR, "error_message"))
                .map(map -> new GenericMessage<>("", map))
                .collect(Collectors.toList());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Flux<WebSocketMessage>> argument = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument.capture());
        List<String> messages = new ArrayList<>();
        argument.getValue().map(WebSocketMessage::getPayloadAsText).subscribe(messages::add);
        refMessages.forEach(sink::next);
        sink.complete();

        List<Map<String, Object>> expected = refMessages.stream()
                .filter(m -> {
                    String studyUuid = (String) m.getHeaders().get(HEADER_STUDY_UUID);
                    String userId = (String) m.getHeaders().get(HEADER_USER_ID);
                    String updateType = (String) m.getHeaders().get(HEADER_UPDATE_TYPE);
                    Boolean headerIsPublicStudy = m.getHeaders().get(HEADER_IS_PUBLIC_STUDY, Boolean.class);
                    String headerMessageError = (String) m.getHeaders().get(HEADER_ERROR);
                    return (headerMessageError == null || connectedUserId.equals(userId))
                            && (headerIsPublicStudy == null || headerIsPublicStudy || connectedUserId.equals(userId))
                            && (filterStudyUuid == null || filterStudyUuid.equals(studyUuid))
                            && (filterUpdateType == null || filterUpdateType.equals(updateType));
                })
                .map(GenericMessage::getHeaders)
                .map(this::toResultHeader)
                .collect(Collectors.toList());

        List<Map<String, Object>> actual = messages.stream().map(t -> {
            try {
                return toResultHeader(((Map<String, Map<String, Object>>) objectMapper.readValue(t, Map.class)).get("headers"));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        assertEquals(expected, actual);
        assertNotEquals(0, actual.size());
        assertEquals(0, actual.stream().filter(m -> m.get(HEADER_STUDY_UUID).equals("private_" + otherUserId)).count());
        assertEquals(0, actual.stream().filter(m -> m.get(HEADER_STUDY_UUID).equals("public_" + otherUserId) && m.get(HEADER_ERROR) != null).count());
    }

    private Map<String, Object> toResultHeader(Map<String, Object> messageHeader) {
        var resHeader = new HashMap<String, Object>();
        resHeader.put(HEADER_TIMESTAMP, messageHeader.get(HEADER_TIMESTAMP));
        resHeader.put(HEADER_UPDATE_TYPE, messageHeader.get(HEADER_UPDATE_TYPE));

        if (messageHeader.get(HEADER_STUDY_UUID) != null) {
            resHeader.put(HEADER_STUDY_UUID, messageHeader.get(HEADER_STUDY_UUID));
        }
        if (messageHeader.get(HEADER_STUDY_NAME) != null) {
            resHeader.put(HEADER_STUDY_NAME, messageHeader.get(HEADER_STUDY_NAME));
        }
        if (messageHeader.get(HEADER_ERROR) != null) {
            resHeader.put(HEADER_ERROR, messageHeader.get(HEADER_ERROR));
        }
        if (messageHeader.get(HEADER_SUBSTATIONS_IDS) != null) {
            resHeader.put(HEADER_SUBSTATIONS_IDS, messageHeader.get(HEADER_SUBSTATIONS_IDS));
        }

        resHeader.remove(HEADER_TIMESTAMP);

        return resHeader;
    }

    @Test
    public void testWithoutFilter() {
        withFilters(null, null);
    }

    @Test
    public void testStudyFilter() {
        withFilters("bar", null);
    }

    @Test
    public void testTypeFilter() {
        withFilters(null, "rab");
    }

    @Test
    public void testStudyAndTypeFilter() {
        withFilters("bar", "rab");
    }

    @Test
    public void testEncodingCharacters() {
        withFilters("foo bar/bar", "foobar");
    }

    @Test
    public void testHeartbeat() {
        setUpUriComponentBuilder("userId");

        var notificationWebSocketHandler = new DirectoryNotificationWebSocketHandler(null, 1);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeNotification().accept(flux);
        notificationWebSocketHandler.handle(ws);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Flux<WebSocketMessage>> argument = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument.capture());
        assertEquals("testsession-0", argument.getValue().blockFirst(Duration.ofSeconds(10)).getPayloadAsText());
    }

    @Test
    public void testDiscard() {
        setUpUriComponentBuilder("userId");

        var notificationWebSocketHandler = new DirectoryNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE);
        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        notificationWebSocketHandler.consumeNotification().accept(flux);
        var sink = atomicRef.get();
        Map<String, Object> headers = Map.of(HEADER_STUDY_UUID, "foo", HEADER_UPDATE_TYPE, "oof");

        sink.next(new GenericMessage<>("", headers)); // should be discarded, no client connected

        notificationWebSocketHandler.handle(ws);

        ArgumentCaptor<Flux<WebSocketMessage>> argument1 = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument1.capture());
        List<String> messages1 = new ArrayList<>();
        Flux<WebSocketMessage> out1 = argument1.getValue();
        Disposable d1 = out1.map(WebSocketMessage::getPayloadAsText).subscribe(messages1::add);
        d1.dispose();

        sink.next(new GenericMessage<>("", headers)); // should be discarded, first client disconnected

        notificationWebSocketHandler.handle(ws);

        ArgumentCaptor<Flux<WebSocketMessage>> argument2 = ArgumentCaptor.forClass(Flux.class);
        verify(ws, times(2)).send(argument2.capture());
        List<String> messages2 = new ArrayList<>();
        Flux<WebSocketMessage> out2 = argument2.getValue();
        Disposable d2 = out2.map(WebSocketMessage::getPayloadAsText).subscribe(messages2::add);
        d2.dispose();

        sink.complete();
        assertEquals(0, messages1.size());
        assertEquals(0, messages2.size());
    }
}

/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.directory.notification.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.gridsuite.directory.notification.server.dto.Filters;
import org.gridsuite.directory.notification.server.dto.FiltersToAdd;
import org.gridsuite.directory.notification.server.dto.FiltersToRemove;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.gridsuite.directory.notification.server.DirectoryNotificationWebSocketHandler.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Jon Harper <jon.harper at rte-france.com>
 */
class DirectoryNotificationWebSocketHandlerTest {

    private ObjectMapper objectMapper;
    private WebSocketSession ws;
    private WebSocketSession ws2;
    private HandshakeInfo handshakeinfo;
    private static final String ELEMENT_UUID = "87a52b4a-143d-4d4e-8b27-88abde90bd0d"; //can't be random with @CsvSource

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper();
        var dataBufferFactory = new DefaultDataBufferFactory();

        ws = Mockito.mock(WebSocketSession.class);
        ws2 = Mockito.mock(WebSocketSession.class);
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

        when(ws2.getHandshakeInfo()).thenReturn(handshakeinfo);
        when(ws2.send(any())).thenReturn(Mono.empty());
        when(ws2.textMessage(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String str = (String) args[0];
            return new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(str.getBytes()));
        });
        when(ws2.pingMessage(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Function<DataBufferFactory, DataBuffer> f = (Function<DataBufferFactory, DataBuffer>) args[0];
            return new WebSocketMessage(WebSocketMessage.Type.PING, f.apply(dataBufferFactory));
        });
        when(ws2.getId()).thenReturn("testsession");

    }

    private void setUpUriComponentBuilder(String connectedUserId) {
        setUpUriComponentBuilder(connectedUserId, null, null);
    }

    private void setUpUriComponentBuilder(String connectedUserId, String queryFilterUpdateType, String queryFilterElementUuid) {
        UriComponentsBuilder uriComponentBuilder = UriComponentsBuilder.fromUriString("http://localhost:1234/notify");

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HEADER_USER_ID, connectedUserId);
        when(handshakeinfo.getHeaders()).thenReturn(httpHeaders);

        if (queryFilterUpdateType != null) {
            uriComponentBuilder.queryParam(FILTER_UPDATE_TYPE, queryFilterUpdateType);
        }

        if (queryFilterElementUuid != null) {
            uriComponentBuilder.queryParam(QUERY_ELEMENT_UUID, queryFilterElementUuid);
        }
        when(handshakeinfo.getUri()).thenReturn(uriComponentBuilder.build().toUri());
    }

    @CsvSource(value = {
        "null, null, false", //testWithoutFilterInBody
        "null, null, true",  //testWithoutFilterInUrl
        "rab, null, false", //testTypeFilterInBody
        "rab, null, true",  //testTypeFilterInUrl
        "foobar, null, false", //testEncodingCharactersInBody
        "foobar, null, true",  //testEncodingCharactersInUrl
        "null, " + ELEMENT_UUID + ", false", //testElementUuidFilterInBody
        "null, " + ELEMENT_UUID + ", true",  //testElementUuidFilterInUrl
    }, nullValues = {"null"})
    @ParameterizedTest(name = "inUrl={2} filter({0}) filterUuid({1})")
    void testWithFilters(String filterUpdateType, String filterElementUuid, boolean inUrl) {
        String connectedUserId = "userId";
        String otherUserId = "userId2";

        Map<String, Object> filterMap = new HashMap<>();
        when(ws.getAttributes()).thenReturn(filterMap);

        if (inUrl) {
            setUpUriComponentBuilder(connectedUserId, filterUpdateType, filterElementUuid);
        } else {
            setUpUriComponentBuilder(connectedUserId);
        }

        var notificationWebSocketHandler = new DirectoryNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE);
        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        notificationWebSocketHandler.consumeNotification().accept(flux);
        var sink = atomicRef.get();
        notificationWebSocketHandler.handle(ws);

        if (!inUrl) {
            if (filterUpdateType != null) {
                filterMap.put(FILTER_UPDATE_TYPE, filterUpdateType);
            }
            if (filterElementUuid != null) {
                filterMap.put(FILTER_ELEMENT_UUIDS, new HashSet<>(Set.of(filterElementUuid)));
            }
        }

        List<GenericMessage<String>> refMessages = Stream.<Map<String, Object>>of(
            Map.of(HEADER_UPDATE_TYPE, "oof"),
            Map.of(HEADER_UPDATE_TYPE, "oof"),
            Map.of(HEADER_UPDATE_TYPE, "oof"),
            Map.of(HEADER_UPDATE_TYPE, "rab"),
            Map.of(HEADER_UPDATE_TYPE, "rab"),
            Map.of(HEADER_UPDATE_TYPE, "rab"),
            Map.of(HEADER_UPDATE_TYPE, "oof"),
            Map.of(HEADER_UPDATE_TYPE, "oof"),
            Map.of(HEADER_UPDATE_TYPE, "oof"),

            Map.of(HEADER_UPDATE_TYPE, "foobar", HEADER_IS_PUBLIC_DIRECTORY, true),

            Map.of(HEADER_DIRECTORY_UUID, "public_" + otherUserId, HEADER_UPDATE_TYPE, "foobar", HEADER_IS_PUBLIC_DIRECTORY, true, HEADER_ERROR, "error_message"),
            Map.of(HEADER_DIRECTORY_UUID, "public_" + connectedUserId, HEADER_UPDATE_TYPE, "oof", HEADER_USER_ID, connectedUserId, HEADER_IS_PUBLIC_DIRECTORY, true, HEADER_ELEMENT_NAME, "titi"),
            Map.of(HEADER_DIRECTORY_UUID, "private_" + connectedUserId, HEADER_UPDATE_TYPE, "oof", HEADER_USER_ID, connectedUserId, HEADER_IS_PUBLIC_DIRECTORY, false),
            Map.of(HEADER_DIRECTORY_UUID, "public_" + otherUserId, HEADER_UPDATE_TYPE, "rab", HEADER_USER_ID, otherUserId, HEADER_IS_PUBLIC_DIRECTORY, true, HEADER_ELEMENT_NAME, "toto"),
            Map.of(HEADER_DIRECTORY_UUID, "private_" + otherUserId, HEADER_UPDATE_TYPE, "rab", HEADER_USER_ID, otherUserId, HEADER_IS_PUBLIC_DIRECTORY, false),
            Map.of(HEADER_DIRECTORY_UUID, "public_" + otherUserId, HEADER_UPDATE_TYPE, "rab", HEADER_USER_ID, otherUserId, HEADER_IS_PUBLIC_DIRECTORY, true,
                    HEADER_ERROR, "error_message", HEADER_NOTIFICATION_TYPE, "UPDATE_DIRECTORY", HEADER_IS_ROOT_DIRECTORY, "false", HEADER_ELEMENT_NAME, "tutu", HEADER_IS_DIRECTORY_MOVING, "false"),
            Map.of(HEADER_ELEMENT_UUID, ELEMENT_UUID, HEADER_USER_ID, connectedUserId),
            Map.of(HEADER_USER_ID, connectedUserId, HEADER_USER_MESSAGE, "testMessage"))
        .map(map -> new GenericMessage<>("", map))
        .toList();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Flux<WebSocketMessage>> argument = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument.capture());
        List<String> messages = new ArrayList<>();
        argument.getValue().map(WebSocketMessage::getPayloadAsText).subscribe(messages::add);
        refMessages.forEach(sink::next);
        sink.complete();

        List<Map<String, Object>> expected = refMessages.stream()
                .filter(m -> {
                    String userId = (String) m.getHeaders().get(HEADER_USER_ID);
                    String updateType = (String) m.getHeaders().get(HEADER_UPDATE_TYPE);
                    String elementUuid = (String) m.getHeaders().get(HEADER_ELEMENT_UUID);
                    String directoryUuid = (String) m.getHeaders().get(HEADER_DIRECTORY_UUID);
                    Boolean headerIsPublicDirectory = m.getHeaders().get(HEADER_IS_PUBLIC_DIRECTORY, Boolean.class);
                    if (m.getHeaders().get(HEADER_ERROR) != null && !connectedUserId.equals(userId)) {
                        return false;
                    }
                    return (connectedUserId.equals(userId) || headerIsPublicDirectory != null && headerIsPublicDirectory)
                            && (filterUpdateType == null || filterUpdateType.equals(updateType))
                            && (filterElementUuid == null || filterElementUuid.equals(directoryUuid) || filterElementUuid.equals(elementUuid));
                })
                .map(GenericMessage::getHeaders)
                .map(DirectoryNotificationWebSocketHandlerTest::toResultHeader)
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
        if (messageHeader.get(HEADER_ELEMENT_UUID) != null) {
            resHeader.put(HEADER_ELEMENT_UUID, messageHeader.get(HEADER_ELEMENT_UUID));
        }
        if (messageHeader.get(HEADER_IS_DIRECTORY_MOVING) != null) {
            resHeader.put(HEADER_IS_DIRECTORY_MOVING, messageHeader.get(HEADER_IS_DIRECTORY_MOVING));
        }
        if (messageHeader.get(HEADER_USER_MESSAGE) != null) {
            resHeader.put(HEADER_USER_MESSAGE, messageHeader.get(HEADER_USER_MESSAGE));
        }
        resHeader.remove(HEADER_TIMESTAMP);

        return resHeader;
    }

    @Test
    void testHeartbeat() {
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
    void testWsReceiveFilters() throws Exception {
        setUpUriComponentBuilder("userId");
        var dataBufferFactory = new DefaultDataBufferFactory();

        var map = new ConcurrentHashMap<String, Object>();
        map.put(FILTER_ELEMENT_UUIDS, new HashSet<>(Set.of("elementUuidFilter1")));
        ArrayList<String> elementUuid = new ArrayList<>();
        elementUuid.add("elementUuidFilter2");
        FiltersToAdd filtersToAdd = new FiltersToAdd("updateTypeFilter", elementUuid);
        FiltersToRemove filtersToRemove = new FiltersToRemove(false, null);
        Filters filters = new Filters(filtersToAdd, filtersToRemove);
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(filters);
        when(ws2.receive()).thenReturn(Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(json.getBytes()))));
        when(ws2.getAttributes()).thenReturn(map);

        var notificationWebSocketHandler = new DirectoryNotificationWebSocketHandler(new ObjectMapper(), 60);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeNotification().accept(flux);
        notificationWebSocketHandler.receive(ws2).subscribe();

        assertEquals("updateTypeFilter", map.get(FILTER_UPDATE_TYPE));
        assertEquals(2, ((Set<String>) map.get(FILTER_ELEMENT_UUIDS)).size());
        assertTrue(((Set<String>) map.get(FILTER_ELEMENT_UUIDS)).contains("elementUuidFilter1") &&
                ((Set<String>) map.get(FILTER_ELEMENT_UUIDS)).contains("elementUuidFilter2"));
    }

    @Test
    void testWsRemoveFilters() throws Exception {
        setUpUriComponentBuilder("userId");
        var dataBufferFactory = new DefaultDataBufferFactory();

        Set<String> elementUuid = new HashSet<>(Set.of("elementUuidFilter1", "elementUuidFilter2", "elementUuidFilter3"));
        var map = new ConcurrentHashMap<String, Object>();
        map.put(FILTER_UPDATE_TYPE, "updateType");
        map.put(FILTER_ELEMENT_UUIDS, elementUuid);
        FiltersToAdd filtersToAdd = new FiltersToAdd();
        FiltersToRemove filtersToRemove = new FiltersToRemove(true, new ArrayList<>(Arrays.asList("elementUuidFilter1", "elementUuidFilter2")));
        Filters filters = new Filters(filtersToAdd, filtersToRemove);
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(filters);
        when(ws2.receive()).thenReturn(Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(json.getBytes()))));
        when(ws2.getAttributes()).thenReturn(map);

        assertEquals("updateType", ws2.getAttributes().get(FILTER_UPDATE_TYPE));
        assertEquals(elementUuid, ws2.getAttributes().get(FILTER_ELEMENT_UUIDS));
        var notificationWebSocketHandler = new DirectoryNotificationWebSocketHandler(new ObjectMapper(), Integer.MAX_VALUE);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeNotification().accept(flux);
        notificationWebSocketHandler.receive(ws2).subscribe();

        assertNull(ws2.getAttributes().get(FILTER_UPDATE_TYPE));
        assertEquals(1, ((Set<String>) map.get(FILTER_ELEMENT_UUIDS)).size());
        assertTrue(((Set<String>) map.get(FILTER_ELEMENT_UUIDS)).contains("elementUuidFilter3"));
    }

    @Test
    void testWsReceiveEmptyFilters() throws Exception {
        setUpUriComponentBuilder("userId");
        var dataBufferFactory = new DefaultDataBufferFactory();

        var map = new ConcurrentHashMap<String, Object>();
        Filters filters = new Filters();
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(filters);
        when(ws2.receive()).thenReturn(Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(json.getBytes()))));
        when(ws2.getAttributes()).thenReturn(map);

        var notificationWebSocketHandler = new DirectoryNotificationWebSocketHandler(new ObjectMapper(), Integer.MAX_VALUE);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeNotification().accept(flux);
        notificationWebSocketHandler.receive(ws2).subscribe();

        assertNull(map.get(FILTER_UPDATE_TYPE));
        assertNull(map.get(FILTER_ELEMENT_UUIDS));
    }

    @Test
    void testWsReceiveUnprocessableFilter() {
        setUpUriComponentBuilder("userId");
        var dataBufferFactory = new DefaultDataBufferFactory();

        var map = new ConcurrentHashMap<String, Object>();
        when(ws2.receive()).thenReturn(Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap("UnprocessableFilter".getBytes()))));
        when(ws2.getAttributes()).thenReturn(map);

        var notificationWebSocketHandler = new DirectoryNotificationWebSocketHandler(new ObjectMapper(), 60);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeNotification().accept(flux);
        notificationWebSocketHandler.receive(ws2).subscribe();

        assertNull(map.get(FILTER_UPDATE_TYPE));
        assertNull(map.get(FILTER_ELEMENT_UUIDS));
    }

    @Test
    void testDiscard() {
        setUpUriComponentBuilder("userId");

        var notificationWebSocketHandler = new DirectoryNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE);
        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        notificationWebSocketHandler.consumeNotification().accept(flux);
        var sink = atomicRef.get();
        Map<String, Object> headers = Map.of(HEADER_UPDATE_TYPE, "oof");

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

# Directory Notification Server

[![Actions Status](https://github.com/gridsuite/directory-notification-server/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/gridsuite/directory-notification-server/actions)
[![Coverage Status](https://sonarcloud.io/api/project_badges/measure?project=org.gridsuite%3Adirectory-notification-server&metric=coverage)](https://sonarcloud.io/component_measures?id=org.gridsuite%3Adirectory-notification-server&metric=coverage)
[![MPL-2.0 License](https://img.shields.io/badge/license-MPL_2.0-blue.svg)](https://www.mozilla.org/en-US/MPL/2.0/)

## Description

Directory Notification Server is the GridExplore notification service. It consumes directory/element update messages from RabbitMQ and exposes them to GridExplore clients through a WebSocket endpoint, applying per-user visibility and filtering rules.

## Functional Scope

- Consume directory update messages from the `directory.update` RabbitMQ destination.
- Broadcast updates to connected WebSocket clients, applying **visibility filtering**: a message is only sent to a client if it targets that client's `userId`, or if it is flagged as belonging to a public directory (`isPublicDirectory` header) — unless it carries an `error` or `userMessage` header, in which case it is only sent to the targeted user.
- Support additional client-side filtering by `updateType` and/or `elementUuids`, provided as query parameters at connection time.
- Match the `elementUuids` filter either directly against the notified element's uuid, or against the uuids listed in the `directoriesInfos` header (used when a notification impacts a set of directories).
- Forward the update payload and selected message headers needed by the frontend (`timestamp`, `updateType`, `error`, `notificationType`, `elementNames`, `directoriesInfos`, `elementUuid`, `isDirectoryMoving`, `userMessage`, `userId`, `exportUuid`).
- Send periodic WebSocket ping frames to keep client connections alive.

## Technical Stack

- Spring Boot (WebFlux, Actuator, Cloud Stream)
- RabbitMQ via Spring Cloud Stream
- WebSocket

## Development Scripts

Build Docker image

```shell
mvn install -DskipTests -Dpowsybl.docker.install
```

## WebSocket API

The service exposes one WebSocket endpoint:

```text
/notify
```

Connection-time filters can be passed as query parameters:

```text
/notify?updateType=<type>&elementUuid=<uuid>
```

The `userId` used for visibility filtering is read from the `userId` handshake header.

Each outbound text message is a JSON object with the consumed message payload and a filtered header set (only headers present in the original message are included):

```json
{
  "payload": "...",
  "headers": {
    "timestamp": "...",
    "updateType": "...",
    "error": "...",
    "notificationType": "...",
    "elementNames": "...",
    "directoriesInfos": "...",
    "elementUuid": "...",
    "isDirectoryMoving": "...",
    "userMessage": "...",
    "userId": "...",
    "exportUuid": "..."
  }
}
```

`error`, `notificationType`, `elementNames`, `directoriesInfos`, `elementUuid`, `isDirectoryMoving`, `userMessage`, `userId`, and `exportUuid` are included only when present in the consumed RabbitMQ message headers.

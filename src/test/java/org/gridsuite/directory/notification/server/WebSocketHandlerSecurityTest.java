/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.directory.notification.server;

import com.powsybl.ws.commons.security.webclient.AbstractWebSocketHandlerSecurityTest;

/**
 * @author Franck Lecuyer {@literal <franck.lecuyer_externe at rte-france.com>}
 */
class WebSocketHandlerSecurityTest extends AbstractWebSocketHandlerSecurityTest {

    @Override
    protected String getBasePackage() {
        return "org.gridsuite.directory.notification.server";
    }
}

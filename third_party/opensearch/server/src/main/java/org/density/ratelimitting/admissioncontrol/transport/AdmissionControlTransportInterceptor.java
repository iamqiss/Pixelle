/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.ratelimitting.admissioncontrol.transport;

import org.density.ratelimitting.admissioncontrol.AdmissionControlService;
import org.density.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.density.transport.TransportInterceptor;
import org.density.transport.TransportRequest;
import org.density.transport.TransportRequestHandler;

/**
 * This class allows throttling by intercepting requests on both the sender and the receiver side.
 */
public class AdmissionControlTransportInterceptor implements TransportInterceptor {

    AdmissionControlService admissionControlService;

    public AdmissionControlTransportInterceptor(AdmissionControlService admissionControlService) {
        this.admissionControlService = admissionControlService;
    }

    /**
     *
     * @return admissionController handler to intercept transport requests
     */
    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
        String action,
        String executor,
        boolean forceExecution,
        TransportRequestHandler<T> actualHandler,
        AdmissionControlActionType admissionControlActionType
    ) {
        return new AdmissionControlTransportHandler<>(
            action,
            actualHandler,
            this.admissionControlService,
            forceExecution,
            admissionControlActionType
        );
    }
}

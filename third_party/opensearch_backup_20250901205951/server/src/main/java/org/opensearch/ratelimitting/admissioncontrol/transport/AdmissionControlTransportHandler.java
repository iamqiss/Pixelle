/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.ratelimitting.admissioncontrol.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.core.concurrency.DensityRejectedExecutionException;
import org.density.ratelimitting.admissioncontrol.AdmissionControlService;
import org.density.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.density.tasks.Task;
import org.density.transport.TransportChannel;
import org.density.transport.TransportRequest;
import org.density.transport.TransportRequestHandler;

/**
 * AdmissionControl Handler to intercept Transport Requests.
 * @param <T> Transport Request
 */
public class AdmissionControlTransportHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

    private final String action;
    private final TransportRequestHandler<T> actualHandler;
    protected final Logger log = LogManager.getLogger(this.getClass());
    AdmissionControlService admissionControlService;
    boolean forceExecution;
    AdmissionControlActionType admissionControlActionType;

    public AdmissionControlTransportHandler(
        String action,
        TransportRequestHandler<T> actualHandler,
        AdmissionControlService admissionControlService,
        boolean forceExecution,
        AdmissionControlActionType admissionControlActionType
    ) {
        super();
        this.action = action;
        this.actualHandler = actualHandler;
        this.admissionControlService = admissionControlService;
        this.forceExecution = forceExecution;
        this.admissionControlActionType = admissionControlActionType;
    }

    /**
     * @param request Transport Request that landed on the node
     * @param channel Transport channel allows to send a response to a request
     * @param task Current task that is executing
     * @throws Exception when admission control rejected the requests
     */
    @Override
    public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
        // skip admission control if force execution is true
        if (!this.forceExecution) {
            // intercept the transport requests here and apply admission control
            try {
                this.admissionControlService.applyTransportAdmissionControl(this.action, this.admissionControlActionType);
            } catch (final DensityRejectedExecutionException openSearchRejectedExecutionException) {
                log.warn(openSearchRejectedExecutionException.getMessage());
                channel.sendResponse(openSearchRejectedExecutionException);
                return;
            }
        }
        actualHandler.messageReceived(request, channel, task);
    }
}

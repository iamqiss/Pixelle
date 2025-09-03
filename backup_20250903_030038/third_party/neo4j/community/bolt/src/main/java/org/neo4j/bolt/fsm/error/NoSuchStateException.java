/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.fsm.error;

import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.Status.General;
import org.neo4j.kernel.api.exceptions.Status.HasStatus;

/**
 * Represents error cases in which a referenced state is not accessible within a state machine.
 */
public class NoSuchStateException extends StateMachineException
        implements HasStatus, ConnectionTerminating, ErrorGqlStatusObject {
    private final StateReference target;
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    @Deprecated
    public NoSuchStateException(StateReference target, Throwable cause) {
        super("No such state: " + target.name(), cause);
        this.target = target;

        this.gqlStatusObject = null;
        this.oldMessage = "No such state: " + target.name();
    }

    public NoSuchStateException(ErrorGqlStatusObject gqlStatusObject, StateReference target, Throwable cause) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, "No such state: " + target.name()), cause);
        this.gqlStatusObject = GqlHelper.getInnerGqlStatusObject(gqlStatusObject, cause);
        this.oldMessage = "No such state: " + target.name();

        this.target = target;
    }

    @Deprecated
    public NoSuchStateException(StateReference target) {
        this(target, null);
    }

    private NoSuchStateException(ErrorGqlStatusObject gqlStatusObject, StateReference target) {
        this(gqlStatusObject, target, null);
    }

    public static NoSuchStateException invalidServerStateTransition(
            String msgTitle, String invalidState, StateReference target) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N00)
                .withParam(GqlParams.StringParam.msgTitle, msgTitle)
                .withParam(GqlParams.StringParam.msg, invalidState)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N09)
                        .withParam(GqlParams.StringParam.boltServerState, invalidState)
                        .build())
                .build();
        return new NoSuchStateException(gql, target);
    }

    public StateReference getTarget() {
        return this.target;
    }

    @Override
    public Status status() {
        return General.UnknownError;
    }

    @Override
    public String legacyMessage() {
        return oldMessage;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }
}

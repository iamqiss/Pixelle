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
package org.neo4j.internal.recordstorage;

import org.neo4j.internal.recordstorage.indexcommand.IndexUpdateCommand;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.storageengine.api.UpdateMode;

public enum CommandSelector {
    NORMAL {
        @Override
        <RECORD extends AbstractBaseRecord> RECORD getBefore(Command.BaseCommand<RECORD> command) {
            return command.before;
        }

        @Override
        <RECORD extends AbstractBaseRecord> RECORD getAfter(Command.BaseCommand<RECORD> command) {
            return command.after;
        }

        @Override
        <T> T getBefore(IndexUpdateCommand<T> command) {
            return command.getBefore();
        }

        @Override
        <T> T getAfter(IndexUpdateCommand<T> command) {
            return command.getAfter();
        }

        @Override
        <T> UpdateMode mode(IndexUpdateCommand<T> command) {
            return command.getUpdateMode();
        }
    },
    REVERSE {
        @Override
        <RECORD extends AbstractBaseRecord> RECORD getBefore(Command.BaseCommand<RECORD> command) {
            return command.after;
        }

        @Override
        <RECORD extends AbstractBaseRecord> RECORD getAfter(Command.BaseCommand<RECORD> command) {
            return command.before;
        }

        @Override
        <T> T getBefore(IndexUpdateCommand<T> command) {
            return command.getAfter();
        }

        @Override
        <T> T getAfter(IndexUpdateCommand<T> command) {
            return command.getBefore();
        }

        @Override
        <T> UpdateMode mode(IndexUpdateCommand<T> command) {
            return switch (command.getUpdateMode()) {
                case ADDED -> UpdateMode.REMOVED;
                case REMOVED -> UpdateMode.ADDED;
                case CHANGED -> UpdateMode.CHANGED;
            };
        }
    };

    abstract <RECORD extends AbstractBaseRecord> RECORD getBefore(Command.BaseCommand<RECORD> command);

    abstract <RECORD extends AbstractBaseRecord> RECORD getAfter(Command.BaseCommand<RECORD> command);

    abstract <T> T getBefore(IndexUpdateCommand<T> command);

    abstract <T> T getAfter(IndexUpdateCommand<T> command);

    abstract <T> UpdateMode mode(IndexUpdateCommand<T> command);
}

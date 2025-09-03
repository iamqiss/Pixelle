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
package org.neo4j.dbms.systemgraph.allocation;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

public record DatabaseAllocationHints(Set<Hint<?>> hints) {
    public static DatabaseAllocationHints EMPTY = new DatabaseAllocationHints(Set.of());

    public static DatabaseAllocationHints DEFAULT =
            new DatabaseAllocationHints(Arrays.stream(Hint.class.getPermittedSubclasses())
                    .filter(hint -> defaultValue(hint).isPresent())
                    .map(hintClass -> defaultValue(hintClass).orElseThrow())
                    .collect(Collectors.toSet()));

    public static Set<String> VALID_HINT_KEYS = Arrays.stream(Hint.class.getPermittedSubclasses())
            .map(DatabaseAllocationHints::key)
            .collect(Collectors.toSet());

    public static DatabaseAllocationHints createFromInput(MapValue providedHints) {
        var hints = new HashSet<Hint<?>>();
        providedHints.foreach((k, v) -> hints.add(createFromInput(k, v)));

        if (hints.isEmpty()) {
            return EMPTY;
        }
        return new DatabaseAllocationHints(Set.copyOf(hints));
    }

    public static DatabaseAllocationHints createFromAllocationHintsNode(Node allocationHintsNode) {
        assertLabel(allocationHintsNode, TopologyGraphDbmsModel.ALLOCATION_HINTS_LABEL);
        return createFromNode(allocationHintsNode, "").orElse(EMPTY);
    }

    public static DatabaseAllocationHints createFromSettingsNode(Node settingsNode) {
        assertLabel(settingsNode, TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_LABEL);
        return createFromNode(settingsNode, TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_DEFAULT_ALLOCATION_HINT_PREFIX)
                .orElse(DEFAULT);
    }

    private static Optional<DatabaseAllocationHints> createFromNode(Node node, String prefix) {
        Set<Hint<?>> hints = node.getAllProperties().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(entry -> Map.entry(entry.getKey().substring(prefix.length()), entry.getValue()))
                .filter(entry -> VALID_HINT_KEYS.contains(entry.getKey()))
                .map(entry -> createFromNodeProperty(entry.getKey(), entry.getValue()))
                .collect(Collectors.toUnmodifiableSet());
        if (hints.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new DatabaseAllocationHints(hints));
    }

    /**
     * Throws {@link IllegalArgumentException} if the provided key and value do not combine to form a valid {@link Hint}. Otherwise, does nothing.
     *
     * @param key   a string key provided within the allocationHints OPTION for CREATE and ALTER DATABASE commands
     * @param value a Cypher value provided within the allocationHints OPTION for CREATE and ALTER DATABASE commands
     */
    public static void validate(String key, AnyValue value) {
        createFromInput(key, value);
    }

    @VisibleForTesting
    static Hint<?> createFromInput(String key, AnyValue value) {
        switch (key) {
            case DatabaseWeight.KEY:
                if (value instanceof IntegralValue v) {
                    return new DatabaseWeight((int) v.longValue());
                } else {
                    throw new IllegalArgumentException(String.format(
                            "Incorrect value type provided for allocation hint '%s'. Expected an Integer but found a %s.",
                            DatabaseWeight.KEY, value.getTypeName()));
                }
            default:
                var validKeys = VALID_HINT_KEYS.stream().collect(Collectors.joining(", ", "'", "'"));
                throw new IllegalArgumentException(String.format(
                        "The key %s is not a recognised allocation hint key! Valid hint keys are: %s", key, validKeys));
        }
    }

    private static Hint<?> createFromNodeProperty(String key, Object value) {
        switch (key) {
            case DatabaseWeight.KEY:
                if (value instanceof Integer v) {
                    return new DatabaseWeight(v);
                } else {
                    throw new IllegalArgumentException(String.format(
                            "Incorrect value type provided for allocation hint '%s'. Expected an Integer but found a %s.",
                            DatabaseWeight.KEY, value.getClass().getSimpleName()));
                }
            default:
                var validKeys = VALID_HINT_KEYS.stream().collect(Collectors.joining(", ", "'", "'"));
                throw new IllegalArgumentException(String.format(
                        "The key %s is not a recognised allocation hint key! Valid hint keys are: %s", key, validKeys));
        }
    }

    /**
     * This method primarily exists to populate the {@link DatabaseAllocationHints#VALID_HINT_KEYS} constant, and is package private rather than private to
     * allow a test to enforce that all permissible subclasses of {@link Hint} define a unique string key.
     *
     * @param hintClass the class tag for the given hint
     * @return the string key for the given hint class. If no key is defined, or if the class is not a hint {@link IllegalArgumentException} is thrown
     */
    @VisibleForTesting
    static String key(Class<?> hintClass) {
        assertClass(hintClass);
        if (hintClass == DatabaseWeight.class) {
            return DatabaseWeight.KEY;
        } else {
            throw new IllegalArgumentException("Unexpected hint type with unknown key: " + hintClass.getSimpleName());
        }
    }

    @VisibleForTesting
    static Optional<Hint<?>> defaultValue(Class<?> hintClass) {
        assertClass(hintClass);
        if (hintClass == DatabaseWeight.class) {
            return Optional.of(DatabaseWeight.DEFAULT_WEIGHT);
        } else {
            throw new IllegalArgumentException("Unexpected hint type with unknown key: " + hintClass.getSimpleName());
        }
    }

    private static void assertClass(Class<?> hintClass) {
        if (!Hint.class.isAssignableFrom(hintClass)) {
            throw new IllegalArgumentException(String.format(
                    "Class does not implement %s: %s", Hint.class.getSimpleName(), hintClass.getSimpleName()));
        }
    }

    private static void assertLabel(Node node, Label label) {
        var labels = Iterables.asSet(node.getLabels());
        var labelString = labels.stream().map(Label::name).collect(Collectors.joining(", ", "'", "'"));
        if (!labels.contains(label)) {
            throw new IllegalArgumentException(String.format(
                    "Incorrect Node labels for creating a DatabaseAllocationHints object! Required label %s, found %s",
                    label.name(), labelString));
        }
    }

    public <U, T extends Hint<U>> Optional<U> hintValue(Class<T> clazz) {
        return hints.stream()
                .filter(hint -> hint.getClass().equals(clazz))
                .map(clazz::cast)
                .map(Hint::getValue)
                .findFirst();
    }

    public MapValue toMapValue() {
        if (hints.isEmpty()) {
            return MapValue.EMPTY;
        }
        var hintsMapBuilder = new MapValueBuilder(hints.size());
        hints.forEach(h -> hintsMapBuilder.add(h.getKey(), Values.of(h.getValue())));
        return hintsMapBuilder.build();
    }

    public void writeToAllocationHintsNode(Node allocationHintsNode) {
        assertLabel(allocationHintsNode, TopologyGraphDbmsModel.ALLOCATION_HINTS_LABEL);
        writeToNode(allocationHintsNode, "");
    }

    public void writeToSettingsNode(Node settingsNode) {
        assertLabel(settingsNode, TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_LABEL);
        writeToNode(settingsNode, TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_DEFAULT_ALLOCATION_HINT_PREFIX);
    }

    private void writeToNode(Node node, String prefix) {
        hints.forEach(hint -> {
            var key = prefix + hint.getKey();
            if (!node.hasProperty(key)) {
                node.setProperty(key, hint.getValue());
            }
        });
    }

    public sealed interface Hint<T> permits DatabaseWeight {
        String getKey();

        T getValue();
    }
}

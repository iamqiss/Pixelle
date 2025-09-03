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
package org.neo4j.procedure.impl;

import static java.lang.reflect.Modifier.isPublic;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.ComponentInjectionException;
import org.neo4j.procedure.Context;

/**
 * Injects annotated fields with appropriate values.
 */
class FieldInjections {
    private final ComponentRegistry components;

    FieldInjections(ComponentRegistry components) {
        this.components = components;
    }

    /**
     * For each annotated field in the provided class, creates a `FieldSetter`.
     * @param cls The class where injection should happen.
     * @return A list of `FieldSetters`
     * @throws ProcedureException if the type of the injected field does not match what has been registered.
     */
    List<FieldSetter> setters(Class<?> cls) throws ProcedureException {
        List<FieldSetter> setters = new LinkedList<>();
        Class<?> currentClass = cls;

        do {
            for (Field field : currentClass.getDeclaredFields()) {
                // ignore synthetic fields
                if (field.isSynthetic()) {
                    continue;
                }
                if (Modifier.isStatic(field.getModifiers())) {
                    if (field.isAnnotationPresent(Context.class)) {
                        throw ProcedureException.faultyClassFieldAnnotationStatic(field.getName(), cls.getSimpleName());
                    }
                    continue;
                }

                assertValidForInjection(cls, field);
                setters.add(createInjector(cls, field));
            }
        } while ((currentClass = currentClass.getSuperclass()) != null);

        return setters;
    }

    private FieldSetter createInjector(Class<?> cls, Field field) throws ProcedureException {

        ThrowingFunction<org.neo4j.kernel.api.procedure.Context, ?, ProcedureException> provider =
                components.providerFor(field.getType());
        if (provider == null) {
            throw ComponentInjectionException.unsupportedInjectableComponentType(
                    cls.getSimpleName(), field.getName(), String.valueOf(field.getType()));
        }
        if (!isPublic(field.getModifiers())) {
            throw ProcedureException.unableToAccessFieldInjection(cls.getSimpleName(), field.getName());
        }

        return new FieldSetter(field, provider);
    }

    private static void assertValidForInjection(Class<?> cls, Field field) throws ProcedureException {
        if (!field.isAnnotationPresent(Context.class)) {
            throw ProcedureException.missingClassFieldAnnotation(cls.getSimpleName(), field.getName());
        }

        if (!isPublic(field.getModifiers()) || Modifier.isFinal(field.getModifiers())) {
            throw ProcedureException.faultyClassFieldAnnotation(cls.getSimpleName(), field.getName());
        }
    }
}

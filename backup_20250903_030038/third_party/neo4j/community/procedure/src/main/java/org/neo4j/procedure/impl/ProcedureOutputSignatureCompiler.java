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
import static java.lang.reflect.Modifier.isStatic;
import static java.util.function.Predicate.not;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.procedure.Description;

/**
 * Given a procedure that outputs <tt>Stream<MyOut></tt> we look for all public fields
 * of <tt>MyOut</tt> to produce a signature.
 */
class ProcedureOutputSignatureCompiler {

    ProcedureOutputSignatureCompiler(Cypher5TypeCheckers typeCheckers) {
        this.typeCheckers = typeCheckers;
    }

    private final Cypher5TypeCheckers typeCheckers;

    /**
     * Determines the output fields of a given method.
     *
     * @param method the procedure method
     * @return an output mapper for the return type of the method.
     * @throws ProcedureException when the types are incorrect
     */
    List<FieldSignature> fieldSignatures(Method method) throws ProcedureException {
        Class<?> cls = method.getReturnType();
        if (cls == Void.class || cls == void.class) {
            return ProcedureSignature.VOID;
        }

        if (cls != Stream.class) {
            throw ProcedureException.invalidReturnTypeExtended(method.getName(), cls);
        }

        Type genericReturnType = method.getGenericReturnType();

        if (!(genericReturnType instanceof ParameterizedType genType)) {
            throw ProcedureException.invalidReturnType(method.getName(), "raw Stream");
        }

        Type recordType = genType.getActualTypeArguments()[0];
        if (recordType instanceof WildcardType) {
            throw ProcedureException.invalidReturnType(method.getName(), "Stream<?>");
        }
        if (recordType instanceof ParameterizedType type) {
            throw ProcedureException.invalidReturnType(method.getName(), "parameterized type such as " + type);
        }

        return fieldSignatures(method.getName(), (Class<?>) recordType);
    }

    List<FieldSignature> fieldSignatures(String methodName, Class<?> userClass) throws ProcedureException {
        assertIsValidRecordClass(methodName, userClass);

        List<Field> fields = instanceFields(userClass);
        FieldSignature[] signature = new FieldSignature[fields.size()];

        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (!userClass.isRecord() && !isPublic(field.getModifiers())) {
                throw ProcedureException.unableToAccessField(userClass.getSimpleName(), field.getName());
            }

            try {
                Cypher5TypeCheckers.TypeChecker checker = typeCheckers.checkerFor(field.getGenericType());
                if (field.isAnnotationPresent(Description.class)) {
                    String description = field.getAnnotation(Description.class).value();
                    signature[i] = FieldSignature.outputField(
                            field.getName(), checker.type(), field.isAnnotationPresent(Deprecated.class), description);
                } else {
                    signature[i] = FieldSignature.outputField(
                            field.getName(), checker.type(), field.isAnnotationPresent(Deprecated.class));
                }

            } catch (ProcedureException e) {
                throw new ProcedureException(
                        e.status(),
                        e,
                        "Field `%s` in record `%s` cannot be converted to a Neo4j type: %s",
                        field.getName(),
                        userClass.getSimpleName(),
                        e.getMessage());
            }
        }

        return Arrays.asList(signature);
    }

    private void assertIsValidRecordClass(String methodName, Class<?> userClass) throws ProcedureException {
        if (userClass.isPrimitive()
                || userClass.isArray()
                || userClass.getPackage() != null
                        && userClass.getPackage().getName().startsWith("java.")) {
            throw ProcedureException.invalidReturnTypeExtended(methodName, userClass);
        }
    }

    static List<Field> instanceFields(Class<?> userClass) {
        return Stream.<Class<?>>iterate(
                        userClass, not(ProcedureOutputSignatureCompiler::isJavaLangClass), Class::getSuperclass)
                .flatMap(c -> Arrays.stream(c.getDeclaredFields()))
                .filter(f -> !isStatic(f.getModifiers()) && !f.isSynthetic())
                .toList();
    }

    private static boolean isJavaLangClass(Class<?> cls) {
        return cls.getPackage().getName().equals("java.lang");
    }
}

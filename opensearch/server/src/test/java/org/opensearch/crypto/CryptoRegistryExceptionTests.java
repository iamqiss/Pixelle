/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.crypto;

import org.density.core.common.io.stream.InputStreamStreamInput;
import org.density.core.common.io.stream.OutputStreamStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.rest.RestStatus;
import org.density.test.DensityTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CryptoRegistryExceptionTests extends DensityTestCase {

    public void testConstructorWithClientNameAndType() {
        String clientName = "test-client";
        String clientType = "test-type";
        CryptoRegistryException exception = new CryptoRegistryException(clientName, clientType);

        assertEquals(RestStatus.NOT_FOUND, exception.status());
        assertEquals(clientName, exception.getName());
        assertEquals(clientType, exception.getType());
    }

    public void testConstructorWithClientNameTypeAndCause() {
        String clientName = "test-client";
        String clientType = "test-type";
        String causeMessage = "Something went wrong.";
        Throwable cause = new Throwable(causeMessage);
        CryptoRegistryException exception = new CryptoRegistryException(clientName, clientType, cause);

        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, exception.status());
        assertEquals(clientName, exception.getName());
        assertEquals(clientType, exception.getType());
        assertEquals(cause, exception.getCause());
    }

    public void testConstructorWithClientNameTypeAndIllegalArgsCause() {
        String clientName = "test-client";
        String clientType = "test-type";
        String causeMessage = "Bad arguments.";
        IllegalArgumentException cause = new IllegalArgumentException(causeMessage);
        ;
        CryptoRegistryException exception = new CryptoRegistryException(clientName, clientType, cause);

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertEquals(clientName, exception.getName());
        assertEquals(clientType, exception.getType());
        assertEquals(cause, exception.getCause());
    }

    public void testConstructorWithClientNameTypeAndCustomMessage() {
        String clientName = "TestClient";
        String clientType = "TestType";
        String customMessage = "Invalid client data.";
        CryptoRegistryException exception = new CryptoRegistryException(clientName, clientType, customMessage);

        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, exception.status());
        assertEquals(clientName, exception.getName());
        assertEquals(clientType, exception.getType());
    }

    public void testSerializationAndDeserialization() throws IOException {
        String clientName = "TestClient";
        String clientType = "TestType";
        CryptoRegistryException originalException = new CryptoRegistryException(clientName, clientType);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
        originalException.writeTo(streamOutput);

        byte[] byteArray = outputStream.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArray);
        StreamInput streamInput = new InputStreamStreamInput(inputStream);
        CryptoRegistryException deserializedException = new CryptoRegistryException(streamInput);

        assertEquals(originalException.getMessage(), deserializedException.getMessage());
        assertEquals(originalException.status(), deserializedException.status());
        assertEquals(originalException.getName(), deserializedException.getName());
        assertEquals(originalException.getType(), deserializedException.getType());
    }
}

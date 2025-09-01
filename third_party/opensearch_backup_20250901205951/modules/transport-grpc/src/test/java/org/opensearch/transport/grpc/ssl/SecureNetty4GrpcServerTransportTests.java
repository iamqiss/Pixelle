/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.ssl;

import org.density.common.network.NetworkService;
import org.density.common.settings.Settings;
import org.density.core.common.transport.TransportAddress;
import org.density.test.DensityTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.grpc.BindableService;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckResponse;

import static org.density.transport.grpc.ssl.SecureSettingsHelpers.ConnectExceptions.BAD_CERT;
import static org.density.transport.grpc.ssl.SecureSettingsHelpers.getServerClientAuthNone;
import static org.density.transport.grpc.ssl.SecureSettingsHelpers.getServerClientAuthOptional;
import static org.density.transport.grpc.ssl.SecureSettingsHelpers.getServerClientAuthRequired;

public class SecureNetty4GrpcServerTransportTests extends DensityTestCase {
    private NetworkService networkService;
    private final List<BindableService> services = new ArrayList<>();

    static Settings createSettings() {
        return Settings.builder().put(SecureNetty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), getPortRange()).build();
    }

    @Before
    public void setup() {
        networkService = new NetworkService(Collections.emptyList());
    }

    @After
    public void shutdown() {
        networkService = null;
    }

    public void testGrpcSecureTransportStartStop() {
        try (
            SecureNetty4GrpcServerTransport transport = new SecureNetty4GrpcServerTransport(
                createSettings(),
                services,
                networkService,
                getServerClientAuthNone()
            )
        ) {
            transport.start();
            assertTrue(transport.getBoundAddress().boundAddresses().length > 0);
            assertNotNull(transport.getBoundAddress().publishAddress().address());
            transport.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testGrpcInsecureAuthTLS() {
        try (
            SecureNetty4GrpcServerTransport transport = new SecureNetty4GrpcServerTransport(
                createSettings(),
                services,
                networkService,
                getServerClientAuthNone()
            )
        ) {
            transport.start();
            assertTrue(transport.getBoundAddress().boundAddresses().length > 0);
            assertNotNull(transport.getBoundAddress().publishAddress().address());
            final TransportAddress remoteAddress = randomFrom(transport.getBoundAddress().boundAddresses());

            // Client without cert
            NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(remoteAddress).insecure(true).build();
            assertEquals(client.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
            client.close();

            transport.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testGrpcOptionalAuthTLS() {
        try (
            SecureNetty4GrpcServerTransport transport = new SecureNetty4GrpcServerTransport(
                createSettings(),
                services,
                networkService,
                getServerClientAuthOptional()
            )
        ) {
            transport.start();
            assertTrue(transport.getBoundAddress().boundAddresses().length > 0);
            assertNotNull(transport.getBoundAddress().publishAddress().address());
            final TransportAddress remoteAddress = randomFrom(transport.getBoundAddress().boundAddresses());

            // Client without cert
            NettyGrpcClient hasNoCertClient = new NettyGrpcClient.Builder().setAddress(remoteAddress).insecure(true).build();
            assertEquals(hasNoCertClient.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
            hasNoCertClient.close();

            // Client with trusted cert
            NettyGrpcClient hasTrustedCertClient = new NettyGrpcClient.Builder().setAddress(remoteAddress).clientAuth(true).build();
            assertEquals(hasTrustedCertClient.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
            hasTrustedCertClient.close();

            transport.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testGrpcRequiredAuthTLS() {
        try (
            SecureNetty4GrpcServerTransport transport = new SecureNetty4GrpcServerTransport(
                createSettings(),
                services,
                networkService,
                getServerClientAuthRequired()
            )
        ) {
            transport.start();
            assertTrue(transport.getBoundAddress().boundAddresses().length > 0);
            assertNotNull(transport.getBoundAddress().publishAddress().address());
            final TransportAddress remoteAddress = randomFrom(transport.getBoundAddress().boundAddresses());

            // Client without cert
            NettyGrpcClient hasNoCertClient = new NettyGrpcClient.Builder().setAddress(remoteAddress).insecure(true).build();
            assertThrows(StatusRuntimeException.class, hasNoCertClient::checkHealth);
            try {
                hasNoCertClient.checkHealth();
            } catch (Exception e) {
                assertEquals(SecureSettingsHelpers.ConnectExceptions.get(e), BAD_CERT);
            }
            hasNoCertClient.close();

            // Client with trusted cert
            NettyGrpcClient hasTrustedCertClient = new NettyGrpcClient.Builder().setAddress(remoteAddress).clientAuth(true).build();
            assertEquals(hasTrustedCertClient.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
            hasTrustedCertClient.close();

            transport.stop();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}

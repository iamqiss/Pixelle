/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.ssl;

import org.density.action.admin.cluster.health.ClusterHealthResponse;
import org.density.cluster.health.ClusterHealthStatus;
import org.density.common.settings.Settings;
import org.density.core.common.transport.TransportAddress;
import org.density.plugins.NetworkPlugin;
import org.density.plugins.Plugin;
import org.density.plugins.SecureAuxTransportSettingsProvider;
import org.density.plugins.SecureHttpTransportSettingsProvider;
import org.density.plugins.SecureSettingsFactory;
import org.density.plugins.SecureTransportSettingsProvider;
import org.density.test.DensityIntegTestCase;
import org.density.transport.grpc.GrpcPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import io.grpc.health.v1.HealthCheckResponse;

import static org.density.transport.AuxTransport.AUX_TRANSPORT_TYPES_KEY;
import static org.density.transport.grpc.ssl.SecureNetty4GrpcServerTransport.GRPC_SECURE_TRANSPORT_SETTING_KEY;
import static org.density.transport.grpc.ssl.SecureSettingsHelpers.getServerClientAuthNone;
import static org.density.transport.grpc.ssl.SecureSettingsHelpers.getServerClientAuthOptional;
import static org.density.transport.grpc.ssl.SecureSettingsHelpers.getServerClientAuthRequired;

public abstract class SecureNetty4GrpcServerTransportIT extends DensityIntegTestCase {

    public static class MockSecurityPlugin extends Plugin implements NetworkPlugin {
        public MockSecurityPlugin() {}

        static class MockSecureSettingsFactory implements SecureSettingsFactory {
            @Override
            public Optional<SecureTransportSettingsProvider> getSecureTransportSettingsProvider(Settings settings) {
                return Optional.empty();
            }

            @Override
            public Optional<SecureHttpTransportSettingsProvider> getSecureHttpTransportSettingsProvider(Settings settings) {
                return Optional.empty();
            }

            @Override
            public Optional<SecureAuxTransportSettingsProvider> getSecureAuxTransportSettingsProvider(Settings settings) {
                return Optional.empty();
            }
        }
    }

    protected TransportAddress randomNetty4GrpcServerTransportAddr() {
        List<TransportAddress> addresses = new ArrayList<>();
        for (SecureNetty4GrpcServerTransport transport : internalCluster().getInstances(SecureNetty4GrpcServerTransport.class)) {
            TransportAddress tAddr = new TransportAddress(transport.getBoundAddress().publishAddress().address());
            addresses.add(tAddr);
        }
        return randomFrom(addresses);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(AUX_TRANSPORT_TYPES_KEY, GRPC_SECURE_TRANSPORT_SETTING_KEY)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GrpcPlugin.class, MockSecurityPlugin.class);
    }

    private SecureSettingsHelpers.ConnectExceptions tryConnectClient(NettyGrpcClient client) {
        try {
            HealthCheckResponse.ServingStatus status = client.checkHealth();
            if (status == HealthCheckResponse.ServingStatus.SERVING) {
                return SecureSettingsHelpers.ConnectExceptions.NONE;
            } else {
                throw new RuntimeException("Illegal state - unexpected server status: " + status.toString());
            }
        } catch (Exception e) {
            return SecureSettingsHelpers.ConnectExceptions.get(e);
        }
    }

    protected SecureSettingsHelpers.ConnectExceptions plaintextClientConnect() throws Exception {
        try (NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(randomNetty4GrpcServerTransportAddr()).build()) {
            return tryConnectClient(client);
        }
    }

    protected SecureSettingsHelpers.ConnectExceptions insecureClientConnect() throws Exception {
        try (
            NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(randomNetty4GrpcServerTransportAddr()).insecure(true).build()
        ) {
            return tryConnectClient(client);
        }
    }

    protected SecureSettingsHelpers.ConnectExceptions trustedCertClientConnect() throws Exception {
        try (
            NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(randomNetty4GrpcServerTransportAddr())
                .clientAuth(true)
                .build()
        ) {
            return tryConnectClient(client);
        }
    }

    public void testClusterHealth() {
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().get();
        assertEquals(ClusterHealthStatus.GREEN, healthResponse.getStatus());
    }

    public static class SecureNetty4GrpcServerTransportNoAuthIT extends SecureNetty4GrpcServerTransportIT {
        public static class NoAuthMockSecurityPlugin extends MockSecurityPlugin {
            public NoAuthMockSecurityPlugin() {}

            @Override
            public Optional<SecureSettingsFactory> getSecureSettingFactory(Settings settings) {
                return Optional.of(new MockSecureSettingsFactory() {
                    @Override
                    public Optional<SecureAuxTransportSettingsProvider> getSecureAuxTransportSettingsProvider(Settings settings) {
                        return Optional.of(getServerClientAuthNone());
                    }
                });
            }
        }

        @Override
        protected Collection<Class<? extends Plugin>> nodePlugins() {
            return List.of(GrpcPlugin.class, NoAuthMockSecurityPlugin.class);
        }

        public void testPlaintextClientConnect() throws Exception {
            assertEquals(plaintextClientConnect(), SecureSettingsHelpers.ConnectExceptions.UNAVAILABLE);
        }

        public void testInsecureClientConnect() throws Exception {
            assertEquals(insecureClientConnect(), SecureSettingsHelpers.ConnectExceptions.NONE);
        }

        public void testTrustedClientConnect() throws Exception {
            assertEquals(trustedCertClientConnect(), SecureSettingsHelpers.ConnectExceptions.NONE);
        }
    }

    public static class SecureNetty4GrpcServerTransportOptionalAuthIT extends SecureNetty4GrpcServerTransportIT {
        public static class OptAuthMockSecurityPlugin extends MockSecurityPlugin {
            public OptAuthMockSecurityPlugin() {}

            @Override
            public Optional<SecureSettingsFactory> getSecureSettingFactory(Settings settings) {
                return Optional.of(new MockSecureSettingsFactory() {
                    @Override
                    public Optional<SecureAuxTransportSettingsProvider> getSecureAuxTransportSettingsProvider(Settings settings) {
                        return Optional.of(getServerClientAuthOptional());
                    }
                });
            }
        }

        @Override
        protected Collection<Class<? extends Plugin>> nodePlugins() {
            return List.of(GrpcPlugin.class, OptAuthMockSecurityPlugin.class);
        }

        public void testPlaintextClientConnect() throws Exception {
            assertEquals(plaintextClientConnect(), SecureSettingsHelpers.ConnectExceptions.UNAVAILABLE);
        }

        public void testInsecureClientConnect() throws Exception {
            assertEquals(insecureClientConnect(), SecureSettingsHelpers.ConnectExceptions.NONE);
        }

        public void testTrustedClientConnect() throws Exception {
            assertEquals(trustedCertClientConnect(), SecureSettingsHelpers.ConnectExceptions.NONE);
        }
    }

    public static class SecureNetty4GrpcServerTransportRequiredAuthIT extends SecureNetty4GrpcServerTransportIT {
        public static class RequireAuthMockSecurityPlugin extends MockSecurityPlugin {
            public RequireAuthMockSecurityPlugin() {}

            @Override
            public Optional<SecureSettingsFactory> getSecureSettingFactory(Settings settings) {
                return Optional.of(new MockSecureSettingsFactory() {
                    @Override
                    public Optional<SecureAuxTransportSettingsProvider> getSecureAuxTransportSettingsProvider(Settings settings) {
                        return Optional.of(getServerClientAuthRequired());
                    }
                });
            }
        }

        @Override
        protected Collection<Class<? extends Plugin>> nodePlugins() {
            return List.of(GrpcPlugin.class, RequireAuthMockSecurityPlugin.class);
        }

        public void testPlaintextClientConnect() throws Exception {
            assertEquals(plaintextClientConnect(), SecureSettingsHelpers.ConnectExceptions.UNAVAILABLE);
        }

        public void testInsecureClientConnect() throws Exception {
            assertEquals(insecureClientConnect(), SecureSettingsHelpers.ConnectExceptions.BAD_CERT);
        }

        public void testTrustedClientConnect() throws Exception {
            assertEquals(trustedCertClientConnect(), SecureSettingsHelpers.ConnectExceptions.NONE);
        }
    }
}

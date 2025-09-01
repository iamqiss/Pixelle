/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.transport;

import org.density.arrow.flight.api.flightinfo.FlightServerInfoAction;
import org.density.arrow.flight.api.flightinfo.NodesFlightInfoAction;
import org.density.arrow.flight.bootstrap.FlightService;
import org.density.arrow.flight.stats.FlightStatsAction;
import org.density.arrow.flight.stats.FlightStatsRestHandler;
import org.density.arrow.spi.StreamManager;
import org.density.cluster.ClusterState;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.service.ClusterService;
import org.density.common.network.NetworkService;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.plugins.SecureTransportSettingsProvider;
import org.density.test.DensityTestCase;
import org.density.threadpool.ExecutorBuilder;
import org.density.threadpool.ThreadPool;
import org.density.transport.AuxTransport;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.density.arrow.flight.bootstrap.FlightService.ARROW_FLIGHT_TRANSPORT_SETTING_KEY;
import static org.density.common.util.FeatureFlags.ARROW_STREAMS;
import static org.density.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlightStreamPluginTests extends DensityTestCase {
    private Settings settings;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put("flight.ssl.enable", true).build();
        clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.nodes()).thenReturn(nodes);
        when(nodes.getLocalNodeId()).thenReturn("test-node");
    }

    @LockFeatureFlag(ARROW_STREAMS)
    public void testPluginEnabledWithStreamManagerApproach() throws IOException {
        FlightStreamPlugin plugin = new FlightStreamPlugin(settings);
        plugin.createComponents(null, clusterService, mock(ThreadPool.class), null, null, null, null, null, null, null, null);
        Map<String, Supplier<AuxTransport>> aux_map = plugin.getAuxTransports(
            settings,
            mock(ThreadPool.class),
            null,
            new NetworkService(List.of()),
            null,
            null
        );

        AuxTransport transport = aux_map.get(ARROW_FLIGHT_TRANSPORT_SETTING_KEY).get();
        assertNotNull(transport);
        assertTrue(transport instanceof FlightService);

        List<ExecutorBuilder<?>> executorBuilders = plugin.getExecutorBuilders(settings);
        assertNotNull(executorBuilders);
        assertFalse(executorBuilders.isEmpty());
        assertEquals(3, executorBuilders.size());

        Optional<StreamManager> streamManager = plugin.getStreamManager();
        assertTrue(streamManager.isPresent());

        List<Setting<?>> settings = plugin.getSettings();
        assertNotNull(settings);
        assertFalse(settings.isEmpty());

        assertTrue(
            plugin.getAuxTransports(null, null, null, new NetworkService(List.of()), null, null)
                .get(ARROW_FLIGHT_TRANSPORT_SETTING_KEY)
                .get() instanceof FlightService
        );
        assertEquals(1, plugin.getRestHandlers(null, null, null, null, null, null, null).size());
        assertTrue(plugin.getRestHandlers(null, null, null, null, null, null, null).get(0) instanceof FlightServerInfoAction);

        assertEquals(1, plugin.getActions().size());
        assertEquals(NodesFlightInfoAction.INSTANCE.name(), plugin.getActions().get(0).getAction().name());

        plugin.close();
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testPluginEnabledStreamTransportApproach() throws IOException {
        FlightStreamPlugin plugin = new FlightStreamPlugin(settings);
        plugin.createComponents(null, clusterService, mock(ThreadPool.class), null, null, null, null, null, null, null, null);
        List<ExecutorBuilder<?>> executorBuilders = plugin.getExecutorBuilders(settings);
        assertNotNull(executorBuilders);
        assertFalse(executorBuilders.isEmpty());
        assertEquals(3, executorBuilders.size());

        Optional<StreamManager> streamManager = plugin.getStreamManager();
        assertTrue(streamManager.isEmpty());

        List<Setting<?>> settings = plugin.getSettings();
        assertNotNull(settings);
        assertFalse(settings.isEmpty());

        assertFalse(
            plugin.getSecureTransports(null, null, null, null, null, null, mock(SecureTransportSettingsProvider.class), null).isEmpty()
        );

        assertEquals(1, plugin.getRestHandlers(null, null, null, null, null, null, null).size());
        assertTrue(plugin.getRestHandlers(null, null, null, null, null, null, null).get(0) instanceof FlightStatsRestHandler);

        assertEquals(1, plugin.getActions().size());
        assertEquals(FlightStatsAction.INSTANCE.name(), plugin.getActions().get(0).getAction().name());

        plugin.close();
    }

    public void testBothDisabled() throws IOException {
        FlightStreamPlugin plugin = new FlightStreamPlugin(settings);
        plugin.createComponents(null, clusterService, mock(ThreadPool.class), null, null, null, null, null, null, null, null);

        List<ExecutorBuilder<?>> executorBuilders = plugin.getExecutorBuilders(settings);
        assertTrue(executorBuilders.isEmpty());

        Optional<StreamManager> streamManager = plugin.getStreamManager();
        assertTrue(streamManager.isEmpty());

        List<Setting<?>> settings = plugin.getSettings();
        assertNotNull(settings);
        assertTrue(settings.isEmpty());

        assertTrue(
            plugin.getSecureTransports(null, null, null, null, null, null, mock(SecureTransportSettingsProvider.class), null).isEmpty()
        );

        assertEquals(0, plugin.getRestHandlers(null, null, null, null, null, null, null).size());

        assertEquals(0, plugin.getActions().size());
        plugin.close();
    }
}

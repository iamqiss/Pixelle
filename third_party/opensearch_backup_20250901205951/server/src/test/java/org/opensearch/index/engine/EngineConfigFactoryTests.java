/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.engine;

import org.apache.logging.log4j.LogManager;
import org.density.Version;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.unit.TimeValue;
import org.density.index.IndexSettings;
import org.density.index.codec.CodecService;
import org.density.index.codec.CodecServiceFactory;
import org.density.index.seqno.RetentionLeases;
import org.density.index.translog.InternalTranslogFactory;
import org.density.index.translog.TranslogDeletionPolicy;
import org.density.index.translog.TranslogDeletionPolicyFactory;
import org.density.index.translog.TranslogReader;
import org.density.index.translog.TranslogWriter;
import org.density.plugins.EnginePlugin;
import org.density.plugins.Plugin;
import org.density.test.IndexSettingsModule;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class EngineConfigFactoryTests extends DensityTestCase {
    public void testCreateEngineConfigFromFactory() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Collections.singletonList(new FooEnginePlugin());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);

        EngineConfig config = factory.newEngineConfig(
            null,
            null,
            indexSettings,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            TimeValue.timeValueMinutes(5),
            null,
            null,
            null,
            null,
            null,
            () -> new RetentionLeases(0, 0, Collections.emptyList()),
            null,
            null,
            false,
            () -> Boolean.TRUE,
            new InternalTranslogFactory(),
            null,
            null,
            null,
            null
        );

        assertNotNull(config.getCodec());
        assertNotNull(config.getCustomTranslogDeletionPolicyFactory());
        assertTrue(config.getCustomTranslogDeletionPolicyFactory().create(indexSettings, null) instanceof CustomTranslogDeletionPolicy);
    }

    public void testCreateEngineConfigFromFactoryMultipleCodecServiceIllegalStateException() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new FooEnginePlugin(), new BarEnginePlugin());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());

        expectThrows(IllegalStateException.class, () -> new EngineConfigFactory(plugins, indexSettings));
    }

    public void testCreateEngineConfigFromFactoryMultipleCodecServiceAndFactoryIllegalStateException() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new FooEnginePlugin(), new BakEnginePlugin());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());

        expectThrows(IllegalStateException.class, () -> new EngineConfigFactory(plugins, indexSettings));
    }

    public void testCreateEngineConfigFromFactoryMultipleCustomTranslogDeletionPolicyFactoryIllegalStateException() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new FooEnginePlugin(), new BazEnginePlugin());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());

        expectThrows(IllegalStateException.class, () -> new EngineConfigFactory(plugins, indexSettings));
    }

    public void testCreateCodecServiceFromFactory() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new BakEnginePlugin());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());

        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);
        EngineConfig config = factory.newEngineConfig(
            null,
            null,
            indexSettings,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            TimeValue.timeValueMinutes(5),
            null,
            null,
            null,
            null,
            null,
            () -> new RetentionLeases(0, 0, Collections.emptyList()),
            null,
            null,
            false,
            () -> Boolean.TRUE,
            new InternalTranslogFactory(),
            null,
            null,
            null,
            null
        );
        assertNotNull(config.getCodec());
    }

    public void testGetEngineFactory() {
        final EngineFactory engineFactory = config -> null;
        EnginePlugin enginePluginThatImplementsGetEngineFactory = new EnginePlugin() {
            @Override
            public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
                return Optional.of(engineFactory);
            }
        };
        assertEquals(engineFactory, enginePluginThatImplementsGetEngineFactory.getEngineFactory(null).orElse(null));

        EnginePlugin enginePluginThatDoesNotImplementsGetEngineFactory = new EnginePlugin() {
        };
        assertFalse(enginePluginThatDoesNotImplementsGetEngineFactory.getEngineFactory(null).isPresent());
    }

    private static class FooEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            return Optional.empty();
        }

        @Override
        public Optional<CodecService> getCustomCodecService(IndexSettings indexSettings) {
            return Optional.of(new CodecService(null, indexSettings, LogManager.getLogger(getClass())));
        }

        @Override
        public Optional<TranslogDeletionPolicyFactory> getCustomTranslogDeletionPolicyFactory() {
            return Optional.of(CustomTranslogDeletionPolicy::new);
        }
    }

    private static class BarEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            return Optional.empty();
        }

        @Override
        public Optional<CodecService> getCustomCodecService(IndexSettings indexSettings) {
            return Optional.of(new CodecService(null, indexSettings, LogManager.getLogger(getClass())));
        }
    }

    private static class BakEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            return Optional.empty();
        }

        @Override
        public Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
            return Optional.of(
                config -> new CodecService(config.getMapperService(), config.getIndexSettings(), LogManager.getLogger(getClass()))
            );
        }
    }

    private static class BazEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            return Optional.empty();
        }

        @Override
        public Optional<TranslogDeletionPolicyFactory> getCustomTranslogDeletionPolicyFactory() {
            return Optional.of(CustomTranslogDeletionPolicy::new);
        }
    }

    private static class CustomTranslogDeletionPolicy extends TranslogDeletionPolicy {
        public CustomTranslogDeletionPolicy(IndexSettings indexSettings, Supplier<RetentionLeases> retentionLeasesSupplier) {
            super();
        }

        @Override
        public void setRetentionSizeInBytes(long bytes) {

        }

        @Override
        public void setRetentionAgeInMillis(long ageInMillis) {

        }

        @Override
        protected void setRetentionTotalFiles(int retentionTotalFiles) {

        }

        @Override
        public long minTranslogGenRequired(List<TranslogReader> readers, TranslogWriter writer) throws IOException {
            return 0;
        }
    }
}

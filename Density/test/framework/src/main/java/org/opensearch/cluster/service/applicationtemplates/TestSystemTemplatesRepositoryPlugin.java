/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.service.applicationtemplates;

import org.density.cluster.applicationtemplates.SystemTemplate;
import org.density.cluster.applicationtemplates.SystemTemplateLoader;
import org.density.cluster.applicationtemplates.SystemTemplateMetadata;
import org.density.cluster.applicationtemplates.SystemTemplateRepository;
import org.density.cluster.applicationtemplates.SystemTemplatesPlugin;
import org.density.cluster.applicationtemplates.TemplateRepositoryMetadata;
import org.density.core.common.bytes.BytesReference;
import org.density.plugins.Plugin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TestSystemTemplatesRepositoryPlugin extends Plugin implements SystemTemplatesPlugin {

    private final SystemTemplateMetadata templateMetadata = SystemTemplateMetadata.fromComponentTemplateInfo("dummy", 1);

    private final TemplateRepositoryMetadata repoMetadata = new TemplateRepositoryMetadata("test", 1);

    private final SystemTemplate systemTemplate = new SystemTemplate(
        BytesReference.fromByteBuffer(ByteBuffer.wrap("content".getBytes(StandardCharsets.UTF_8))),
        templateMetadata,
        repoMetadata
    );

    @Override
    public SystemTemplateRepository loadRepository() throws IOException {
        return new SystemTemplateRepository() {
            @Override
            public TemplateRepositoryMetadata metadata() {
                return repoMetadata;
            }

            @Override
            public List<SystemTemplateMetadata> listTemplates() throws IOException {
                return List.of(templateMetadata);
            }

            @Override
            public SystemTemplate getTemplate(SystemTemplateMetadata template) throws IOException {
                return systemTemplate;
            }

            @Override
            public void close() throws Exception {}
        };
    }

    @Override
    public SystemTemplateLoader loaderFor(SystemTemplateMetadata templateMetadata) {
        return new SystemTemplateLoader() { // Asserting Loader
            @Override
            public boolean loadTemplate(SystemTemplate template) throws IOException {
                assert template.templateMetadata() == TestSystemTemplatesRepositoryPlugin.this.templateMetadata;
                assert template.repositoryMetadata() == repoMetadata;
                assert template.templateContent() == systemTemplate.templateContent();
                return true;
            }
        };
    }
}

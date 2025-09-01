/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.gradle;

import org.density.gradle.DensityDistribution.Platform;
import org.density.gradle.DensityDistribution.Type;
import org.density.gradle.docker.DockerSupportPlugin;
import org.density.gradle.docker.DockerSupportService;
import org.density.gradle.transform.SymbolicLinkPreservingUntarTransform;
import org.density.gradle.transform.UnzipTransform;
import org.density.gradle.util.GradleUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.internal.artifacts.ArtifactAttributes;
import org.gradle.api.provider.Provider;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

/**
 * A plugin to manage getting and extracting distributions of Density.
 * <p>
 * The plugin provides hooks to register custom distribution resolutions.
 * This plugin resolves distributions from the Density downloads service if
 * no registered resolution strategy can resolve to a distribution.
 */
public class DistributionDownloadPlugin implements Plugin<Project> {

    static final String RESOLUTION_CONTAINER_NAME = "density_distributions_resolutions";
    private static final String CONTAINER_NAME = "density_distributions";
    private static final String FAKE_IVY_GROUP = "density-distribution";
    private static final String FAKE_SNAPSHOT_IVY_GROUP = "density-distribution-snapshot";
    private static final String DOWNLOAD_REPO_NAME = "density-downloads";
    private static final String SNAPSHOT_REPO_NAME = "density-snapshots";
    public static final String DISTRO_EXTRACTED_CONFIG_PREFIX = "density_distro_extracted_";

    private static final String RELEASE_PATTERN_LAYOUT = "/core/density/[revision]/[module]-min-[revision](-[classifier]).[ext]";
    private static final String SNAPSHOT_PATTERN_LAYOUT =
        "/snapshots/core/density/[revision]/[module]-min-[revision](-[classifier])-latest.[ext]";
    private static final String BUNDLE_PATTERN_LAYOUT =
        "/ci/dbc/distribution-build-density/[revision]/latest/linux/x64/tar/dist/density/[module]-[revision](-[classifier]).[ext]";

    private NamedDomainObjectContainer<DensityDistribution> distributionsContainer;
    private NamedDomainObjectContainer<DistributionResolution> distributionsResolutionStrategiesContainer;

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(DockerSupportPlugin.class);
        Provider<DockerSupportService> dockerSupport = GradleUtils.getBuildService(
            project.getGradle().getSharedServices(),
            DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME
        );

        project.getDependencies().registerTransform(UnzipTransform.class, transformSpec -> {
            transformSpec.getFrom().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.ZIP_TYPE);
            transformSpec.getTo().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });

        ArtifactTypeDefinition tarArtifactTypeDefinition = project.getDependencies().getArtifactTypes().maybeCreate("tar.gz");
        project.getDependencies().registerTransform(SymbolicLinkPreservingUntarTransform.class, transformSpec -> {
            transformSpec.getFrom().attribute(ArtifactAttributes.ARTIFACT_FORMAT, tarArtifactTypeDefinition.getName());
            transformSpec.getTo().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });

        setupResolutionsContainer(project);
        setupDistributionContainer(project, dockerSupport);
        setupDownloadServiceRepo(project);
        project.afterEvaluate(this::setupDistributions);
    }

    private void setupDistributionContainer(Project project, Provider<DockerSupportService> dockerSupport) {
        distributionsContainer = project.container(DensityDistribution.class, name -> {
            Configuration fileConfiguration = project.getConfigurations().create("density_distro_file_" + name);
            Configuration extractedConfiguration = project.getConfigurations().create(DISTRO_EXTRACTED_CONFIG_PREFIX + name);
            extractedConfiguration.getAttributes().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
            return new DensityDistribution(name, project.getObjects(), dockerSupport, fileConfiguration, extractedConfiguration);
        });
        project.getExtensions().add(CONTAINER_NAME, distributionsContainer);
    }

    private void setupResolutionsContainer(Project project) {
        distributionsResolutionStrategiesContainer = project.container(DistributionResolution.class);
        // We want this ordered in the same resolution strategies are added
        distributionsResolutionStrategiesContainer.whenObjectAdded(
            resolveDependencyNotation -> resolveDependencyNotation.setPriority(distributionsResolutionStrategiesContainer.size())
        );
        project.getExtensions().add(RESOLUTION_CONTAINER_NAME, distributionsResolutionStrategiesContainer);
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<DensityDistribution> getContainer(Project project) {
        return (NamedDomainObjectContainer<DensityDistribution>) project.getExtensions().getByName(CONTAINER_NAME);
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<DistributionResolution> getRegistrationsContainer(Project project) {
        return (NamedDomainObjectContainer<DistributionResolution>) project.getExtensions().getByName(RESOLUTION_CONTAINER_NAME);
    }

    // pkg private for tests
    void setupDistributions(Project project) {
        for (DensityDistribution distribution : distributionsContainer) {
            distribution.finalizeValues();
            DependencyHandler dependencies = project.getDependencies();
            // for the distribution as a file, just depend on the artifact directly
            DistributionDependency distributionDependency = resolveDependencyNotation(project, distribution);
            dependencies.add(distribution.configuration.getName(), distributionDependency.getDefaultNotation());
            // no extraction allowed for rpm, deb or docker
            if (distribution.getType().shouldExtract()) {
                // The extracted configuration depends on the artifact directly but has
                // an artifact transform registered to resolve it as an unpacked folder.
                dependencies.add(distribution.getExtracted().getName(), distributionDependency.getExtractedNotation());
            }
        }
    }

    private DistributionDependency resolveDependencyNotation(Project p, DensityDistribution distribution) {
        return distributionsResolutionStrategiesContainer.stream()
            .sorted(Comparator.comparingInt(DistributionResolution::getPriority))
            .map(r -> r.getResolver().resolve(p, distribution))
            .filter(Objects::nonNull)
            .findFirst()
            .orElseGet(() -> DistributionDependency.of(dependencyNotation(distribution)));
    }

    private static void addIvyRepo(Project project, String name, String url, String group, String... patternLayout) {
        project.getRepositories().exclusiveContent(exclusiveContentRepository -> {
            exclusiveContentRepository.filter(config -> config.includeGroup(group));
            exclusiveContentRepository.forRepositories(Arrays.stream(patternLayout).map(pattern -> project.getRepositories().ivy(repo -> {
                repo.setName(name);
                repo.setUrl(url);
                repo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
                repo.patternLayout(layout -> layout.artifact(pattern));
            })).toArray(IvyArtifactRepository[]::new));
        });
    }

    private static void setupDownloadServiceRepo(Project project) {
        if (project.getRepositories().findByName(DOWNLOAD_REPO_NAME) != null) {
            return;
        }
        Object customDistributionUrl = project.findProperty("customDistributionUrl");
        Object customDistributionDownloadType = project.findProperty("customDistributionDownloadType");
        // distributionDownloadType is default min if is not specified; download the distribution from CI if is bundle
        String distributionDownloadType = customDistributionDownloadType != null
            && customDistributionDownloadType.toString().equals("bundle") ? "bundle" : "min";
        if (customDistributionUrl != null) {
            addIvyRepo(project, DOWNLOAD_REPO_NAME, customDistributionUrl.toString(), FAKE_IVY_GROUP, "");
            addIvyRepo(project, SNAPSHOT_REPO_NAME, customDistributionUrl.toString(), FAKE_SNAPSHOT_IVY_GROUP, "");
            return;
        }
        switch (distributionDownloadType) {
            case "bundle":
                addIvyRepo(project, DOWNLOAD_REPO_NAME, "https://ci.density.org", FAKE_IVY_GROUP, BUNDLE_PATTERN_LAYOUT);
                addIvyRepo(project, SNAPSHOT_REPO_NAME, "https://ci.density.org", FAKE_SNAPSHOT_IVY_GROUP, BUNDLE_PATTERN_LAYOUT);
                break;
            case "min":
                addIvyRepo(
                    project,
                    DOWNLOAD_REPO_NAME,
                    "https://artifacts.density.org",
                    FAKE_IVY_GROUP,
                    "/releases" + RELEASE_PATTERN_LAYOUT,
                    "/release-candidates" + RELEASE_PATTERN_LAYOUT
                );
                addIvyRepo(
                    project,
                    SNAPSHOT_REPO_NAME,
                    "https://artifacts.density.org",
                    FAKE_SNAPSHOT_IVY_GROUP,
                    SNAPSHOT_PATTERN_LAYOUT
                );
                break;
            default:
                throw new IllegalArgumentException("Unsupported property argument: " + distributionDownloadType);
        }
    }

    /**
     * Returns a dependency object representing the given distribution.
     * <p>
     * The returned object is suitable to be passed to {@link DependencyHandler}.
     * The concrete type of the object will be a set of maven coordinates as a {@link String}.
     * Maven coordinates point to either the integ-test-zip coordinates on maven central, or a set of artificial
     * coordinates that resolve to the Density download service through an ivy repository.
     */
    private String dependencyNotation(DensityDistribution distribution) {
        Version distroVersion = Version.fromString(distribution.getVersion());
        if (distribution.getType() == Type.INTEG_TEST_ZIP) {
            return "org.density.distribution.integ-test-zip:density:" + distribution.getVersion() + "@zip";
        }

        String extension = distribution.getType().toString();
        String classifier = distroVersion.onOrAfter("1.0.0") ? ":x64" : ":x86_64";
        if (distribution.getType() == Type.ARCHIVE) {
            extension = distribution.getPlatform() == Platform.WINDOWS ? "zip" : "tar.gz";

            switch (distribution.getArchitecture()) {
                case ARM64:
                    classifier = ":" + distribution.getPlatform() + "-arm64";
                    break;
                case X64:
                    classifier = ":" + distribution.getPlatform() + "-x64";
                    break;
                case S390X:
                    classifier = ":" + distribution.getPlatform() + "-s390x";
                    break;
                case PPC64LE:
                    classifier = ":" + distribution.getPlatform() + "-ppc64le";
                    break;
                case RISCV64:
                    classifier = ":" + distribution.getPlatform() + "-riscv64";
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported architecture: " + distribution.getArchitecture());
            }
        } else if (distribution.getType() == Type.DEB) {
            classifier = ":amd64";
        }

        String group = distribution.getVersion().endsWith("-SNAPSHOT") ? FAKE_SNAPSHOT_IVY_GROUP : FAKE_IVY_GROUP;
        return group + ":density" + ":" + distribution.getVersion() + classifier + "@" + extension;
    }
}

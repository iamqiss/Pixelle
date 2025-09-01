/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.routing.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.density.action.LatchedActionListener;
import org.density.cluster.Diff;
import org.density.cluster.routing.IndexRoutingTable;
import org.density.cluster.routing.RoutingTable;
import org.density.cluster.routing.RoutingTableIncrementalDiff;
import org.density.cluster.routing.StringKeyDiffProvider;
import org.density.common.blobstore.BlobPath;
import org.density.common.lifecycle.AbstractLifecycleComponent;
import org.density.common.remote.RemoteWritableEntityStore;
import org.density.common.remote.RemoteWriteableEntityBlobStore;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.io.IOUtils;
import org.density.core.action.ActionListener;
import org.density.core.compress.Compressor;
import org.density.gateway.remote.ClusterMetadataManifest;
import org.density.gateway.remote.RemoteClusterStateUtils;
import org.density.gateway.remote.RemoteStateTransferException;
import org.density.gateway.remote.model.RemoteRoutingTableBlobStore;
import org.density.gateway.remote.routingtable.RemoteIndexRoutingTable;
import org.density.gateway.remote.routingtable.RemoteRoutingTableDiff;
import org.density.index.translog.transfer.BlobStoreTransferService;
import org.density.node.remotestore.RemoteStoreNodeAttribute;
import org.density.repositories.RepositoriesService;
import org.density.repositories.Repository;
import org.density.repositories.blobstore.BlobStoreRepository;
import org.density.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.density.node.remotestore.RemoteStoreNodeAttribute.isRemoteRoutingTableConfigured;

/**
 * A Service which provides APIs to upload and download routing table from remote store.
 *
 * @density.internal
 */
public class InternalRemoteRoutingTableService extends AbstractLifecycleComponent implements RemoteRoutingTableService {

    private static final Logger logger = LogManager.getLogger(InternalRemoteRoutingTableService.class);
    private final Settings settings;
    private final Supplier<RepositoriesService> repositoriesService;
    private Compressor compressor;
    private RemoteWritableEntityStore<IndexRoutingTable, RemoteIndexRoutingTable> remoteIndexRoutingTableStore;
    private RemoteWritableEntityStore<Diff<RoutingTable>, RemoteRoutingTableDiff> remoteRoutingTableDiffStore;
    private final ClusterSettings clusterSettings;
    private BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool;
    private final String clusterName;

    public InternalRemoteRoutingTableService(
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadpool,
        String clusterName
    ) {
        assert isRemoteRoutingTableConfigured(settings) : "Remote routing table is not enabled";
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.threadPool = threadpool;
        this.clusterName = clusterName;
        this.clusterSettings = clusterSettings;
    }

    public List<IndexRoutingTable> getIndicesRouting(RoutingTable routingTable) {
        return new ArrayList<>(routingTable.indicesRouting().values());
    }

    /**
     * Returns diff between the two routing tables, which includes upserts and deletes.
     *
     * @param before previous routing table
     * @param after  current routing table
     * @return incremental diff of the previous and current routing table
     */
    @Override
    public StringKeyDiffProvider<IndexRoutingTable> getIndicesRoutingMapDiff(RoutingTable before, RoutingTable after) {
        return new RoutingTableIncrementalDiff(before, after);
    }

    /**
     * Async action for writing one {@code IndexRoutingTable} to remote store
     *
     * @param term current term
     * @param version current version
     * @param clusterUUID current cluster UUID
     * @param indexRouting indexRoutingTable to write to remote store
     * @param latchedActionListener listener for handling async action response
     */
    @Override
    public void getAsyncIndexRoutingWriteAction(
        String clusterUUID,
        long term,
        long version,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {

        RemoteIndexRoutingTable remoteIndexRoutingTable = new RemoteIndexRoutingTable(indexRouting, clusterUUID, compressor, term, version);

        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(remoteIndexRoutingTable.getUploadedMetadata()),
            ex -> latchedActionListener.onFailure(
                new RemoteStateTransferException("Exception in writing index to remote store: " + indexRouting.getIndex().toString(), ex)
            )
        );

        remoteIndexRoutingTableStore.writeAsync(remoteIndexRoutingTable, completionListener);
    }

    @Override
    public void getAsyncIndexRoutingDiffWriteAction(
        String clusterUUID,
        long term,
        long version,
        StringKeyDiffProvider<IndexRoutingTable> routingTableDiff,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        RemoteRoutingTableDiff remoteRoutingTableDiff = new RemoteRoutingTableDiff(
            (RoutingTableIncrementalDiff) routingTableDiff,
            clusterUUID,
            compressor,
            term,
            version
        );
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(remoteRoutingTableDiff.getUploadedMetadata()),
            ex -> latchedActionListener.onFailure(
                new RemoteStateTransferException("Exception in writing index routing diff to remote store", ex)
            )
        );

        remoteRoutingTableDiffStore.writeAsync(remoteRoutingTableDiff, completionListener);
    }

    /**
     * Combines IndicesRoutingMetadata from previous manifest and current uploaded indices, removes deleted indices.
     * @param previousManifest previous manifest, used to get all existing indices routing paths
     * @param indicesRoutingUploaded current uploaded indices routings
     * @param indicesRoutingToDelete indices to delete
     * @return combined list of metadata
     */
    public List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(
        ClusterMetadataManifest previousManifest,
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingUploaded,
        List<String> indicesRoutingToDelete
    ) {
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndicesRouting = previousManifest.getIndicesRouting()
            .stream()
            .collect(Collectors.toMap(ClusterMetadataManifest.UploadedIndexMetadata::getIndexName, Function.identity()));

        indicesRoutingUploaded.forEach(
            uploadedIndexRouting -> allUploadedIndicesRouting.put(uploadedIndexRouting.getIndexName(), uploadedIndexRouting)
        );
        indicesRoutingToDelete.forEach(allUploadedIndicesRouting::remove);

        return new ArrayList<>(allUploadedIndicesRouting.values());
    }

    @Override
    public void getAsyncIndexRoutingReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<IndexRoutingTable> latchedActionListener
    ) {

        ActionListener<IndexRoutingTable> actionListener = ActionListener.wrap(
            latchedActionListener::onResponse,
            latchedActionListener::onFailure
        );

        RemoteIndexRoutingTable remoteIndexRoutingTable = new RemoteIndexRoutingTable(uploadedFilename, clusterUUID, compressor);

        remoteIndexRoutingTableStore.readAsync(remoteIndexRoutingTable, actionListener);
    }

    @Override
    public void getAsyncIndexRoutingTableDiffReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<Diff<RoutingTable>> latchedActionListener
    ) {
        ActionListener<Diff<RoutingTable>> actionListener = ActionListener.wrap(
            latchedActionListener::onResponse,
            latchedActionListener::onFailure
        );

        RemoteRoutingTableDiff remoteRoutingTableDiff = new RemoteRoutingTableDiff(uploadedFilename, clusterUUID, compressor);
        remoteRoutingTableDiffStore.readAsync(remoteRoutingTableDiff, actionListener);
    }

    @Override
    public List<ClusterMetadataManifest.UploadedIndexMetadata> getUpdatedIndexRoutingTableMetadata(
        List<String> updatedIndicesRouting,
        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndicesRouting
    ) {
        return updatedIndicesRouting.stream().map(idx -> {
            Optional<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndexMetadataOptional = allIndicesRouting.stream()
                .filter(idx2 -> idx2.getIndexName().equals(idx))
                .findFirst();
            assert uploadedIndexMetadataOptional.isPresent() == true;
            return uploadedIndexMetadataOptional.get();
        }).collect(Collectors.toList());
    }

    @Override
    protected void doClose() throws IOException {
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
    }

    @Override
    protected void doStart() {
        assert isRemoteRoutingTableConfigured(settings) == true : "Remote routing table is not enabled";
        final String remoteStoreRepo = RemoteStoreNodeAttribute.getRoutingTableRepoName(settings);
        assert remoteStoreRepo != null : "Remote routing table repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
        compressor = blobStoreRepository.getCompressor();

        this.remoteIndexRoutingTableStore = new RemoteRoutingTableBlobStore<>(
            new BlobStoreTransferService(blobStoreRepository.blobStore(), threadPool),
            blobStoreRepository,
            clusterName,
            threadPool,
            ThreadPool.Names.REMOTE_STATE_READ,
            clusterSettings
        );

        this.remoteRoutingTableDiffStore = new RemoteWriteableEntityBlobStore<>(
            new BlobStoreTransferService(blobStoreRepository.blobStore(), threadPool),
            blobStoreRepository,
            clusterName,
            threadPool,
            ThreadPool.Names.REMOTE_STATE_READ,
            RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN
        );
    }

    @Override
    protected void doStop() {}

    @Override
    public void deleteStaleIndexRoutingPaths(List<String> stalePaths) throws IOException {
        try {
            logger.debug(() -> "Deleting stale index routing files from remote - " + stalePaths);
            blobStoreRepository.blobStore().blobContainer(BlobPath.cleanPath()).deleteBlobsIgnoringIfNotExists(stalePaths);
        } catch (IOException e) {
            logger.error(() -> new ParameterizedMessage("Failed to delete some stale index routing paths from {}", stalePaths), e);
            throw e;
        }
    }

    public void deleteStaleIndexRoutingDiffPaths(List<String> stalePaths) throws IOException {
        try {
            logger.debug(() -> "Deleting stale index routing diff files from remote - " + stalePaths);
            blobStoreRepository.blobStore().blobContainer(BlobPath.cleanPath()).deleteBlobsIgnoringIfNotExists(stalePaths);
        } catch (IOException e) {
            logger.error(() -> new ParameterizedMessage("Failed to delete some stale index routing diff paths from {}", stalePaths), e);
            throw e;
        }
    }
}

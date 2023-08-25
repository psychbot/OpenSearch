/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Contains all the method needed for a remote store node lifecycle.
 */
public class RemoteStoreService {

    private static final Logger logger = LogManager.getLogger(RemoteStoreService.class);
    private final Supplier<RepositoriesService> repositoriesService;
    private static final String REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX = "remote_store";
    public static final String REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.segment.repository";
    public static final String REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.translog.repository";
    public static final String REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s.type";
    public static final String REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX = "remote_store.repository.%s.settings.";
    public static final Setting<String> REMOTE_STORE_MIGRATION_SETTING = Setting.simpleString("remote_store.migration",
        MigrationTypes.NOT_MIGRATING.value,
        MigrationTypes::validate,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope);

    public enum MigrationTypes {
        NOT_MIGRATING("not_migrating"),
        MIGRATING_TO_REMOTE_STORE("migrating_to_remote_store"),
        MIGRATING_TO_HOT("migrating_to_hot");
        public static MigrationTypes validate(String migrationType) {
            try {
                return MigrationTypes.valueOf(migrationType.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("[" + migrationType + "] migration type is not supported. " +
                    "Supported migration types are [" + MigrationTypes.values().toString() + "]");
            }
        }

        public final String value;
        MigrationTypes(String value) {
            this.value = value;
        }
    }

    public RemoteStoreService(Supplier<RepositoriesService> repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    private static String validateAttributeNonNull(DiscoveryNode node, String attributeKey) {
        String attributeValue = node.getAttributes().get(attributeKey);
        if (attributeValue == null || attributeValue.isEmpty()) {
            throw new IllegalStateException("joining node [" + node + "] doesn't have the node attribute [" + attributeKey + "].");
        }

        return attributeValue;
    }

    private static Map<String, String> validateSettingsAttributesNonNull(DiscoveryNode node, String settingsAttributeKeyPrefix) {
        return node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> validateAttributeNonNull(node, key)));
    }

    // TODO: Add logic to mark these repository as System Repository once thats merged.
    // Visible for testing
    public static RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
        String type = validateAttributeNonNull(
            node,
            String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name)
        );
        Map<String, String> settingsMap = validateSettingsAttributesNonNull(
            node,
            String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, name)
        );

        Settings.Builder settings = Settings.builder();
        settingsMap.entrySet().forEach(entry -> settings.put(entry.getKey(), entry.getValue()));

        return new RepositoryMetadata(name, type, settings.build());
    }

    private static RepositoriesMetadata buildRepositoriesMetadata(DiscoveryNode node) {
        String segmentRepositoryName = node.getAttributes().get(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
        String translogRepositoryName = node.getAttributes().get(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);
        if (segmentRepositoryName.equals(translogRepositoryName)) {
            return new RepositoriesMetadata(Collections.singletonList(buildRepositoryMetadata(node, segmentRepositoryName)));
        } else {
            List<RepositoryMetadata> repositoryMetadataList = new ArrayList<>();
            repositoryMetadataList.add(buildRepositoryMetadata(node, segmentRepositoryName));
            repositoryMetadataList.add(buildRepositoryMetadata(node, translogRepositoryName));
            return new RepositoriesMetadata(repositoryMetadataList);
        }
    }

    private void verifyRepository(RepositoryMetadata repositoryMetadata) {
        ActionListener<VerifyRepositoryResponse> listener = new ActionListener<>() {

            @Override
            public void onResponse(VerifyRepositoryResponse verifyRepositoryResponse) {
                logger.info("Successfully verified repository : " + verifyRepositoryResponse.toString());
            }

            @Override
            public void onFailure(Exception e) {
                throw new IllegalStateException("Failed to finish remote store repository verification" + e.getMessage());
            }
        };

        repositoriesService.get()
            .verifyRepository(
                repositoryMetadata.name(),
                ActionListener.delegateFailure(
                    listener,
                    (delegatedListener, verifyResponse) -> delegatedListener.onResponse(
                        new VerifyRepositoryResponse(verifyResponse.toArray(new DiscoveryNode[0]))
                    )
                )
            );
    }

    private ClusterState createRepository(RepositoryMetadata newRepositoryMetadata, ClusterState currentState) {
        RepositoriesService.validate(newRepositoryMetadata.name());

        Metadata metadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
        RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
        if (repositories == null) {
            Repository repository = repositoriesService.get().createRepository(newRepositoryMetadata);
            logger.info(
                "Remote store repository with name {} and type {} created.",
                repository.getMetadata().name(),
                repository.getMetadata().type()
            );
            repositories = new RepositoriesMetadata(Collections.singletonList(newRepositoryMetadata));
        } else {
            List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size() + 1);

            for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                if (repositoryMetadata.name().equals(newRepositoryMetadata.name())) {
                    if (newRepositoryMetadata.equalsIgnoreGenerations(repositoryMetadata)) {
                        return new ClusterState.Builder(currentState).build();
                    } else {
                        throw new IllegalStateException(
                            "new repository metadata ["
                                + newRepositoryMetadata
                                + "] supplied by joining node is different from existing repository metadata ["
                                + repositoryMetadata
                                + "]."
                        );
                    }
                } else {
                    repositoriesMetadata.add(repositoryMetadata);
                }
            }
            Repository repository = repositoriesService.get().createRepository(newRepositoryMetadata);
            logger.info(
                "Remote store repository with name {} and type {} created",
                repository.getMetadata().name(),
                repository.getMetadata().type()
            );
            repositoriesMetadata.add(newRepositoryMetadata);
            repositories = new RepositoriesMetadata(repositoriesMetadata);
        }
        mdBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    private boolean isRepositoryCreated(RepositoryMetadata repositoryMetadata) {
        try {
            repositoriesService.get().repository(repositoryMetadata.name());
            return true;
        } catch (RepositoryMissingException e) {
            return false;
        }
    }

    private boolean isRepositoryAddedInClusterState(RepositoryMetadata repositoryMetadata, ClusterState currentState) {
        RepositoriesMetadata repositoriesMetadata = currentState.metadata().custom(RepositoriesMetadata.TYPE);
        if (repositoriesMetadata == null) {
            return false;
        }
        for (RepositoryMetadata existingRepositoryMetadata : repositoriesMetadata.repositories()) {
            existingRepositoryMetadata.equalsIgnoreGenerations(repositoryMetadata);
            return true;
        }
        return false;
    }

    private ClusterState createOrVerifyRepository(RepositoriesMetadata repositoriesMetadata, ClusterState currentState) {
        ClusterState newState = ClusterState.builder(currentState).build();
        for (RepositoryMetadata repositoryMetadata : repositoriesMetadata.repositories()) {
            if (isRepositoryCreated(repositoryMetadata)) {
                verifyRepository(repositoryMetadata);
            } else {
                if (!isRepositoryAddedInClusterState(repositoryMetadata, currentState)) {
                    newState = ClusterState.builder(createRepository(repositoryMetadata, newState)).build();
                }
            }
        }
        return newState;
    }

    public ClusterState joinCluster(RemoteStoreNode joiningRemoteStoreNode, ClusterState currentState) {
        List<DiscoveryNode> existingNodes = new ArrayList<>(currentState.nodes().getNodes().values());
        if (existingNodes.isEmpty()) {
            return currentState;
        }
        ClusterState.Builder newState = ClusterState.builder(currentState);
        if (isRemoteStoreNode(existingNodes.get(0))) {
            RemoteStoreNode existingRemoteStoreNode = createRemoteStoreNode(existingNodes.get(0));
            if (joiningRemoteStoreNode.equals(existingRemoteStoreNode)) {
                newState = ClusterState.builder(createOrVerifyRepository(joiningRemoteStoreNode.getRepositoriesMetadata(), currentState));
            }
        } else {
            throw new IllegalStateException(
                "a remote store node [" + joiningRemoteStoreNode + "] is trying to join a non remote store cluster."
            );
        }
        return newState.build();
    }

    public static RemoteStoreNode createRemoteStoreNode(DiscoveryNode node) {
        return new RemoteStoreNode(node, buildRepositoriesMetadata(node));
    }

    public static boolean isRemoteStoreNode(DiscoveryNode node) {
        if (node == null) {
            return false;
        }

        return node.getAttributes().keySet().stream().anyMatch(key -> key.startsWith(REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX));
    }

    public static boolean ensureNodeCompatibility(DiscoveryNode joiningNode, DiscoveryNode existingNode) {
        if (isRemoteStoreNode(joiningNode)) {
            if (isRemoteStoreNode(existingNode)) {
                RemoteStoreNode joiningRemoteStoreNode = RemoteStoreService.createRemoteStoreNode(joiningNode);
                RemoteStoreNode existingRemoteStoreNode = RemoteStoreService.createRemoteStoreNode(existingNode);
                return existingRemoteStoreNode.equals(joiningRemoteStoreNode);
            } else {
                throw new IllegalStateException(
                    "a remote store node [" + joiningNode + "] is trying to join a non " + "remote store cluster."
                );
            }
        } else {
            if (isRemoteStoreNode(existingNode)) {
                throw new IllegalStateException("a non remote store node [" + joiningNode + "] is trying to join a remote store cluster.");
            }
        }
        return false;
    }
}

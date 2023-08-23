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
import org.opensearch.cluster.coordination.JoinTaskExecutor;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * RemoteStore Repository Registration helper
 */
public class RemoteStoreRepositoryRegistrationHelper {

    private static final Logger logger = LogManager.getLogger(RemoteStoreRepositoryRegistrationHelper.class);
    public static final String REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.segment.repository";
    public static final String REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.translog.repository";
    public static final String REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s.type";
    public static final String REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX = "remote_store.repository.%s.settings.";

    private static Map<String, String> validateSettingsAttributesNonNull(DiscoveryNode node, String settingsAttributeKeyPrefix) {
        return node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key, key -> validateAttributeNonNull(node, key)));
    }

    private static String validateAttributeNonNull(DiscoveryNode node, String attributeKey) {
        String attributeValue = node.getAttributes().get(attributeKey);
        if (attributeValue == null || attributeValue.isEmpty()) {
            throw new IllegalStateException("joining node [" + node + "] doesn't have the node attribute [" + attributeKey + "].");
        }

        return attributeValue;
    }

    /**
     * A node will be declared as remote store node if it has any of the remote store node attributes.
     * The method validates that the joining node has any of the remote store node attributes or not.
     */
    public static boolean isRemoteStoreNode(DiscoveryNode node) {
        if (node == null) {
            return false;
        }
        Map<String, String> joiningNodeAttributes = node.getAttributes();
        String segmentRepositoryName = joiningNodeAttributes.get(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
        String segmentRepositoryTypeAttributeKey = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            segmentRepositoryName
        );
        String segmentRepositorySettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            segmentRepositoryName
        );
        List<String> segmentRepositorySettingsAttributeKeys = joiningNodeAttributes.keySet()
            .stream()
            .filter(key -> key.startsWith(segmentRepositorySettingsAttributeKeyPrefix))
            .collect(Collectors.toList());

        String translogRepositoryName = joiningNodeAttributes.get(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);
        String translogRepositoryTypeAttributeKey = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            translogRepositoryName
        );
        String translogRepositorySettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            translogRepositoryName
        );
        List<String> translogRepositorySettingsAttributeKeys = joiningNodeAttributes.keySet()
            .stream()
            .filter(key -> key.startsWith(translogRepositorySettingsAttributeKeyPrefix))
            .collect(Collectors.toList());

        boolean remoteStoreNode = joiningNodeAttributes.get(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY) != null
            || joiningNodeAttributes.get(segmentRepositoryTypeAttributeKey) != null
            || segmentRepositorySettingsAttributeKeys.size() != 0
            || joiningNodeAttributes.get(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY) != null
            || joiningNodeAttributes.get(translogRepositoryTypeAttributeKey) != null
            || translogRepositorySettingsAttributeKeys.size() != 0;

        if (remoteStoreNode) {
            validateAttributeNonNull(node, REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
            validateAttributeNonNull(node, segmentRepositoryTypeAttributeKey);
            validateSettingsAttributesNonNull(node, segmentRepositorySettingsAttributeKeyPrefix);
            validateAttributeNonNull(node, REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);
            validateAttributeNonNull(node, translogRepositoryTypeAttributeKey);
            validateSettingsAttributesNonNull(node, translogRepositorySettingsAttributeKeyPrefix);
        }

        return remoteStoreNode;
    }

    private static void compareAttribute(DiscoveryNode joiningNode, DiscoveryNode existingNode, String attributeKey) {
        String joiningNodeAttributeValue = validateAttributeNonNull(joiningNode, attributeKey);
        String existingNodeAttribute = validateAttributeNonNull(existingNode, attributeKey);

        if (existingNodeAttribute.equals(joiningNodeAttributeValue) == false) {
            throw new IllegalStateException(
                "joining node ["
                    + joiningNode
                    + "] has node attribute ["
                    + attributeKey
                    + "] value ["
                    + joiningNodeAttributeValue
                    + "] which is different than existing node ["
                    + existingNode
                    + "] value ["
                    + existingNodeAttribute
                    + "]."
            );
        }
    }

    private static void compareSettingsAttributes(DiscoveryNode joiningNode, DiscoveryNode existingNode, String attributeKeyPrefix) {
        List<String> existingNodeSettingsAttributeKeys = existingNode.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(attributeKeyPrefix))
            .collect(Collectors.toList());
        existingNodeSettingsAttributeKeys.stream().forEach(key -> compareAttribute(joiningNode, existingNode, key));
    }

    /**
     * During node join request this method will validate if the joining node has the same remote store node
     * attributes as of the existing node in the cluster.
     *
     * TODO: The below check is valid till we support migration, once we start supporting migration a remote
     *       store node will be able to join a non remote store cluster and vice versa. #7986
     */
    public static boolean compareRemoteStoreNodeAttributes(DiscoveryNode joiningNode, DiscoveryNode existingNode) {
        String segmentRepositoryName = validateAttributeNonNull(existingNode, REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
        String translogRepositoryName = validateAttributeNonNull(existingNode, REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);

        compareAttribute(joiningNode, existingNode, REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
        compareAttribute(
            joiningNode,
            existingNode,
            String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, segmentRepositoryName)
        );
        compareSettingsAttributes(
            joiningNode,
            existingNode,
            String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, segmentRepositoryName)
        );
        compareAttribute(joiningNode, existingNode, REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);
        compareAttribute(
            joiningNode,
            existingNode,
            String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, translogRepositoryName)
        );
        compareSettingsAttributes(
            joiningNode,
            existingNode,
            String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, translogRepositoryName)
        );

        return true;
    }

    private static Settings buildSettings(DiscoveryNode node, String settingsAttributePrefix) {
        Settings.Builder settings = Settings.builder();
        node.getAttributes()
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().startsWith(settingsAttributePrefix))
            .forEach(entry -> settings.put(entry.getKey().replace(settingsAttributePrefix, ""), entry.getValue()));
        return settings.build();
    }

    // TODO: Add logic to mark these repository as System Repository once thats merged.
    // Visible For testing
    public static RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
        String type = node.getAttributes().get(String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name));
        String settingsAttributePrefix = String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, name);

        return new RepositoryMetadata(name, type, buildSettings(node, settingsAttributePrefix));

    }

    private static void verifyRemoteStoreRepository(
        String repositoryName,
        ActionListener<VerifyRepositoryResponse> listener,
        RepositoriesService repositoriesService
    ) {
        repositoriesService.verifyRepository(
            repositoryName,
            ActionListener.delegateFailure(
                listener,
                (delegatedListener, verifyResponse) -> delegatedListener.onResponse(
                    new VerifyRepositoryResponse(verifyResponse.toArray(new DiscoveryNode[0]))
                )
            )
        );
    }

    private static void validateRemoteStoreRepositories(
        DiscoveryNode joiningNode,
        ClusterState currentState,
        RepositoriesService repositoriesService
    ) {
        compareRemoteStoreNodeAttributes(joiningNode, (DiscoveryNode) currentState.nodes().getNodes().values().toArray()[0]);

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

        verifyRemoteStoreRepository(
            joiningNode.getAttributes().get(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY),
            listener,
            repositoriesService
        );
        verifyRemoteStoreRepository(
            joiningNode.getAttributes().get(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY),
            listener,
            repositoriesService
        );
    }

    private static ClusterState updateClusterStateWithRepositoryMetadata(
        ClusterState currentState,
        RepositoryMetadata newRepositoryMetadata,
        RepositoriesService repositoriesService
    ) {
        RepositoriesService.validate(newRepositoryMetadata.name());

        Metadata metadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
        RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
        if (repositories == null) {
            Repository repository = repositoriesService.createRepository(newRepositoryMetadata, repositoriesService.getTypesRegistry());
            logger.info(
                "Remote store repository with name {} and type {} Created",
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
                                + "]"
                        );
                    }
                } else {
                    repositoriesMetadata.add(repositoryMetadata);
                }
            }
            Repository repository = repositoriesService.createRepository(newRepositoryMetadata, repositoriesService.getTypesRegistry());
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

    private static ClusterState addRepository(
        DiscoveryNode joiningNode,
        ClusterState currentState,
        RepositoriesService repositoriesService,
        String repositoryNameAttributeKey
    ) {
        String repositoryName = joiningNode.getAttributes().get(repositoryNameAttributeKey);
        ClusterState newState = updateClusterStateWithRepositoryMetadata(
            currentState,
            buildRepositoryMetadata(joiningNode, repositoryName),
            repositoriesService
        );

        return newState;
    }

    private static ClusterState addRemoteStoreRepositories(
        DiscoveryNode joiningNode,
        ClusterState currentState,
        RepositoriesService repositoriesService
    ) {
        ClusterState newState = ClusterState.builder(currentState).build();
        List<DiscoveryNode> existingNodes = new ArrayList<>(currentState.nodes().getNodes().values());

        if (joiningNode.equals(existingNodes.get(0)) || compareRemoteStoreNodeAttributes(joiningNode, existingNodes.get(0))) {
            newState = addRepository(joiningNode, currentState, repositoriesService, REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
            newState = addRepository(
                joiningNode,
                ClusterState.builder(newState).build(),
                repositoriesService,
                REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY
            );
        }
        return newState;
    }

    /**
     * Validated or adds the remote store repositories to cluster state post its creation if the repositories doesn't
     * exist. Returns the updated cluster state.
     */
    public static ClusterState validateOrAddRemoteStoreRepositories(
        JoinTaskExecutor.Task joinTask,
        ClusterState currentState,
        RepositoriesService repositoriesService
    ) {
        ClusterState newState = ClusterState.builder(currentState).build();
        /** Skipping the validation/addition of remote store repository in cluster state if task doesn't contain node
         *  or node isn't a remote store node. **/
        if (joinTask.node() != null && isRemoteStoreNode(joinTask.node())) {
            if (JoinTaskExecutor.Task.ELECT_LEADER_TASK_REASON.equals(joinTask.reason())) {
                newState = ClusterState.builder(addRemoteStoreRepositories(joinTask.node(), newState, repositoriesService)).build();
            } else {
                validateRemoteStoreRepositories(joinTask.node(), newState, repositoriesService);
            }
        }

        return newState;
    }
}

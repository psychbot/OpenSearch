/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.repository;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.RepositoriesService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * RemoteStore Repository Registration helper
 */
public class RemoteStoreRepositoryRegistrationHelper {

    public static final String REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY = "cluster.remote_store.segment";
    public static final String REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY = "cluster.remote_store.translog";
    public static final String REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s.type";
    public static final String REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s.settings";

    private static void validateAttributeNonNull(DiscoveryNode joiningNode, String attributeKey) {
        String attributeValue = joiningNode.getAttributes().get(attributeKey);
        if (attributeValue == null || attributeValue.isEmpty()) {
            throw new IllegalStateException("joining node [" + joiningNode + "] doesn't have the node attribute [" + attributeKey + "]");
        }
    }

    /**
     * A node will be declared as remote store node if it has any of the remote store node attributes.
     * The method validates that the joining node has any of the remote store node attributes or not.
     * @param joiningNode
     * @return boolean value on the basis of remote store node attributes.
     */
    public static boolean isRemoteStoreNode(DiscoveryNode joiningNode) {
        Map<String, String> joiningNodeAttributes = joiningNode.getAttributes();
        String segmentRepositoryName = joiningNodeAttributes.get(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
        String segmentRepositoryTypeAttributeKey = String.format(REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, segmentRepositoryName);
        String segmentRepositorySettingsAttributeKey = String.format(
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_FORMAT,
            segmentRepositoryName
        );
        String translogRepositoryName = joiningNodeAttributes.get(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);
        String translogRepositoryTypeAttributeKey = String.format(
            REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            translogRepositoryName
        );
        String translogRepositorySettingsAttributeKey = String.format(
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_FORMAT,
            translogRepositoryName
        );

        boolean remoteStoreNode = joiningNodeAttributes.get(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY) != null
            || joiningNodeAttributes.get(segmentRepositoryTypeAttributeKey) != null
            || joiningNodeAttributes.get(segmentRepositorySettingsAttributeKey) != null
            || joiningNodeAttributes.get(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY) != null
            || joiningNodeAttributes.get(translogRepositoryTypeAttributeKey) != null
            || joiningNodeAttributes.get(translogRepositorySettingsAttributeKey) != null;

        if (remoteStoreNode) {
            validateAttributeNonNull(joiningNode, REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
            validateAttributeNonNull(joiningNode, segmentRepositoryTypeAttributeKey);
            validateAttributeNonNull(joiningNode, segmentRepositorySettingsAttributeKey);
            validateAttributeNonNull(joiningNode, REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);
            validateAttributeNonNull(joiningNode, translogRepositoryTypeAttributeKey);
            validateAttributeNonNull(joiningNode, translogRepositorySettingsAttributeKey);
        }

        return remoteStoreNode;
    }

    private static void compareAttribute(DiscoveryNode joiningNode, DiscoveryNode existingNode, String attributeKey) {
        String joiningNodeAttribute = joiningNode.getAttributes().get(attributeKey);
        String existingNodeAttribute = existingNode.getAttributes().get(attributeKey);

        if (existingNodeAttribute.equals(joiningNodeAttribute) == false) {
            throw new IllegalStateException(
                "joining node ["
                    + joiningNode
                    + "] has node attribute ["
                    + attributeKey
                    + "] value ["
                    + joiningNodeAttribute
                    + "] which is different than existing node ["
                    + existingNode
                    + "] value ["
                    + existingNodeAttribute
                    + "]"
            );
        }
    }

    // TODO: See a better way to compare the remote store node attributes.
    public static void compareNodeAttributes(DiscoveryNode joiningNode, DiscoveryNode existingNode) {
        String segmentRepositoryName = existingNode.getAttributes().get(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
        String translogRepositoryName = existingNode.getAttributes().get(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);

        compareAttribute(joiningNode, existingNode, REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
        compareAttribute(
            joiningNode,
            existingNode,
            String.format(REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, segmentRepositoryName)
        );
        compareAttribute(
            joiningNode,
            existingNode,
            String.format(REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_FORMAT, segmentRepositoryName)
        );
        compareAttribute(joiningNode, existingNode, REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);
        compareAttribute(
            joiningNode,
            existingNode,
            String.format(REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, translogRepositoryName)
        );
        compareAttribute(
            joiningNode,
            existingNode,
            String.format(REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_FORMAT, translogRepositoryName)
        );
    }

    private static Settings buildSettings(String stringSettings) {
        Settings.Builder settings = Settings.builder();

        String[] stringKeyValue = stringSettings.split(",");
        for (int i = 0; i < stringKeyValue.length; i++) {
            String[] keyValue = stringKeyValue[i].split(":");
            settings.put(keyValue[0].trim(), keyValue[1].trim());
        }

        return settings.build();
    }

    // TODO: Add logic to mark these repository as System Repository once thats merged.
    // Visible For testing
    public static RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
        String type = node.getAttributes().get(String.format(REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name));
        String settings = node.getAttributes().get(String.format(REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_FORMAT, name));

        validateAttributeNonNull(node, String.format(REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name));
        validateAttributeNonNull(node, String.format(REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_FORMAT, name));

        return new RepositoryMetadata(name, type, buildSettings(settings));

    }

    /**
     * Validated or adds the remote store repository to cluster state if it doesn't exist.
     * @param joiningNode
     * @param currentState
     * @return updated cluster state
     */
    public static ClusterState validateOrAddRemoteStoreRepository(DiscoveryNode joiningNode, ClusterState currentState) {
        List<DiscoveryNode> existingNodes = new ArrayList<>(currentState.getNodes().getNodes().values());

        ClusterState newState = ClusterState.builder(currentState).build();

        // TODO: Mutating cluster state like this can be dangerous, this will need refactoring.
        if (existingNodes.size() == 0) {
            validateAttributeNonNull(joiningNode, REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
            validateAttributeNonNull(joiningNode, REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);

            newState = updateClusterStateWithRepositoryMetadata(
                currentState,
                buildRepositoryMetadata(joiningNode, joiningNode.getAttributes().get(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY))
            );
            newState = updateClusterStateWithRepositoryMetadata(
                newState,
                buildRepositoryMetadata(joiningNode, joiningNode.getAttributes().get(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY))
            );
            return newState;
        } else {
            compareNodeAttributes(joiningNode, existingNodes.get(0));
        }

        return newState;
    }

    private static ClusterState updateClusterStateWithRepositoryMetadata(
        ClusterState currentState,
        RepositoryMetadata newRepositoryMetadata
    ) {
        RepositoriesService.validate(newRepositoryMetadata.name());

        Metadata metadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
        RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
        if (repositories == null) {
            repositories = new RepositoriesMetadata(Collections.singletonList(newRepositoryMetadata));
        } else {
            List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size() + 1);

            for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                if (repositoryMetadata.name().equals(newRepositoryMetadata.name())) {
                    if (newRepositoryMetadata.equalsIgnoreGenerations(repositoryMetadata)) {
                        // Previous version is the same as this one no update is needed.
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
            repositoriesMetadata.add(newRepositoryMetadata);
            repositories = new RepositoriesMetadata(repositoriesMetadata);
        }
        mdBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }
}

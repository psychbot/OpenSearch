/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.repository;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains all the parameters related to a remote store node.
 */
public class RemoteStoreNode extends DiscoveryNode {

    private final RepositoriesMetadata repositoriesMetadata;
    private final Set<BlobStoreRepository> blobStoreRepository;

    public static final String REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX = "remote_store";
    public static final String REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.segment.repository";
    public static final String REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.translog.repository";
    public static final String REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s.type";
    public static final String REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX = "remote_store.repository.%s.settings.";

    public RemoteStoreNode(String name, String id, TransportAddress transportAddress, Map<String, String> attributes,
                           Set<DiscoveryNodeRole> roles, Version version, Set<BlobStoreRepository> blobStoreRepository) {
        super(name, id, transportAddress, attributes, roles, version);
        this.repositoriesMetadata = buildRepositoriesMetadata();
        this.blobStoreRepository = blobStoreRepository;
    }

    RepositoriesMetadata buildRepositoriesMetadata() {
        String segmentRepositoryName = getAttributes().get(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
        String translogRepositoryName = getAttributes().get(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);
        if (segmentRepositoryName.equals(translogRepositoryName)) {
            return new RepositoriesMetadata(Collections.singletonList(buildRepositoryMetadata(segmentRepositoryName)));
        } else {
            List<RepositoryMetadata> repositoryMetadataList = new ArrayList<>();
            repositoryMetadataList.add(buildRepositoryMetadata(segmentRepositoryName));
            repositoryMetadataList.add(buildRepositoryMetadata(translogRepositoryName));
            return new RepositoriesMetadata(repositoryMetadataList);
        }
    }

    // TODO: Add logic to mark these repository as System Repository once thats merged.
    // Visible for testing
    RepositoryMetadata buildRepositoryMetadata(String name) {
        String type = validateAttributeNonNull(String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name));
        Map<String, String> settingsMap = validateSettingsAttributesNonNull(
            String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, name)
        );
        Settings.Builder settings = Settings.builder();
        settingsMap.entrySet().forEach(entry -> settings.put(entry.getKey(), entry.getValue()));
        return new RepositoryMetadata(name, type, settings.build());
    }

    private String validateAttributeNonNull(String attributeKey) {
        String attributeValue = this.getAttributes().get(attributeKey);
        if (attributeValue == null || attributeValue.isEmpty()) {
            throw new IllegalStateException("joining node [" + this + "] doesn't have the node attribute [" + attributeKey + "].");
        }

        return attributeValue;
    }

    private Map<String, String> validateSettingsAttributesNonNull(String settingsAttributeKeyPrefix) {
        return this.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> validateAttributeNonNull(key)));
    }

    RepositoriesMetadata getRepositoriesMetadata() {
        return this.repositoriesMetadata;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this, repositoriesMetadata);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteStoreNode that = (RemoteStoreNode) o;
        return this.getRepositoriesMetadata().equalsIgnoreGenerations(that.getRepositoriesMetadata());
    }
}

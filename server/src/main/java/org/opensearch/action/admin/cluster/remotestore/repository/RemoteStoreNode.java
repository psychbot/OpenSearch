/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.repository;

import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.node.DiscoveryNode;

/**
 * Contains all the parameters related to a remote store node.
 */
public class RemoteStoreNode {

    private final DiscoveryNode node;
    private final RepositoriesMetadata repositoriesMetadata;

    public RemoteStoreNode(DiscoveryNode node, RepositoriesMetadata repositoriesMetadata) {
        this.node = node;
        this.repositoriesMetadata = repositoriesMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteStoreNode that = (RemoteStoreNode) o;

        return this.getRepositoriesMetadata().equalsIgnoreGenerations(that.getRepositoriesMetadata());
    }

    RepositoriesMetadata getRepositoriesMetadata() {
        return this.repositoriesMetadata;
    }
}

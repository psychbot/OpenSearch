/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.action.admin.cluster.remotestore.repository.RemoteStoreRepositoryRegistrationHelper.buildRepositoryMetadata;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class RemoteStoreRepositoryRegistrationIT extends RemoteStoreBaseIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    private void assertRemoteStoreRepositoryOnAllNodes() {
        RepositoriesMetadata repositories = internalCluster().getInstance(ClusterService.class, internalCluster().getNodeNames()[0])
            .state()
            .metadata()
            .custom(RepositoriesMetadata.TYPE);
        RepositoryMetadata actualSegmentRepository = repositories.repository(REPOSITORY_NAME);
        RepositoryMetadata actualTranslogRepository = repositories.repository(REPOSITORY_2_NAME);

        for (String nodeName : internalCluster().getNodeNames()) {
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nodeName);
            DiscoveryNode node = clusterService.localNode();
            RepositoryMetadata expectedSegmentRepository = buildRepositoryMetadata(node, REPOSITORY_NAME);
            RepositoryMetadata expectedTranslogRepository = buildRepositoryMetadata(node, REPOSITORY_2_NAME);
            assertTrue(actualSegmentRepository.equalsIgnoreGenerations(expectedSegmentRepository));
            assertTrue(actualTranslogRepository.equalsIgnoreGenerations(expectedTranslogRepository));
        }
    }

    public void testSingleNodeClusterRepositoryRegistration() {
        internalCluster().startClusterManagerOnlyNode(remoteStoreNodeAttributes(REPOSITORY_NAME, REPOSITORY_2_NAME));
        ensureStableCluster(1);

        assertRemoteStoreRepositoryOnAllNodes();
    }

    public void testMultiNodeClusterRepositoryRegistration() {
        Settings clusterSettings = remoteStoreNodeAttributes(REPOSITORY_NAME, REPOSITORY_2_NAME);
        internalCluster().startClusterManagerOnlyNode(clusterSettings);
        internalCluster().startNodes(3, clusterSettings);
        ensureStableCluster(4);

        assertRemoteStoreRepositoryOnAllNodes();
    }

    public void testMultiNodeClusterRepositoryRegistrationWithMultipleMasters() {
        Settings clusterSettings = remoteStoreNodeAttributes(REPOSITORY_NAME, REPOSITORY_2_NAME);
        internalCluster().startClusterManagerOnlyNodes(3, clusterSettings);
        internalCluster().startNodes(3, clusterSettings);
        ensureStableCluster(6);

        assertRemoteStoreRepositoryOnAllNodes();
    }
}

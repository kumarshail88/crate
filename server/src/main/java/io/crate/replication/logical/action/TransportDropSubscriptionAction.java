/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.replication.logical.action;

import io.crate.common.collections.Tuple;
import io.crate.common.unit.TimeValue;
import io.crate.exceptions.Exceptions;
import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.cluster.AbstractOpenCloseTableClusterStateTaskExecutor;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.metadata.cluster.OpenTableClusterStateTaskExecutor;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.replication.logical.exceptions.SubscriptionAlreadyExistsException;
import io.crate.replication.logical.exceptions.SubscriptionUnknownException;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.user.WriteUserResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME;
import static io.crate.metadata.cluster.AbstractOpenCloseTableClusterStateTaskExecutor.OpenCloseTable;

@Singleton
public class TransportDropSubscriptionAction extends TransportMasterNodeAction<DropSubscriptionRequest, AcknowledgedResponse> {

    public static final String ACTION_NAME = "internal:crate:replication/logical/subscription/drop";

    private final OpenTableClusterStateTaskExecutor openTableClusterStateTaskExecutor;

    private final Schemas schemas;

    @Inject
    public TransportDropSubscriptionAction(TransportService transportService,
                                           ClusterService clusterService,
                                           ThreadPool threadPool,
                                           IndexNameExpressionResolver indexNameExpressionResolver,
                                           OpenTableClusterStateTaskExecutor openTableClusterStateTaskExecutor,
                                           Schemas schemas) {
        super(ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            DropSubscriptionRequest::new,
            indexNameExpressionResolver);
        this.openTableClusterStateTaskExecutor = openTableClusterStateTaskExecutor;
        this.schemas = schemas;
    }


    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected WriteUserResponse read(StreamInput in) throws IOException {
        return new WriteUserResponse(in);
    }

    @Override
    protected void masterOperation(DropSubscriptionRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
        List<IndexMetadata> subscriptionIndices = getSubscriptionIndices(request.name(), state.metadata().indices());
        List<OpenCloseTable> openCloseTables = getSubscriptionTables(request.name(), schemas);

        submitCloseSubscriptionsTablesTask(openCloseTables)
            .thenCompose(res -> submitDropSubscriptionTask(request.name(), request.ifExists(), subscriptionIndices))
            .thenCompose(res -> submitOpenSubscriptionsTablesTask(openCloseTables, subscriptionIndices))
            .whenComplete(
                (ignore, err) -> {
                    if (err == null) {
                        listener.onResponse(new AcknowledgedResponse(true));
                    } else {
                        // Hint in logs with action to repeat is already shown on the corresponding step.
                        listener.onFailure(Exceptions.toRuntimeException(err));
                    }
                }
            );
    }

    @Override
    protected ClusterBlockException checkBlock(DropSubscriptionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    /**
     * Closes tables and consequently closes all shards of the index
     * which, in turn, stops trackers and removes retention lease on the remote cluster.
     */
    private CompletableFuture<Void> submitCloseSubscriptionsTablesTask(List<OpenCloseTable> openCloseTables) {
        var future = new CompletableFuture<Void>();

        future.complete(null);

        /* TODO: Uncomment code below once TransportCloseTable is adapted to handle multiple tables.

        clusterService.submitStateUpdateTask(
            "drop-sub-close-tables",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    // extract relevant logic from AddCloseBlocksTask(listener, request)),
                    // make it accept multiple tables - update builders inside loop
                    // then call that code here and in TransportCloseTable's execute call extracted method like f(List.of(request.relation))
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("Couldn't execute task 'drop-sub-close-tables'. Please run command DROP SUBSCRIPTION again.");
                    future.completeExceptionally(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    future.complete(null);
                }
            }
        );
        */
        return future;
    }

    /**
     * Removes subscription from the cluster metadata and
     * removes setting LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME from each relevant for subscription index.
     */
    private CompletableFuture<Void> submitDropSubscriptionTask(String subscriptionName,
                                                               boolean ifExists,
                                                               List<IndexMetadata> subscriptionIndices) {
        var future = new CompletableFuture<Void>();

        // Check if we're still the elected master before sending second update task
        if (!clusterService.state().nodes().isLocalNodeElectedMaster()) {
            logger.error("Couldn't execute task 'drop-sub-remove-setting-and-metadata'. Please run command DROP SUBSCRIPTION again.");
            future.completeExceptionally(new IllegalStateException("Master was re-elected, cannot execute 'drop-sub-remove-setting-and-metadata'"));
        }

        clusterService.submitStateUpdateTask(
            "drop-sub-remove-setting-and-metadata",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata currentMetadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                    SubscriptionsMetadata oldMetadata = (SubscriptionsMetadata) mdBuilder.getCustom(SubscriptionsMetadata.TYPE);
                    if (oldMetadata != null && oldMetadata.subscription().containsKey(subscriptionName)) {

                        SubscriptionsMetadata newMetadata = SubscriptionsMetadata.newInstance(oldMetadata);
                        newMetadata.subscription().remove(subscriptionName);
                        assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                        mdBuilder.putCustom(SubscriptionsMetadata.TYPE, newMetadata);

                        // mdBuilder is mutated accordingly.
                        removeSubscriptionSetting(subscriptionIndices, mdBuilder);

                        return ClusterState.builder(currentState).metadata(mdBuilder).build();
                    } else if (ifExists == false) {
                        throw new SubscriptionUnknownException(subscriptionName);
                    }
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("Couldn't execute task 'drop-sub-remove-setting-and-metadata'. Please run command DROP SUBSCRIPTION again.");
                    future.completeExceptionally(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    future.complete(null);
                }
            }
        );
        return future;
    }

    /**
     * Opens tables after removing replication setting
     * and consequently updates DocTableInfo-s with the normal engine and makes tables writable.
     */
    private CompletableFuture<Void> submitOpenSubscriptionsTablesTask(List<OpenCloseTable> openCloseTables, List<IndexMetadata> subscriptionIndices) {
        var future = new CompletableFuture<Void>();

        // Check if we're still the elected master before sending third update task
        if (!clusterService.state().nodes().isLocalNodeElectedMaster()) {
            logger.error("Couldn't execute task 'drop-sub-close-tables'. Please run command DROP SUBSCRIPTION again.");
            future.completeExceptionally(new IllegalStateException("Master was re-elected, cannot execute 'drop-sub-close-tables'"));
        }


        clusterService.submitStateUpdateTask(
            "drop-sub-open-tables",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return openTableClusterStateTaskExecutor.openTables(openCloseTables, subscriptionIndices, currentState);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("Couldn't execute task 'drop-sub-open-tables'. Please run command DROP SUBSCRIPTION again.");
                    future.completeExceptionally(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    future.complete(null);
                }
            }
        );
        return future;
    }


    private List<IndexMetadata> getSubscriptionIndices(@Nonnull String subscriptionToDrop, ImmutableOpenMap<String, IndexMetadata> indices) {
        List<IndexMetadata> relevantIndices = new ArrayList<>();
        Iterator<IndexMetadata> indicesIterator = indices.valuesIt();
        while (indicesIterator.hasNext()) {
            IndexMetadata indexMetadata = indicesIterator.next();
            var settings = indexMetadata.getSettings();
            var subscriptionName = REPLICATION_SUBSCRIPTION_NAME.get(settings); // Can be null.
            if (subscriptionToDrop.equals(subscriptionName)) {
                relevantIndices.add(indexMetadata);
            }
        }
        return relevantIndices;
    }

    private List<OpenCloseTable> getSubscriptionTables(@Nonnull String subscriptionToDrop, Schemas schemas) {
        return InformationSchemaIterables.tablesStream(schemas)
            .filter(t -> {
                if (t instanceof DocTableInfo dt) {
                    return subscriptionToDrop.equals(REPLICATION_SUBSCRIPTION_NAME.get(dt.parameters()));
                }
                return false;
            })
            .map(dt -> new OpenCloseTable(dt.ident(), null))
            .collect(Collectors.toList());
    }

    private void removeSubscriptionSetting(List<IndexMetadata> subscriptionIndices, Metadata.Builder mdBuilder) {
        for (IndexMetadata indexMetadata: subscriptionIndices) {
            var settingsBuilder = Settings.builder().put(indexMetadata.getSettings());
            settingsBuilder.remove(REPLICATION_SUBSCRIPTION_NAME.getKey());
            mdBuilder.put(
                IndexMetadata
                    .builder(indexMetadata)
                    .settingsVersion(1 + indexMetadata.getSettingsVersion())
                    .settings(settingsBuilder)
            );
        }
    }

}

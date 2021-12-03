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

import io.crate.common.unit.TimeValue;
import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.replication.logical.exceptions.SubscriptionUnknownException;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.action.support.master.MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT;

@Singleton
public class TransportDropSubscriptionAction extends AbstractDDLTransportAction<DropSubscriptionRequest, AcknowledgedResponse> {

    public static final String ACTION_NAME = "internal:crate:replication/logical/subscription/drop";

    @Inject
    public TransportDropSubscriptionAction(TransportService transportService,
                                           ClusterService clusterService,
                                           ThreadPool threadPool,
                                           IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            indexNameExpressionResolver,
            DropSubscriptionRequest::new,
            AcknowledgedResponse::new,
            AcknowledgedResponse::new,
            "drop-subscription");
    }

    @Override
    protected void masterOperation(DropSubscriptionRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {

        Function<ClusterState, ClusterState> closeSubscriptionIndices = curState -> {
            return curState;
            //TODO Apply all relevant changes from CloseTableClusterStateTaskExecutor?
        };

        Function<ClusterState, ClusterState> removeSubscriptionSetting = curState -> {
            return curState;
            // TODO DROP setting
        };

        Function<ClusterState, ClusterState> openSubscriptionIndices = curState -> {
            return curState;
            //TODO Apply all relevant changes from OpenTableClusterStateTaskExecutor?
        };

        List<AckedClusterStateUpdateTaskChain.ChainableTask> tasks = List.of(
            new AckedClusterStateUpdateTaskChain.ChainableTask(
                closeSubscriptionIndices,
                "Please run command DROP SUBSCRIPTION again",
                "close-subscription-indices"
            ),
            new AckedClusterStateUpdateTaskChain.ChainableTask(
                removeSubscriptionSetting,
                "Please run command DROP SUBSCRIPTION again",
                "remove-subscription-setting"
            ),
            new AckedClusterStateUpdateTaskChain.ChainableTask(
                openSubscriptionIndices,
                "Please run command ALTER TABLE OPEN for subscribed tables",
                "open-subscription-indices"
            )
        );

        AckedClusterStateUpdateTaskChain ackedClusterStateUpdateTaskChain =
            new AckedClusterStateUpdateTaskChain(tasks, clusterService, listener, request, Priority.HIGH);

        ackedClusterStateUpdateTaskChain.startTaskChain();
    }

    @Override
    public ClusterStateTaskExecutor<DropSubscriptionRequest> clusterStateTaskExecutor(DropSubscriptionRequest request) {
        return new DDLClusterStateTaskExecutor<>() {
            @Override
            protected ClusterState execute(ClusterState currentState, DropSubscriptionRequest dropSubscriptionRequest) throws Exception {
                Metadata currentMetadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                SubscriptionsMetadata oldMetadata = (SubscriptionsMetadata) mdBuilder.getCustom(SubscriptionsMetadata.TYPE);
                if (oldMetadata != null && oldMetadata.subscription().containsKey(request.name())) {

                    // LogicalReplicationService will react to cluster change events and will stop the replication.
                    SubscriptionsMetadata newMetadata = SubscriptionsMetadata.newInstance(oldMetadata);
                    newMetadata.subscription().remove(request.name());
                    assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                    mdBuilder.putCustom(SubscriptionsMetadata.TYPE, newMetadata);

                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                } else if (request.ifExists() == false) {
                    throw new SubscriptionUnknownException(request.name());
                }

                return currentState;
            }
        };
    }

    @Override
    protected ClusterBlockException checkBlock(DropSubscriptionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}

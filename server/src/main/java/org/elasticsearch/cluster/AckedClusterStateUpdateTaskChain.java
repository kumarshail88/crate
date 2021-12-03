/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package org.elasticsearch.cluster;

import io.crate.common.collections.Tuple;
import io.crate.replication.logical.LogicalReplicationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.function.Function;

/**
 * Executes given tasks on a master node.
 * Every task must be acked on each node before the next task is started.
 */
public final class AckedClusterStateUpdateTaskChain {

    private static final Logger LOGGER = LogManager.getLogger(AckedClusterStateUpdateTaskChain.class);

    private final ChainableAckedClusterStateUpdateTask firstTask;
    private final ClusterService clusterService;

    public AckedClusterStateUpdateTaskChain(List<ChainableTask> taskChain,
                                            ClusterService clusterService,
                                            ActionListener<AcknowledgedResponse> listener,
                                            AcknowledgedRequest request,
                                            Priority priority) {
        Objects.requireNonNull(taskChain, "Task chain cannot be null.");
        assert !taskChain.isEmpty() : "Task chain must consist of at least one task.";
        this.clusterService = clusterService;

        // Wrap into next task aware instances.
        ChainableAckedClusterStateUpdateTask current = null;
        ChainableAckedClusterStateUpdateTask next = null;
        ListIterator<ChainableTask> it = taskChain.listIterator(taskChain.size());
        while (it.hasPrevious()) {
            current = new ChainableAckedClusterStateUpdateTask(priority, request, listener, it.previous(), next);
            next = current; // We are going from right to left, preparing next = "i+1", for the next iteration.
        }
        this.firstTask = current;
    }


    public static record ChainableTask(Function<ClusterState, ClusterState> stateUpdater, String hintOnError, String taskName) { }

    public void startTaskChain() {
        clusterService.submitStateUpdateTask(firstTask.taskName, firstTask);
    }

    public final class ChainableAckedClusterStateUpdateTask extends AckedClusterStateUpdateTask {

        private final ActionListener listener;
        private final ChainableAckedClusterStateUpdateTask nextTask;
        private final Function<ClusterState, ClusterState> stateUpdater;
        private final String hintOnError;
        private final String taskName;

        public ChainableAckedClusterStateUpdateTask(Priority priority,
                                                     AckedRequest request,
                                                     ActionListener listener,
                                                     ChainableTask chainableTask,
                                                     @Nullable ChainableAckedClusterStateUpdateTask nextTask) {
            super(priority, request, listener);
            this.listener = listener;
            this.stateUpdater = chainableTask.stateUpdater;
            this.taskName = chainableTask.taskName;
            this.hintOnError = chainableTask.hintOnError;
            this.nextTask = nextTask;
        }

        @Override
        public void onAllNodesAcked(@Nullable Exception e) {
            if (e == null) {
                /*
                if (!clusterService.state().nodes().isLocalNodeElectedMaster()) {
                    LOGGER.warn("Master was re-elected, cannot execute task {}, {}", taskName, hintOnError);
                    return;
                }
                */
                if(nextTask != null) {
                    clusterService.submitStateUpdateTask(nextTask.taskName, nextTask);
                } else {
                    listener.onResponse(newResponse(true));
                }
            } else {
                LOGGER.error("Couldn't execute task {}, {}", taskName, hintOnError);
                listener.onFailure(e);
            }
        }

        @Override
        protected Object newResponse(boolean acknowledged) {
            return new AcknowledgedResponse(acknowledged);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            return stateUpdater.apply(currentState);
        }
    }
}

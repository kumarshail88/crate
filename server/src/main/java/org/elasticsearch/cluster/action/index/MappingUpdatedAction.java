/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.action.index;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.Mapping;

import io.crate.common.unit.TimeValue;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;

/**
 * Called by shards in the cluster when their mapping was dynamically updated and it needs to be updated
 * in the cluster state meta data (and broadcast to all members).
 */
public class MappingUpdatedAction {

    public static final Setting<TimeValue> INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("indices.mapping.dynamic_timeout", TimeValue.timeValueSeconds(30),
            Property.Dynamic, Property.NodeScope);

    private IndicesAdminClient client;
    private volatile TimeValue dynamicMappingUpdateTimeout;

    @Inject
    public MappingUpdatedAction(Settings settings, ClusterSettings clusterSettings) {
        this.dynamicMappingUpdateTimeout = INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING, this::setDynamicMappingUpdateTimeout);
    }

    private void setDynamicMappingUpdateTimeout(TimeValue dynamicMappingUpdateTimeout) {
        this.dynamicMappingUpdateTimeout = dynamicMappingUpdateTimeout;
    }


    public void setClient(Client client) {
        this.client = client.admin().indices();
    }

    /**
     * Update mappings synchronously on the master node, waiting for at most
     * {@code timeout}. When this method returns successfully mappings have
     * been applied to the master node and propagated to data nodes.
     */
    public void updateMappingOnMaster(Index index, Mapping mappingUpdate, ActionListener<Void> listener) {
        client.preparePutMapping().setConcreteIndex(index).setSource(mappingUpdate.toString(), XContentType.JSON)
            .setMasterNodeTimeout(dynamicMappingUpdateTimeout).setTimeout(TimeValue.ZERO)
            .execute(new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(Exceptions.toException(SQLExceptions.unwrap(e)));
                }
            });
    }
}

/*
 * (C) Copyright 2017 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Kevin Leturc
 */
package org.nuxeo.mongodb.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.repository.RepositoryService;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.model.ComponentContext;
import org.nuxeo.runtime.model.ComponentInstance;
import org.nuxeo.runtime.model.DefaultComponent;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * Component used to get a database connection to MongoDB. Don't expose {@link MongoClient} directly, because it's this
 * component which is responsible to create and close it.
 *
 * @since 9.1
 */
public class MongoDBComponent extends DefaultComponent {

    private static final Log log = LogFactory.getLog(MongoDBComponent.class);

    private static final String EP_CONNECTION = "connection";

    private MongoDBConnectionConfig connectionConfig;

    private MongoClient client;

    @Override
    public void registerContribution(Object contribution, String extensionPoint, ComponentInstance contributor) {
        switch (extensionPoint) {
        case EP_CONNECTION:
            connectionConfig = (MongoDBConnectionConfig) contribution;
            log.info("Registering connection configuration: " + connectionConfig + ", loaded from "
                    + contributor.getName());
            break;
        default:
            throw new IllegalStateException("Invalid EP: " + extensionPoint);
        }
    }

    @Override
    public void applicationStarted(ComponentContext context) {
        log.info("Activate MongoDB component");
        client = MongoDBConnectionHelper.newMongoClient(connectionConfig.getServer());
    }

    @Override
    public void deactivate(ComponentContext context) {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public int getApplicationStartedOrder() {
        RepositoryService component = (RepositoryService) Framework.getRuntime().getComponent(
                "org.nuxeo.ecm.core.repository.RepositoryServiceComponent");
        return component.getApplicationStartedOrder() + 1;
    }

    /**
     * @return the database configured by {@link MongoDBConnectionConfig}.
     */
    public MongoDatabase getDatabase() {
        if (client == null) {
            throw new NuxeoException(
                    "You need a connection contribution to MongoDBComponent in order to get a database connection");
        }
        return MongoDBConnectionHelper.getDatabase(client, connectionConfig.getDbname());
    }

}

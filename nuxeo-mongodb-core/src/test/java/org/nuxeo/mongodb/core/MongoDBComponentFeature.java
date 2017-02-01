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

import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.bson.Document;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.repository.RepositoryService;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.model.URLStreamRef;
import org.nuxeo.runtime.test.runner.ConditionalIgnoreRule;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.RuntimeFeature;
import org.nuxeo.runtime.test.runner.RuntimeHarness;
import org.nuxeo.runtime.test.runner.SimpleFeature;
import org.osgi.framework.Bundle;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * @since 9.1
 */
// @Deploy({ "org.nuxeo.runtime.metrics"})
@Deploy({ "org.nuxeo.mongodb.core", "org.nuxeo.mongodb.core.test" })
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
@ConditionalIgnoreRule.Ignore(condition = IgnoreNoMongoDB.class, cause = "Needs a MongoDB server!")
public class MongoDBComponentFeature extends SimpleFeature {

    @Override
    public void start(FeaturesRunner runner) {
        // We need to deploy the bundle like this to have the same context and properties as StorageConfiguration
        try {
            RuntimeHarness harness = runner.getFeature(RuntimeFeature.class).getHarness();
            Bundle bundle = harness.getOSGiAdapter().getRegistry().getBundle("org.nuxeo.mongodb.core.test");
            URL auditContribUrl = bundle.getEntry("OSGI-INF/mongodb-core-test-contrib.xml");
            harness.getContext().deploy(new URLStreamRef(auditContribUrl));
        } catch (IOException e) {
            throw new NuxeoException(e);
        }
    }

    @Override
    public void afterTeardown(FeaturesRunner runner) {
        // We need to clean all collections except the one from repositories
        List<String> repositoryNames = Framework.getService(RepositoryService.class).getRepositoryNames();
        // Clean MongoDB database
        // Get a connection to MongoDB
        MongoDBComponent mongoComponent = (MongoDBComponent) Framework.getRuntime().getComponent(
                "org.nuxeo.mongodb.core.MongoDBComponent");
        // Get database
        MongoDatabase database = mongoComponent.getDatabase();
        for (String collName : database.listCollections().map(doc -> doc.get("name").toString())) {
            // Filter out collections used by repositories (eg: 'default' and 'default.counters')
            if (repositoryNames.stream().anyMatch(collName::startsWith)) {
                continue;
            }
            MongoCollection<Document> collection = database.getCollection(collName);
            collection.drop();
        }
    }

}

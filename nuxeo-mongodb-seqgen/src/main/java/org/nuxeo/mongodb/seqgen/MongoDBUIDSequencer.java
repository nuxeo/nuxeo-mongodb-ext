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
package org.nuxeo.mongodb.seqgen;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.nuxeo.ecm.core.uidgen.AbstractUIDSequencer;
import org.nuxeo.ecm.core.uidgen.UIDSequencer;
import org.nuxeo.mongodb.core.MongoDBComponent;
import org.nuxeo.mongodb.core.MongoDBSerializationHelper;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.services.config.ConfigurationService;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Updates;

/**
 * MongoDB implementation of {@link UIDSequencer}.
 * <p>
 * We use MongoDB upsert feature to provide a sequencer.
 *
 * @since 9.1
 */
public class MongoDBUIDSequencer extends AbstractUIDSequencer {

    public static final String SEQUENCE_DATABASE_ID = "sequence";

    public static final String COLLECTION_NAME_PROPERTY = "nuxeo.mongodb.seqgen.collection.name";

    public static final String DEFAULT_COLLECTION_NAME = "sequence";

    public static final Long ONE = 1L;

    public static final String SEQUENCE_VALUE_FIELD = "sequence";

    private MongoCollection<Document> coll;

    @Override
    public void init() {
        getCollection();

    }

    private MongoCollection<Document> getCollection() {
        if (coll == null) {
            // Get collection name
            ConfigurationService configurationService = Framework.getService(ConfigurationService.class);
            String collName = configurationService.getProperty(COLLECTION_NAME_PROPERTY, DEFAULT_COLLECTION_NAME);
            // Get a connection to MongoDB
            MongoDBComponent mongoComponent = (MongoDBComponent) Framework.getRuntime().getComponent(
                    "org.nuxeo.mongodb.core.MongoDBComponent");
            // Get database
            MongoDatabase database = mongoComponent.getDatabase(SEQUENCE_DATABASE_ID);
            // Get collection
            coll = database.getCollection(collName);
        }
        return coll;
    }

    @Override
    public int getNext(String key) {
        return (int) getNextLong(key);
    }

    @Override
    public long getNextLong(String key) {
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().upsert(true);
        Bson filter = Filters.eq(MongoDBSerializationHelper.MONGODB_ID, key);
        Bson update = Updates.combine(Updates.inc(SEQUENCE_VALUE_FIELD, ONE),
                Updates.setOnInsert(MongoDBSerializationHelper.MONGODB_ID, key));
        Document sequence = getCollection().findOneAndUpdate(filter, update, options);
        // If sequence is null, then it means we just create it - convert null to 1
        if (sequence == null) {
            return 1L;
        }
        // As we retrieve the document before the update we need to add 1 manually
        return (long) MongoDBSerializationHelper.bsonToFieldMap(sequence).get(SEQUENCE_VALUE_FIELD) + 1;
    }

    @Override
    public void dispose() {
        if (coll != null) {
            coll = null;
        }
    }

}

/*
 * (C) Copyright 2017 Nuxeo (http://nuxeo.com/) and others.
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
 *     Funsho David
 *
 */

package org.nuxeo.directory.mongodb;

import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.nuxeo.common.xmap.annotation.XNode;
import org.nuxeo.common.xmap.annotation.XObject;
import org.nuxeo.ecm.core.schema.types.SchemaImpl;
import org.nuxeo.ecm.core.schema.types.primitives.StringType;
import org.nuxeo.ecm.directory.AbstractReference;
import org.nuxeo.ecm.directory.BaseDirectoryDescriptor;
import org.nuxeo.ecm.directory.DirectoryCSVLoader;
import org.nuxeo.ecm.directory.DirectoryException;
import org.nuxeo.mongodb.core.MongoDBSerializationHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @since 9.1
 */
@XObject("reference")
public class MongoDBReference extends AbstractReference implements Cloneable {

    @XNode("@collection")
    protected String collection;

    @XNode("@sourceField")
    protected String sourceField;

    @XNode("@targetField")
    protected String targetField;

    @XNode("@dataFile")
    protected String dataFileName;

    private boolean initialized = false;

    @XNode("@field")
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    @XNode("@directory")
    public void setTargetDirectoryName(String targetDirectoryName) {
        this.targetDirectoryName = targetDirectoryName;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getSourceField() {
        return sourceField;
    }

    public void setSourceField(String sourceField) {
        this.sourceField = sourceField;
    }

    public String getTargetField() {
        return targetField;
    }

    public void setTargetField(String targetField) {
        this.targetField = targetField;
    }

    @Override
    public void addLinks(String sourceId, List<String> targetIds) throws DirectoryException {
        if (targetIds == null) {
            return;
        }
        try (MongoDBSession session = getMongoDBSession()) {
            addLinks(sourceId, targetIds, session);
        }
    }

    public void addLinks(String sourceId, List<String> targetIds, MongoDBSession session) throws DirectoryException {
        if (targetIds == null) {
            return;
        }
        targetIds.forEach(targetId -> addLink(sourceId, targetId, session));
    }

    @Override
    public void addLinks(List<String> sourceIds, String targetId) throws DirectoryException {
        if (sourceIds == null) {
            return;
        }
        try (MongoDBSession session = getMongoDBSession()) {
            addLinks(sourceIds, targetId, session);
        }
    }

    public void addLinks(List<String> sourceIds, String targetId, MongoDBSession session) throws DirectoryException {
        if (sourceIds == null) {
            return;
        }
        sourceIds.forEach(sourceId -> addLink(sourceId, targetId, session));
    }

    private void addLink(String sourceId, String targetId, MongoDBSession session) throws DirectoryException {
        try {
            Map<String, Object> fieldMap = new HashMap<>();
            fieldMap.put(sourceField, sourceId);
            fieldMap.put(targetField, targetId);
            Document newDoc = MongoDBSerializationHelper.fieldMapToBson(fieldMap);
            if (session.getCollection(collection).count(newDoc) > 0) {
                return;
            }
            session.getCollection(collection).insertOne(newDoc);
        } catch (MongoWriteException e) {
            throw new DirectoryException(e);
        }
    }

    @Override
    public void removeLinksForSource(String sourceId) throws DirectoryException {
        try (MongoDBSession session = getMongoDBSession()) {
            removeLinksForSource(sourceId, session);
        }
    }

    public void removeLinksForSource(String sourceId, MongoDBSession session) {
        removeLinksFor(sourceField, sourceId, session);
    }

    @Override
    public void removeLinksForTarget(String targetId) throws DirectoryException {
        try (MongoDBSession session = getMongoDBSession()) {
            removeLinksFor(targetField, targetId, session);
        }
    }

    private void removeLinksFor(String field, String value, MongoDBSession session) {
        try {
            DeleteResult result = session.getCollection(collection)
                                         .deleteMany(MongoDBSerializationHelper.fieldMapToBson(field, value));
            if (!result.wasAcknowledged()) {
                throw new DirectoryException(
                        "Error while deleting the entry, the request has not been acknowledged by the server");
            }
        } catch (MongoWriteException e) {
            throw new DirectoryException(e);
        }
    }

    @Override
    public List<String> getTargetIdsForSource(String sourceId) throws DirectoryException {
        try (MongoDBSession session = getMongoDBSession()) {
            return getIdsFor(sourceField, sourceId, targetField, session);
        }
    }

    public List<String> getTargetIdsForSource(String sourceId, MongoDBSession session) throws DirectoryException {
        return getIdsFor(sourceField, sourceId, targetField, session);
    }

    @Override
    public List<String> getSourceIdsForTarget(String targetId) throws DirectoryException {
        try (MongoDBSession session = getMongoDBSession()) {
            return getIdsFor(targetField, targetId, sourceField, session);
        }
    }

    private List<String> getIdsFor(String queryField, String value, String resultField, MongoDBSession session) {
        FindIterable<Document> docs = session.getCollection(collection)
                                             .find(MongoDBSerializationHelper.fieldMapToBson(queryField, value));
        List<String> ids = StreamSupport.stream(docs.spliterator(), false)
                                        .map(doc -> doc.getString(resultField))
                                        .collect(Collectors.toList());
        return ids;
    }

    @Override
    public void setTargetIdsForSource(String sourceId, List<String> targetIds) throws DirectoryException {
        try (MongoDBSession session = getMongoDBSession()) {
            setTargetIdsForSource(sourceId, targetIds, session);
        }
    }

    public void setTargetIdsForSource(String sourceId, List<String> targetIds, MongoDBSession session)
            throws DirectoryException {
        setIdsFor(sourceField, sourceId, targetField, targetIds, session);
    }

    @Override
    public void setSourceIdsForTarget(String targetId, List<String> sourceIds) throws DirectoryException {
        try (MongoDBSession session = getMongoDBSession()) {
            setIdsFor(targetField, targetId, sourceField, sourceIds, session);
        }
    }

    private void setIdsFor(String field, String value, String fieldToUpdate, List<String> ids, MongoDBSession session) {

        Set<String> idsToAdd = new HashSet<>();
        if (ids != null) {
            idsToAdd.addAll(ids);
        }
        List<String> idsToDelete = new ArrayList<>();

        List<String> existingIds = getIdsFor(field, value, fieldToUpdate, session);
        for (String id : existingIds) {
            if (idsToAdd.contains(id)) {
                idsToAdd.remove(id);
            } else {
                idsToDelete.add(id);
            }
        }

        if (!idsToDelete.isEmpty()) {
            idsToDelete.forEach(id -> removeLinksFor(fieldToUpdate, id, session));
        }

        if (!idsToAdd.isEmpty()) {
            if (sourceField.equals(field)) {
                idsToAdd.forEach(id -> addLink(field, id, session));
            } else {
                idsToAdd.forEach(id -> addLink(id, field, session));
            }
        }
    }

    @Override
    public MongoDBReference clone() {
        MongoDBReference clone = (MongoDBReference) super.clone();
        return clone;
    }

    protected MongoDBSession getMongoDBSession() throws DirectoryException {
        if (!initialized && dataFileName != null) {
            try (MongoDBSession session = (MongoDBSession) getSourceDirectory().getSession()) {
                // fake schema for DirectoryCSVLoader.loadData
                SchemaImpl schema = new SchemaImpl(collection, null);
                schema.addField(sourceField, StringType.INSTANCE, null, 0, Collections.emptySet());
                schema.addField(targetField, StringType.INSTANCE, null, 0, Collections.emptySet());
                DirectoryCSVLoader.loadData(dataFileName, BaseDirectoryDescriptor.DEFAULT_DATA_FILE_CHARACTER_SEPARATOR,
                        schema, map -> session.createEntry(collection, map));
                initialized = true;
            }
        }
        return (MongoDBSession) getSourceDirectory().getSession();
    }

}

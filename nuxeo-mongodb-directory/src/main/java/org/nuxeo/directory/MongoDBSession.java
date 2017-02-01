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
 *     Funsho David
 *
 */

package org.nuxeo.directory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.Document;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.api.impl.DocumentModelListImpl;
import org.nuxeo.ecm.core.api.model.Property;
import org.nuxeo.ecm.core.api.security.SecurityConstants;
import org.nuxeo.ecm.core.schema.types.Field;
import org.nuxeo.ecm.directory.BaseDirectoryDescriptor.SubstringMatchType;
import org.nuxeo.ecm.directory.BaseSession;
import org.nuxeo.ecm.directory.DirectoryException;
import org.nuxeo.ecm.directory.EntrySource;
import org.nuxeo.ecm.directory.PasswordHelper;
import org.nuxeo.ecm.directory.Session;
import org.nuxeo.mongodb.core.MongoDBConnectionHelper;
import org.nuxeo.mongodb.core.MongoDBSerializationHelper;

import com.mongodb.MongoClient;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * MongoDB implementation of a {@link Session}
 * 
 * @since 9.1
 */
public class MongoDBSession extends BaseSession implements EntrySource {

    public static final String MONGODB_SET = "$set";

    protected MongoClient client;

    protected String dbName;

    protected String schemaName;

    protected String directoryName;

    protected SubstringMatchType substringMatchType;

    protected final Map<String, Field> schemaFieldMap;

    public MongoDBSession(MongoDBDirectory directory) {
        super(directory);
            client = MongoDBConnectionHelper.newMongoClient(directory.getDescriptor().getServerUrl());
            dbName = directory.getDescriptor().getDatabaseName();
            directoryName = directory.getName();
            schemaName = directory.getSchema();
            substringMatchType = directory.getDescriptor().getSubstringMatchType();
            schemaFieldMap = directory.getSchemaFieldMap();
    }

    @Override
    public MongoDBDirectory getDirectory() {
        return (MongoDBDirectory) directory;
    }

    @Override
    public DocumentModel getEntry(String id) throws DirectoryException {
        return getEntry(id, true);
    }

    @Override
    public DocumentModel getEntry(String id, boolean fetchReferences) throws DirectoryException {
        if (!hasPermission(SecurityConstants.READ)) {
            return null;
        }
        return directory.getCache().getEntry(id, this, fetchReferences);
    }

    @Override
    public DocumentModelList getEntries() throws DirectoryException {
        if (!hasPermission(SecurityConstants.READ)) {
            return new DocumentModelListImpl();
        }
        return query(Collections.emptyMap());
    }

    @Override
    public DocumentModel createEntry(Map<String, Object> fieldMap) throws DirectoryException {
        checkPermission(SecurityConstants.WRITE);
        String id = String.valueOf(fieldMap.get(getIdField()));
        if (hasEntry(id)) {
            throw new DirectoryException(String.format("Entry with id %s already exists", id));
        }
        try {
            Document bson = MongoDBSerializationHelper.fieldMapToBson(fieldMap);
            getCollection().insertOne(bson);
            DocumentModel docModel = BaseSession.createEntryModel(null, schemaName, id, fieldMap, isReadOnly());
            return docModel;
        } catch (MongoWriteException e) {
            throw new DirectoryException(e);
        }
    }

    @Override
    public void updateEntry(DocumentModel docModel) throws DirectoryException {
        checkPermission(SecurityConstants.WRITE);
        Map<String, Object> fieldMap = new HashMap<>();

        for (String fieldName : schemaFieldMap.keySet()) {
            Property prop = docModel.getPropertyObject(schemaName, fieldName);
            if (prop != null && prop.isDirty()) {
                fieldMap.put(prop.getName(), prop.getValue());
            }
        }

        String id = docModel.getId();
        Document bson = MongoDBSerializationHelper.fieldMapToBson(getIdField(), id);

        Document props = new Document();
        props.append(MONGODB_SET, MongoDBSerializationHelper.fieldMapToBson(fieldMap));

        try {
            UpdateResult result = getCollection().updateOne(bson, props);
            // Throw an error if no document matched the update
            if (!result.wasAcknowledged()) {
                throw new DirectoryException(
                        "Error while updating the entry, the request has not been acknowledged by the server");
            }
            if (result.getMatchedCount() == 0) {
                throw new DirectoryException(
                        String.format("Error while updating the entry, no document was found with the id %s", id));
            }
        } catch (MongoWriteException e) {
            throw new DirectoryException(e);
        }
    }

    @Override
    public void deleteEntry(DocumentModel docModel) throws DirectoryException {
        deleteEntry(docModel.getId());
    }

    @Override
    public void deleteEntry(String id) throws DirectoryException {
        checkPermission(SecurityConstants.WRITE);
        checkDeleteConstraints(id);
        try {
            DeleteResult result = getCollection().deleteOne(
                    MongoDBSerializationHelper.fieldMapToBson(getIdField(), id));
            if (!result.wasAcknowledged()) {
                throw new DirectoryException(
                        "Error while deleting the entry, the request has not been acknowledged by the server");
            }
        } catch (MongoWriteException e) {
            throw new DirectoryException(e);
        }

    }

    @Override
    public void deleteEntry(String id, Map<String, String> map) throws DirectoryException {
        deleteEntry(id);
    }

    @Override
    public DocumentModelList query(Map<String, Serializable> filter) throws DirectoryException {
        return query(filter, Collections.emptySet());
    }

    @Override
    public DocumentModelList query(Map<String, Serializable> filter, Set<String> fulltext) throws DirectoryException {
        return query(filter, fulltext, new HashMap<>());
    }

    @Override
    public DocumentModelList query(Map<String, Serializable> filter, Set<String> fulltext, Map<String, String> orderBy)
            throws DirectoryException {
        return query(filter, fulltext, orderBy, false);
    }

    @Override
    public DocumentModelList query(Map<String, Serializable> filter, Set<String> fulltext, Map<String, String> orderBy,
            boolean fetchReferences) throws DirectoryException {
        return query(filter, fulltext, orderBy, fetchReferences, -1, -1);
    }

    @Override
    public DocumentModelList query(Map<String, Serializable> filter, Set<String> fulltext, Map<String, String> orderBy,
            boolean fetchReferences, int limit, int offset) throws DirectoryException {

        Document bson = buildQuery(filter, fulltext);

        DocumentModelList entries = new DocumentModelListImpl();

        FindIterable<Document> results = getCollection().find(bson).skip(offset);
        if (limit > 0) {
            results.limit(limit);
        }
        for (Document resultDoc : results) {
            // Cast object to document model
            Map<String, Object> fieldMap = MongoDBSerializationHelper.bsonToFieldMap(resultDoc);
            entries.add(fieldMapToDocumentModel(fieldMap));
        }

        if (orderBy != null && !orderBy.isEmpty()) {
            getDirectory().orderEntries(entries, orderBy);
        }

        return entries;
    }

    protected Document buildQuery(Map<String, Serializable> fieldMap, Set<String> fulltext) {

        Document bson = new Document();
        for (Map.Entry<String, Serializable> entry : fieldMap.entrySet()) {
            Object value = MongoDBSerializationHelper.valueToBson(entry.getValue());
            if (value != null) {
                String key = entry.getKey();
                if (fulltext.contains(key)) {
                    String val = String.valueOf(value);
                    switch (substringMatchType) {
                    case subany:
                        addField(bson, key, Pattern.compile(val, Pattern.CASE_INSENSITIVE));
                        break;
                    case subinitial:
                        addField(bson, key, Pattern.compile('^' + val, Pattern.CASE_INSENSITIVE));
                        break;
                    case subfinal:
                        addField(bson, key, Pattern.compile(val + '$', Pattern.CASE_INSENSITIVE));
                        break;
                    }
                } else {
                    addField(bson, key, value);
                }
            }
        }
        return bson;
    }

    protected void addField(Document bson, String key, Object value) {
        bson.put(key, value);
    }

    @Override
    public void close() throws DirectoryException {
        client.close();
    }

    @Override
    public List<String> getProjection(Map<String, Serializable> filter, String columnName) throws DirectoryException {
        return getProjection(filter, Collections.emptySet(), columnName);
    }

    @Override
    public List<String> getProjection(Map<String, Serializable> filter, Set<String> fulltext, String columnName)
            throws DirectoryException {
        DocumentModelList docList = query(filter, fulltext);
        List<String> result = new ArrayList<>();
        for (DocumentModel docModel : docList) {
            Object obj = docModel.getProperty(schemaName, columnName);
            String propValue = String.valueOf(obj);
            result.add(propValue);
        }
        return result;
    }

    @Override
    public boolean authenticate(String username, String password) throws DirectoryException {
        Document user = getCollection().find(MongoDBSerializationHelper.fieldMapToBson(getIdField(), username)).first();
        String storedPassword = user.getString(getPasswordField());
        return PasswordHelper.verifyPassword(password, storedPassword);
    }

    @Override
    public boolean hasEntry(String id) {
        return getCollection().count(MongoDBSerializationHelper.fieldMapToBson(getIdField(), id)) > 0;
    }

    @Override
    public DocumentModel createEntry(DocumentModel documentModel) {
        return createEntry(documentModel.getProperties(schemaName));
    }

    @Override
    public DocumentModel getEntryFromSource(String id, boolean fetchReferences) throws DirectoryException {
        return query(Collections.singletonMap(getIdField(), id), Collections.emptySet(), Collections.emptyMap(),
                fetchReferences, 1, -1).get(0);

    }

    /**
     * Retrieve the collection associated to this directory
     * 
     * @return the collection
     */
    public MongoCollection<Document> getCollection() {
        return MongoDBConnectionHelper.getCollection(client, dbName, directoryName);
    }

    /**
     * Check if the MongoDB server has the collection
     * 
     * @param collection the collection name
     * @return true if the server has the collection, false otherwise
     */
    public boolean hasCollection(String collection) {
        return MongoDBConnectionHelper.hasCollection(client, dbName, collection);
    }

    protected DocumentModel fieldMapToDocumentModel(Map<String, Object> fieldMap) {
        String id = String.valueOf(fieldMap.get(getIdField()));
        DocumentModel docModel = BaseSession.createEntryModel(null, schemaName, id, fieldMap, isReadOnly());
        return docModel;
    }

}

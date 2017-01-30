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
 */

package org.nuxeo.directory;

import org.nuxeo.ecm.core.schema.SchemaManager;
import org.nuxeo.ecm.core.schema.types.Field;
import org.nuxeo.ecm.core.schema.types.Schema;
import org.nuxeo.ecm.directory.AbstractDirectory;
import org.nuxeo.ecm.directory.Directory;
import org.nuxeo.ecm.directory.DirectoryCSVLoader;
import org.nuxeo.ecm.directory.DirectoryException;
import org.nuxeo.ecm.directory.Session;
import org.nuxeo.runtime.api.Framework;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * MongoDB implementation of a {@link Directory}
 * 
 * @since 9.1
 */
public class MongoDBDirectory extends AbstractDirectory {

    protected final Map<String, Field> schemaFieldMap;

    protected boolean initialized;

    public MongoDBDirectory(MongoDBDirectoryDescriptor descriptor) {
        super(descriptor);

        // cache parameterization
        cache.setEntryCacheName(descriptor.cacheEntryName);
        cache.setEntryCacheWithoutReferencesName(descriptor.cacheEntryWithoutReferencesName);
        cache.setNegativeCaching(descriptor.negativeCaching);

        SchemaManager schemaManager = Framework.getService(SchemaManager.class);
        Schema schema = schemaManager.getSchema(getSchema());
        if (schema == null) {
            throw new DirectoryException(getSchema() + " is not a registered schema");
        }
        schemaFieldMap = new LinkedHashMap<>();
        schema.getFields().forEach(f -> schemaFieldMap.put(f.getName().getLocalName(), f));

        initialized = false;
    }

    @Override
    public MongoDBDirectoryDescriptor getDescriptor() {
        return (MongoDBDirectoryDescriptor) descriptor;
    }

    @Override
    public Session getSession() throws DirectoryException {
        MongoDBSession session = new MongoDBSession(this);
        addSession(session);
        if (!initialized && descriptor.getDataFileName() != null && !session.hasCollection(getName())) {
            Schema schema = Framework.getService(SchemaManager.class).getSchema(getSchema());
            DirectoryCSVLoader.loadData(descriptor.getDataFileName(), descriptor.getDataFileCharacterSeparator(),
                    schema, session::createEntry);
            initialized = true;
        }
        return session;
    }

    public Map<String, Field> getSchemaFieldMap() {
        return schemaFieldMap;
    }
}

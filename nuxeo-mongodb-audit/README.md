nuxeo-mongodb-audit
===================

## About

This project provides a backend based on MongoDB for Nuxeo for Audit Service.

The idea is to use MongoDB as storage backend for the Audit trail entries.

## How it works

If you install the [package marketplace](https://github.com/nuxeo/marketplace-mongodb-ext), you just need to activate mongo-audit template to make it works.

Then an MongoDB based `AuditBackend` is contributed at startup to replace the default Elasticsearch based one.

The backend use the `audit` MongoDB connection or the `default` one if not found.
Entries are stored in `audit` collection unless you override it by contributing to ConfigurationService like this:

    <extension target="org.nuxeo.runtime.ConfigurationService" point="configuration">
      <property name="nuxeo.mongodb.audit.collection.name">myCollection</property>
    </extension>

The queries and PageProviders are based on MongoDB native DSL.

## Building

To build and run the tests, simply start the Maven build:

    mvn clean install

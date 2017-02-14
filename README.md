# Nuxeo MongoDB Extension

Nuxeo MongoDB Extension is a plugin to add audit and directory features working on MongoDB.

# Installation

- From the Nuxeo Marketplace: install [Nuxeo MongoDB Extension](https://connect.nuxeo.com/nuxeo/site/marketplace/package/nuxeo-mongodb-ext).
- From the Nuxeo server web UI "Admin / Update Center / Packages from Nuxeo Marketplace
- From the command line: `nuxeoctl mp-install nuxeo-mongodb-ext`

# Code
## QA

[![Build Status](https://qa2.nuxeo.org/jenkins/buildStatus/icon?job=8.10/addons_nuxeo-mongodb-ext-8.10)](https://qa2.nuxeo.org/jenkins/job/8.10/job/addons_nuxeo-mongodb-ext-8.10/)

## Build

    mvn clean install

## Deploy (how to install build product)

Install [Nuxeo MongoDB Extension](https://connect.nuxeo.com/nuxeo/site/marketplace/package/nuxeo-mongodb-ext). Then you will have two templates:
- mongodb-audit
- mongodb-directory

Add the desired templates to enable the features.

# Contributing / Reporting issues

https://jira.nuxeo.com/browse/NXP

# Licensing
 
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)
 
# About Nuxeo
 
The [Nuxeo Platform](http://www.nuxeo.com/products/content-management-platform/) is an open source customizable and extensible content management platform for building business applications. It provides the foundation for developing [document management](http://www.nuxeo.com/solutions/document-management/), [digital asset management](http://www.nuxeo.com/solutions/digital-asset-management/), [case management application](http://www.nuxeo.com/solutions/case-management/) and [knowledge management](http://www.nuxeo.com/solutions/advanced-knowledge-base/). You can easily add features using ready-to-use addons or by extending the platform using its extension point system.

The Nuxeo Platform is developed and supported by Nuxeo, with contributions from the community.

Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with
SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris.
More information is available at [www.nuxeo.com](http://www.nuxeo.com).

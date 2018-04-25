<!--
Copyright (c) 2015-2016 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# AsterixDB for YCSB
This driver is a binfing for the This driver is a binding for the YCSB facilities to operate against a AsterixDB Server cluster. Tested on version 0.9.4.

## Quickstart

### 1. Start a AsterixDB Server
Please see [AsterixDB](https://ci.apache.org/projects/asterixdb/index.html) for more details and instructions.

### 2. Set up YCSB
You need to clone the repository and compile everything.

```
git clone git@github.com:autopear/YCSB.git
cd YCSB
git checkout asterixdb
mvn clean package -DskipTests
```

### 3. Create a AsterixDB dataset for testing
Please refer to the example in [src/main/conf/create-table-feed.sqlpp](src/main/conf/create-table-feed.sqlpp). Note if you modify the dataverse name, the number of columns, or any parameter for the feed in (src/main/conf/db.properties)[src/main/conf/db.properties], you must change the SQL++ command accordingly.

### 4. Run the Workload
Before you can actually run the workload, you need to "load" the data first.

```
bin/ycsb load asterixdb -P workloads/workloada -P asterixdb/src/main/conf/db.properties
```

Then, you can run the workload:

```
bin/ycsb run asterixdb -P workloads/workloada -P asterixdb/src/main/conf/db.properties
```

## Configuration Options
Following options can be configurable using `-p`.
To do...

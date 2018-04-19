/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class AsterixDBClient extends DB {

  public void init() throws DBException {
  }

  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.ERROR;
  }

  public Status delete(String table, String key) {
    return Status.ERROR;
  }

  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return Status.ERROR;
  }

  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.ERROR;
  }

  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return Status.ERROR;
  }

}
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

import com.yahoo.ycsb.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;

public class AsterixDBClient extends DB {

  public static final String DB_URL = "db.url";
  public static final String DB_DATAVERSE = "db.dataverse";
  public static final String DB_BATCH_SIZE = "db.batchsize";
  public static final String DB_COLUMNS = "db.columns";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "FIELD";

  private static final JSONParser PARSER = new JSONParser();

  /** URL for posting queries. */
  private String SERVICE_URL;

  /** Name of the dataverse */
  private String DATAVERSE;

  /** Batch size for insertion. If set to 1, batch insertion is disabled. */
  private long BATCH_SIZE = 1;

  /** Number of columns in the table except the primary key. */
  private int NUM_COLUMNS = 10;

  private AsterixDBConnector CONN = null;
  private boolean IS_INIT = false;

  /** All columns except the primary key. */
  private List<String> COLUMNS = new ArrayList<>();

  /** Buffer array for batch insertion and conversion to string. */
  private JSONArray BATCH_INSERTS = new JSONArray();

  @Override
  public void init() throws DBException {
    if (IS_INIT) {
      System.err.println("Client connection already initialized.");
      return;
    }

    Properties props = getProperties();
    SERVICE_URL = props.getProperty(DB_URL, "http://localhost:19002/query/service");
    DATAVERSE = props.getProperty(DB_DATAVERSE, "ycsb");
    BATCH_SIZE = Long.parseLong(props.getProperty(DB_BATCH_SIZE, "1"));
    NUM_COLUMNS = Integer.parseInt(props.getProperty(DB_COLUMNS, "10"));
    for (int i=0; i<NUM_COLUMNS; i++)
      COLUMNS.add(COLUMN_PREFIX + i);

    CONN = new AsterixDBConnector(SERVICE_URL);

    if (!CONN.isValid())
      System.err.println("Connection is invalid.");

    IS_INIT = true;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    // Construct SQL++ query
    String sql = "USE " + DATAVERSE + ";" +
        " SELECT " + String.join(", ", (fields == null ? COLUMNS : fields)) +
        " FROM " + table +
        " WHERE " + PRIMARY_KEY + " = \"" + JSONObject.escape(key) + "\";";

    // Execute query and wait to get result
    if (CONN.execute(sql)) {
      boolean hasError = false;
      String err = "";
      String line;
      while (!(line = CONN.nextResult()).isEmpty()) {
        if (!hasError) {
          // Each record in the result must be in the format of a JSON object
          if (line.startsWith("{") && line.endsWith("}")) {
            try {
              JSONObject record = (JSONObject)(PARSER.parse(line)); // Parse to JSON object to get value for given key
              if (result != null) {
                for (String col : (fields == null ? COLUMNS : fields))
                  result.put(col, new StringByteIterator(record.get(col).toString()));
              }
            } catch (ParseException ex) {
              hasError = true;
              err = ex.toString();
            }
          } else {
            hasError = true;
            err = "Unsupported record: " + line;
          }
        }
        // We continue to get lines from the HTTP response, this ensures closing internal buffers correctly
      }
      if (hasError) {
        System.err.println("Error in processing read from table: " + table + " " + err);
        return Status.ERROR;
      } else
        return Status.OK;
    } else {
      System.err.println("Error in processing read to table: " + table + " " + CONN.error());
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    // Construct SQL++ query
    String sql = "USE " + DATAVERSE + ";" +
        " SELECT " + String.join(", ", (fields == null ? COLUMNS : fields)) +
        " FROM " + table +
        " WHERE " + PRIMARY_KEY + " >= \"" + JSONObject.escape(startkey) + "\"" +
        " ORDER BY " + PRIMARY_KEY + " LIMIT " + recordcount + ";";

    // Execute query and wait to get result
    if (CONN.execute(sql)) {
      boolean hasError = false;
      String err = "";
      String line;
      while (!(line = CONN.nextResult()).isEmpty()) {
        if (!hasError) {
          // Each record in the result must be in the format of a JSON object
          if (line.startsWith("{") && line.endsWith("}")) {
            try {
              JSONObject record = (JSONObject)(PARSER.parse(line)); // Parse to JSON object to get value for given key
              if (result != null) {
                HashMap<String, ByteIterator> oneResult = new HashMap<>();
                for (String col : (fields == null ? COLUMNS : fields))
                  oneResult.put(col, new StringByteIterator(record.get(col).toString()));
                result.add(oneResult);
              }
            } catch (ParseException ex) {
              hasError = true;
              err = ex.toString();
            }
          } else {
            hasError = true;
            err = "Unsupported record: " + line;
          }
        }
        // We continue to get lines from the HTTP response, this ensures closing internal buffers correctly
      }
      if (hasError) {
        System.err.println("Error in processing read from table: " + table + " " + err);
        return Status.ERROR;
      } else
        return Status.OK;
    } else {
      System.err.println("Error in processing scan to table: " + table + " " + CONN.error());
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    // Construct SQL++ query
    String sql = "USE " + DATAVERSE + ";" +
        " DELETE FROM " + table + " WHERE " + PRIMARY_KEY + " = \"" + JSONObject.escape(key) + "\";";

    // Execute query without getting result
    if (CONN.executeUpdate(sql))
      return Status.OK;
    else {
      System.err.println("Error in processing delete from table: " + table + " " + CONN.error());
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    // Prepare a record to insert
    JSONObject statement = new JSONObject();
    statement.put(PRIMARY_KEY, key);
    for (String col : values.keySet())
      statement.put(col, values.get(col).toString());

    BATCH_INSERTS.add(statement); // Add to buffer array

    // Batch insertion or single insertion depending on BATCH_SIZE
    if (BATCH_INSERTS.size() == BATCH_SIZE) {
      // Construct SQL++ query
      String sql = "USE " + DATAVERSE + ";" +
          " INSERT INTO " + table + " (" + BATCH_INSERTS.toJSONString() + ");";
      BATCH_INSERTS.clear(); // Reset buffer array

      // Execute query without getting result
      if (CONN.executeUpdate(sql))
        return Status.OK;
      else {
        System.err.println("Error in processing insert to table: " + table + " " + CONN.error());
        return Status.ERROR;
      }
    }

    // Batching
    return Status.BATCHED_OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    // Construct a list of fields to be read from the original table, and fields to be actually updated
    List<String> attributes = new ArrayList<>();
    attributes.add(PRIMARY_KEY); // We don't update the primary key
    for (String col : COLUMNS) {
      if (values.containsKey(col)) {
        // Update the value for the field, using format "new value" AS field
        String newValue = "\"" + JSONObject.escape(values.get(col).toString()) + "\" AS " + col;
        attributes.add(newValue);
      } else
        attributes.add(col); // The field is not updated
    }

    // Construct SQL++ query
    String sql = "USE " + DATAVERSE + ";" +
        " UPSERT INTO " + table + "(SELECT " +
        String.join(", ", attributes) +
        " FROM " + table + " WHERE " + PRIMARY_KEY + " = \"" + JSONObject.escape(key) + "\"" +
        ");";

    // Execute query without getting result
    if (CONN.executeUpdate(sql))
      return Status.OK;
    else {
      System.err.println("Error in processing update to table: " + table + " " + CONN.error());
      return Status.ERROR;
    }
  }

}
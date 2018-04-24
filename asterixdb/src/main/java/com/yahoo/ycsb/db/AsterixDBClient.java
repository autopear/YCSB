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

package com.yahoo.ycsb.db.asterixdb;

import com.yahoo.ycsb.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;

/**
 * Client for AsterixDB.
 */
public class AsterixDBClient extends DB {

  public static final String DB_URL = "db.url";
  public static final String DB_DATAVERSE = "db.dataverse";
  public static final String DB_BATCHSIZE = "db.batchsize";
  public static final String DB_COLUMNS = "db.columns";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "field";

  private static final JSONParser PARSER = new JSONParser();

  private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

  /** URL for posting queries. */
  private String pServiceURL;

  /** Name of the dataverse. */
  private String pDataverse;

  /** Batch size for insertion. If set to 1, batch insertion is disabled. */
  private long pBatchSize = 1;

  /** Number of columns in the table except the primary key. */
  private int pNumCols = 10;

  private AsterixDBConnector pConn = null;
  private boolean pIsInit = false;

  /** All columns except the primary key. */
  private List<String> pCols = new ArrayList<>();

  /** Buffer array for batch insertion and conversion to string. */
  private List<String> pBatchInserts = new ArrayList<>();

  @Override
  public void init() throws DBException {
    if (pIsInit) {
      System.err.println("Client connection already initialized.");
      return;
    }

    Properties props = getProperties();
    pServiceURL = props.getProperty(DB_URL, "http://localhost:19002/query/service");
    pDataverse = props.getProperty(DB_DATAVERSE, "ycsb");
    pBatchSize = Long.parseLong(props.getProperty(DB_BATCHSIZE, "1"));
    pNumCols = Integer.parseInt(props.getProperty(DB_COLUMNS, "10"));
    for (int i=0; i<pNumCols; i++) {
      pCols.add(COLUMN_PREFIX + i);
    }

    pConn = new AsterixDBConnector(pServiceURL);

    if (!pConn.isValid()) {
      System.err.println("Connection is invalid.");
    }

    pIsInit = true;
  }

  private static String escapeString(final String str) {
    return str.replaceAll("'", "''");
  }

  private static String bytesToHex(final ByteIterator data) {
    byte[] bytes = data.toArray();
    char[] hexChars = new char[bytes.length * 2];
    for (int j=0; j<bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    // Construct SQL++ query
    String sql = "USE " + pDataverse + ";" +
        "SELECT " + PRIMARY_KEY + "," + String.join(",", (fields == null ? pCols : fields)) +
        " FROM " + table +
        " WHERE " + PRIMARY_KEY + "='" + escapeString(key) + "';";

    // Execute query and wait to get result
    if (pConn.execute(sql)) {
      boolean hasError = false;
      String err = "";
      while (true) {
        String line = pConn.nextResult();
        if (line.isEmpty()) {
          break;
        }
        if (!hasError) {
          // Each record in the result must be in the format of a JSON object
          if (line.startsWith("{") && line.endsWith("}")) {
            try {
              JSONObject record = (JSONObject)(PARSER.parse(line)); // Parse to JSON object to get value for given key
              if (result != null) {
                for (String col : (fields == null ? pCols : fields)) {
                  result.put(col, new StringByteIterator(record.get(col).toString()));
                }
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
      } else {
        return Status.OK;
      }
    } else {
      System.err.println("Error in processing read to table: " + table + " " + pConn.error());
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    // Construct SQL++ query
    String sql = "USE " + pDataverse + ";" +
        "SELECT " + PRIMARY_KEY + "," + String.join(",", (fields == null ? pCols : fields)) +
        " FROM " + table +
        " WHERE " + PRIMARY_KEY + ">='" + escapeString(startkey) + "'" +
        " ORDER BY " + PRIMARY_KEY + " LIMIT " + recordcount + ";";

    // Execute query and wait to get result
    if (pConn.execute(sql)) {
      boolean hasError = false;
      String err = "";
      while (true) {
        String line = pConn.nextResult();
        if (line.isEmpty()) {
          break;
        }
        if (!hasError) {
          // Each record in the result must be in the format of a JSON object
          if (line.startsWith("{") && line.endsWith("}")) {
            try {
              JSONObject record = (JSONObject)(PARSER.parse(line)); // Parse to JSON object to get value for given key
              if (result != null) {
                HashMap<String, ByteIterator> oneResult = new HashMap<>();
                for (String col : (fields == null ? pCols : fields)) {
                  oneResult.put(col, new StringByteIterator(record.get(col).toString()));
                }
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
      } else {
        return Status.OK;
      }
    } else {
      System.err.println("Error in processing scan to table: " + table + " " + pConn.error());
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    // Construct SQL++ query
    String sql = "USE " + pDataverse + ";" +
        "DELETE FROM " + table + " WHERE " + PRIMARY_KEY + "='" + escapeString(key) + "';";

    // Execute query without getting result
    if (pConn.executeUpdate(sql)) {
      return Status.OK;
    } else {
      System.err.println("Error in processing delete from table: " + table + " " + pConn.error());
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    // Prepare a record to insert
    String statement = "{\"" + PRIMARY_KEY + "\":\"" + JSONObject.escape(key) + "\"";
    for (String col : pCols) {
      if (values.containsKey(col)) {
        statement += (",\"" + col + "\":hex(\"" + bytesToHex(values.get(col)) + "\")");
      }
    }
    statement += "}";

    pBatchInserts.add(statement); // Add to buffer array

    // Batch insertion or single insertion depending on pBatchSize
    if (pBatchInserts.size() == pBatchSize) {
      // Construct SQL++ query
      String sql = "USE " + pDataverse + ";" +
          "INSERT INTO " + table + " ([" + String.join(",", pBatchInserts) + "]);";
      pBatchInserts.clear(); // Reset buffer array

      // Execute query without getting result
      if (pConn.executeUpdate(sql)) {
        return Status.OK;
      } else {
        System.err.println("Error in processing insert to table: " + table + " " + pConn.error());
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
    for (String col : pCols) {
      if (values.containsKey(col)) {
        // Update the value for the field, using format "new value" AS field
        String newValue = "hex(\"" + bytesToHex(values.get(col)) + "\") AS " + col;
        attributes.add(newValue);
      } else {
        attributes.add(col); // The field is not updated
      }
    }

    // Construct SQL++ query
    String sql = "USE " + pDataverse + ";" +
        "UPSERT INTO " + table + "(SELECT " +
        String.join(",", attributes) +
        " FROM " + table + " WHERE " + PRIMARY_KEY + "='" + escapeString(key) + "'" +
        ");";

    // Execute query without getting result
    if (pConn.executeUpdate(sql)) {
      return Status.OK;
    } else {
      System.err.println("Error in processing update to table: " + table + " " + pConn.error());
      return Status.ERROR;
    }
  }

}
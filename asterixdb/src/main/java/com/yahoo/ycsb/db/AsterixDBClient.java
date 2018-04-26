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
import org.apache.commons.validator.routines.UrlValidator;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

import static org.apache.commons.validator.routines.UrlValidator.ALLOW_LOCAL_URLS;

/**
 * Client for AsterixDB.
 */
public class AsterixDBClient extends DB {

  public static final String DB_URL = "db.url";
  public static final String DB_DATAVERSE = "db.dataverse";
  public static final String DB_DATASET = "db.dataset";
  public static final String DB_BATCHSIZE = "db.batchsize";
  public static final String DB_COLUMNS = "db.columns";
  public static final String DB_UPSERT = "db.upsertenabled";
  public static final String DB_FEEDENABLED = "db.feedenabled";
  public static final String DB_FEEDHOST = "db.feedhost";
  public static final String DB_FEEDPORT = "db.feedport";
  public static final String PRINTCMD = "printcmd";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "field";

  /** A global JSON parser. */
  private static final JSONParser PARSER = new JSONParser();

  /** Array for converting bytes to Hexadecimal string. */
  private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

  /** URL for posting queries. */
  private String pServiceURL;

  /** Name of the dataverse. */
  private String pDataverse;

  /** Name of the dataset. */
  private String pDataset;

  /** Batch size for insertion. If set to 1, batch insertion is disabled. */
  private long pBatchSize = 1;

  /** Number of columns in the table except the primary key. */
  private int pNumCols = 10;

  /** Number of columns in the table except the primary key. */
  private boolean pUpsert = false;

  /** Whether to use SocketFeed for data ingestion. */
  private boolean pFeedEnabled = false;

  /** Hostname or IP for SocketFeed. */
  private String pFeedHost = "";

  /** Port for SocketFeed. */
  private int pFeedPort = -1;

  /** Print SQL++ command. */
  private boolean pPrintCmd = false;

  private Socket pFeedSock = null;
  private PrintWriter pFeedWriter = null;

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
    pDataset = props.getProperty(DB_DATASET, "");
    pBatchSize = Long.parseLong(props.getProperty(DB_BATCHSIZE, "1"));
    pNumCols = Integer.parseInt(props.getProperty(DB_COLUMNS, "10"));
    pUpsert = (props.getProperty(DB_UPSERT, "false").compareTo("true") == 0);
    pFeedEnabled = (props.getProperty(DB_FEEDENABLED, "false").compareTo("true") == 0);
    pFeedHost = props.getProperty(DB_FEEDHOST, "");
    pFeedPort = Integer.parseInt(props.getProperty(DB_FEEDPORT, "-1"));
    pPrintCmd = (props.getProperty(PRINTCMD, "false").compareTo("true") == 0);
    for (int i = 0; i < pNumCols; i++) {
      pCols.add(COLUMN_PREFIX + i);
    }

    if (pFeedEnabled) {
      if (pFeedPort > 65535 || pFeedPort < 0) {
        System.err.println("Invalid port " + pFeedPort + ".");
      } else {
        String feedURL = "http://" + pFeedHost + ":" + pFeedPort + "/";
        String[] schemes = {"http"};
        UrlValidator urlValidator = new UrlValidator(schemes, ALLOW_LOCAL_URLS);
        if (urlValidator.isValid(feedURL)) {
          try {
            pFeedSock = new Socket(pFeedHost, pFeedPort);
            pFeedSock.setKeepAlive(true);
            pFeedWriter = new PrintWriter(pFeedSock.getOutputStream());
          } catch (UnknownHostException ex) {
            System.err.println("Invalid feed configuration " + ex.toString());
            pFeedSock = null;
            pFeedWriter = null;
          } catch (IOException ex) {
            System.err.println("Error creating SocketFeed " + ex.toString());
            pFeedSock = null;
            pFeedWriter = null;
          }
        } else {
          System.err.println("Invalid hostname \"" + pFeedHost + "\" or invalid port " + pFeedPort + ".");
        }
      }
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
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String actualTable = pDataset.isEmpty() ? table : pDataset;

    // Construct SQL++ query
    String sql = "USE " + pDataverse + ";" +
        "SELECT " + PRIMARY_KEY + "," + String.join(",", (fields == null ? pCols : fields)) +
        " FROM " + actualTable +
        " WHERE " + PRIMARY_KEY + "='" + escapeString(key) + "';";

    if (pPrintCmd) {
      System.out.println("READ: " + sql);
    }

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
              JSONObject record = (JSONObject) (PARSER.parse(line)); // Parse to JSON object to get value for given key
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
        System.err.println("Error in processing read from table: " + actualTable + " " + err);
        return Status.ERROR;
      } else {
        return Status.OK;
      }
    } else {
      System.err.println("Error in processing read to table: " + actualTable + " " + pConn.error());
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    String actualTable = pDataset.isEmpty() ? table : pDataset;

    // Construct SQL++ query
    String sql = "USE " + pDataverse + ";" +
        "SELECT " + PRIMARY_KEY + "," + String.join(",", (fields == null ? pCols : fields)) +
        " FROM " + actualTable +
        " WHERE " + PRIMARY_KEY + ">='" + escapeString(startkey) + "'" +
        " ORDER BY " + PRIMARY_KEY + " LIMIT " + recordcount + ";";

    if (pPrintCmd) {
      System.out.println("SCAN: " + sql);
    }

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
              JSONObject record = (JSONObject) (PARSER.parse(line)); // Parse to JSON object to get value for given key
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
        System.err.println("Error in processing read from table: " + actualTable + " " + err);
        return Status.ERROR;
      } else {
        return Status.OK;
      }
    } else {
      System.err.println("Error in processing scan to table: " + actualTable + " " + pConn.error());
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    String actualTable = pDataset.isEmpty() ? table : pDataset;

    // Construct SQL++ query
    String sql = "USE " + pDataverse + ";" +
        "DELETE FROM " + actualTable + " WHERE " + PRIMARY_KEY + "='" + escapeString(key) + "';";

    if (pPrintCmd) {
      System.out.println("DELETE: " + sql);
    }

    // Execute query without getting result
    if (pConn.executeUpdate(sql)) {
      return Status.OK;
    } else {
      System.err.println("Error in processing delete from table: " + actualTable + " " + pConn.error());
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

    if (pFeedWriter == null) {
      String actualTable = pDataset.isEmpty() ? table : pDataset;

      pBatchInserts.add(statement); // Add to buffer array

      // Batch insertion or single insertion depending on pBatchSize
      if (pBatchInserts.size() == pBatchSize) {
        // Construct SQL++ query
        String sql = "USE " + pDataverse + ";" +
            (pUpsert ? "UPSERT" : "INSERT") +
            " INTO " + actualTable + " ([" + String.join(",", pBatchInserts) + "]);";
        pBatchInserts.clear(); // Reset buffer array

        if (pPrintCmd) {
          System.out.println("INSERT: " + sql);
        }

        // Execute query without getting result
        if (pConn.executeUpdate(sql)) {
          return Status.OK;
        } else {
          System.err.println("Error in processing insert to table: " + actualTable + " " + pConn.error());
          return Status.ERROR;
        }
      }

      // Batching
      return Status.BATCHED_OK;
    } else {
      if (pPrintCmd) {
        System.out.println("INSERT: " + statement);
      }

      pFeedWriter.write(statement);
      if (pFeedWriter.checkError()) {
        System.err.println("Error in processing insert to table: " + (pDataset.isEmpty() ? table : pDataset));
        return Status.ERROR;
      } else {
        return Status.OK;
      }
    }
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

    String actualTable = pDataset.isEmpty() ? table : pDataset;

    // Construct SQL++ query, use UPSERT to simulate UPDATE
    String sql = "USE " + pDataverse + ";" +
        "UPSERT INTO " + actualTable + " (SELECT " +
        String.join(",", attributes) +
        " FROM " + actualTable + " WHERE " + PRIMARY_KEY + "='" + escapeString(key) + "'" +
        ");";

    if (pPrintCmd) {
      System.out.println("UPDATE: " + sql);
    }

    // Execute query without getting result
    if (pConn.executeUpdate(sql)) {
      return Status.OK;
    } else {
      System.err.println("Error in processing update to table: " + actualTable + " " + pConn.error());
      return Status.ERROR;
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (pFeedWriter != null) {
      pFeedWriter.flush();
      pFeedWriter.close();
      try {
        pFeedSock.close();
      } catch (IOException ex) {
        // pass
      }
    }
  }

}
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
  public static final String DB_DATASET = "table";
  public static final String DB_BATCHINSERTS = "db.batchinserts";
  public static final String DB_BATCHUPDATES = "db.batchupdates";
  public static final String DB_UPSERT = "db.upsertenabled";
  public static final String DB_FEEDENABLED = "db.feedenabled";
  public static final String DB_FEEDHOST = "db.feedhost";
  public static final String DB_FEEDPORT = "db.feedport";
  public static final String PRINTCMD = "printcmd";

  /** Array for converting bytes to Hexadecimal string. */
  private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

  /** URL for posting queries. */
  private String pServiceURL;

  /** Name of the dataverse. */
  private String pDataverse;

  /** Name of the dataset. */
  private String pDataset;

  /** Batch size for insertion. If set to 1, batch insertion is disabled. */
  private long pBatchInserts = 1;

  /** Batch size for updates. */
  private long pBatchUpdates = 1;

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

  /** The primary key in the table. */
  private String pPK;

  /** All fields except the primary key. */
  private List<String> pFields = new ArrayList<>();

  /** Buffer array for batch insertion and conversion to string. */
  private List<String> pInsertStatements = new ArrayList<>();

  /** Buffer array for batch update and conversion to string. */
  private List<String> pUpdateStatements = new ArrayList<>();

  @Override
  public void init() throws DBException {
    if (pIsInit) {
      System.err.println("Client connection already initialized.");
      return;
    }

    Properties props = getProperties();
    pServiceURL = props.getProperty(DB_URL, "http://localhost:19002/query/service");
    pDataverse = props.getProperty(DB_DATAVERSE, "ycsb");
    pDataset = props.getProperty(DB_DATASET, "usertable");
    pBatchInserts = Long.parseLong(props.getProperty(DB_BATCHINSERTS, "1"));
    pBatchUpdates = Long.parseLong(props.getProperty(DB_BATCHUPDATES, "1"));
    pUpsert = (props.getProperty(DB_UPSERT, "false").compareTo("true") == 0);
    pFeedEnabled = (props.getProperty(DB_FEEDENABLED, "false").compareTo("true") == 0);
    pFeedHost = props.getProperty(DB_FEEDHOST, "");
    pFeedPort = Integer.parseInt(props.getProperty(DB_FEEDPORT, "-1"));
    pPrintCmd = (props.getProperty(PRINTCMD, "false").compareTo("true") == 0);

    if (!isValidName(pDataverse)) {
      throw new DBException("Invalid dataverse \"" + pDataverse + "\"");
    }

    if (!isValidName(pDataset)) {
      throw new DBException("Invalid dataset \"" + pDataset + "\"");
    }

    if (pFeedEnabled) {
      connectFeed();
    }

    pConn = new AsterixDBConnector(pServiceURL);

    if (!pConn.isValid()) {
      throw new DBException("Connection is invalid");
    }

    if (!getPrimaryKeysAndAllFields()) {
      throw new DBException("Failed to get the table's metadata");
    }

    pIsInit = true;
  }

  /** Create connection to the socket feed. */
  private void connectFeed() throws DBException  {
    if (pFeedPort > 65535 || pFeedPort < 0) {
      throw new DBException("Invalid port " + pFeedPort + ".");
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
          pFeedSock = null;
          pFeedWriter = null;
          throw new DBException("Invalid feed configuration " + ex.toString());
        } catch (IOException ex) {
          pFeedSock = null;
          pFeedWriter = null;
          throw new DBException("Error creating SocketFeed " + ex.toString());
        }
      } else {
        throw new DBException("Invalid hostname \"" + pFeedHost + "\" or invalid port " + pFeedPort);
      }
    }
  }

  /** Check if it is a valid name for dataverse or dataset. */
  private static boolean isValidName(final String name) {
    if (name.isEmpty()) {
      return false;
    } else if (name.length() == 1) {
      char c = name.charAt(0);
      return ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'));
    } else {
      char c = name.charAt(0);
      if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
        for (int i=1; i<name.length()-1; i++) {
          c = name.charAt(i);
          if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (c == '_')) {
            continue;
          } else {
            return false;
          }
        }
        c = name.charAt(name.length() - 1);
        return ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'));
      } else {
        return false;
      }
    }
  }

  /** Retrieve the primary key and all other fields from the dataset. */
  private boolean getPrimaryKeysAndAllFields() {
    if (pIsInit) {
      return false;
    }

    List<String> pks = pConn.getPrimaryKeys(pDataverse, pDataset);
    if (pks == null) {
      System.err.println("Error getting the primary key (" + pDataset + "): " + pConn.error());
      return false;
    }
    if (pks.size() > 1) {
      if (pks == null) {
        System.err.println("There must be only 1 primary key");
        return false;
      }
    }
    pPK = pks.get(0);

    Map<String, String> fields = pConn.getAllFields(pDataverse, pDataset);
    if (fields == null || fields.isEmpty()) {
      System.err.println("Error getting all the fields (" + pDataset + "): " + pConn.error());
      return false;
    }

    for (String f : fields.keySet()) {
      if (f.compareTo(pPK) != 0) {
        pFields.add(f);
      }
    }

    Collections.sort(pFields);

    return true;
  }

  /** Escape a string value to be encapsulated by single quotes. */
  private static String escapeString(final String str) {
    return str.replaceAll("'", "''");
  }

  /** Convert bytes data to hexadecimal string. */
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

  /** Convert hexadecimal string to bytes data. */
  private static byte[] hexToBytes(final String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
          + Character.digit(s.charAt(i+1), 16));
    }
    return data;
  }

  /** Print a command. */
  private void printCmd(final String operation, final String cmd) {
    if (pPrintCmd) {
      System.out.println(operation + ":\n" + cmd);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    // Construct SQL++ query
    String sql = "USE " + pDataverse + ";" +
        "SELECT " + pPK + "," + String.join(",", (fields == null ? pFields : fields)) +
        " FROM " + table +
        " WHERE " + pPK + "='" + escapeString(key) + "';";

    printCmd("READ", sql);

    // Execute query and wait to get result
    if (!pConn.execute(sql)) {
      System.err.println("Error in processing read to table (" + table + "): " + pConn.error());
      return Status.ERROR;
    }
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
            // Parse to JSON object to get value for given key
            JSONObject record = (JSONObject) (pConn.parser().parse(line));
            if (result != null) {
              for (String col : (fields == null ? pFields : fields)) {
                result.put(col, new ByteArrayByteIterator(hexToBytes(record.get(col).toString())));
              }
            }
          } catch (ParseException ex) {
            hasError = true;
            err = "ParseException: " + ex.toString();
          }
        } else if (line.isEmpty()) {
          // pass
        } else {
          hasError = true;
          err = "Unsupported record: " + line;
        }
      }
      // We continue to get lines from the HTTP response, this ensures closing internal buffers correctly
    }
    if (hasError) {
      System.err.println("Error in processing read from table (" + table + "): " + err);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    // Construct SQL++ query
    String sql = "USE " + pDataverse + ";" +
        "SELECT " + pPK + "," + String.join(",", (fields == null ? pFields : fields)) +
        " FROM " + table +
        " WHERE " + pPK + ">='" + escapeString(startkey) + "'" +
        " ORDER BY " + pPK + " LIMIT " + recordcount + ";";

    printCmd("SCAN", sql);

    // Execute query and wait to get result
    if (!pConn.execute(sql)) {
      System.err.println("Error in processing scan to table (" + table + "): " + pConn.error());
      return Status.ERROR;
    }

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
            // Parse to JSON object to get value for given key
            JSONObject record = (JSONObject) (pConn.parser().parse(line));
            if (result != null) {
              HashMap<String, ByteIterator> oneResult = new HashMap<>();
              for (String col : (fields == null ? pFields : fields)) {
                oneResult.put(col, new ByteArrayByteIterator(hexToBytes(record.get(col).toString())));
              }
              result.add(oneResult);
            }
          } catch (ParseException ex) {
            hasError = true;
            err = "ParseException: " + ex.toString();
          }
        } else if (line.isEmpty()) {
          // pass
        } else {
          hasError = true;
          err = "Unsupported record: " + line;
        }
      }
      // We continue to get lines from the HTTP response, this ensures closing internal buffers correctly
    }
    if (hasError) {
      System.err.println("Error in processing read from table (" + table + "): " + err);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    // Construct SQL++ query
    String sql = "USE " + pDataverse + ";" +
        "DELETE FROM " + table + " WHERE " + pPK + "='" + escapeString(key) + "';";

    printCmd("DELETE", sql);

    // Execute query without getting result
    if (!pConn.executeUpdate(sql)) {
      System.err.println("Error in processing delete from table (" + table + "): " + pConn.error());
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    // Prepare a record to insert
    String statement = "{\"" + pPK + "\":\"" + JSONObject.escape(key) + "\"";
    for (String col : pFields) {
      if (values.containsKey(col)) {
        statement += (",\"" + col + "\":hex(\"" + bytesToHex(values.get(col)) + "\")");
      }
    }
    statement += "}";

    if (pFeedEnabled) {
      // Insert via SocketFeed
      printCmd("FEED", statement);

      pFeedWriter.write(statement);

      if (pFeedWriter.checkError()) {
        System.err.println("Error in processing insert to table (" + (pDataset.isEmpty() ? table : pDataset)
            + ") via feed");
        try {
          connectFeed(); // Reconnect feed for retry
        } catch (DBException ex) {
          System.err.println("Error connecting to feed: " + ex.toString());
        }
        return Status.ERROR;
      }
    } else {
      // Insert via INSERT or UPSERT query
      String sql;
      if (pBatchInserts == 1) {
        // Construct SQL++ query
        sql = "USE " + pDataverse + ";" +
            (pUpsert ? "UPSERT" : "INSERT") + " INTO " + table + " ([" + statement + "]);";
      } else {
        pInsertStatements.add(statement); // Add to buffer array
        if (pInsertStatements.size() < pBatchInserts) {
          return Status.BATCHED_OK; // Batching
        }
        // Construct SQL++ query
        sql = "USE " + pDataverse + ";" +
            (pUpsert ? "UPSERT" : "INSERT") +
            " INTO " + table + " ([" + String.join(",", pInsertStatements) + "]);";
        pInsertStatements.clear(); // Reset buffer array
      }

      printCmd(pUpsert ? "UPSERT" : "INSERT", sql);

      // Execute query without getting result
      if (!pConn.executeUpdate(sql)) {
        System.err.println("Error in processing insert to table (" + table + "): " + pConn.error());
        return Status.ERROR;
      }
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    // Construct a list of fields to be read from the original table, and fields to be actually updated
    List<String> attributes = new ArrayList<>();
    attributes.add(pPK); // We don't update the primary key
    for (String col : pFields) {
      if (values.containsKey(col)) {
        // Update the value for the field, using format "new value" AS field
        String newValue = "hex(\"" + bytesToHex(values.get(col)) + "\") AS " + col;
        attributes.add(newValue);
      } else {
        attributes.add(col); // The field is not updated
      }
    }

    // Select a record and replace certain field(s)
    String statement = "SELECT " + String.join(",", attributes) +
        " FROM " + table + " WHERE " + pPK + "='" + escapeString(key) + "'";

    String sql;
    if (pBatchUpdates == 1) {
      // Construct SQL++ query, use UPSERT to simulate UPDATE
      sql = "USE " + pDataverse + ";" +
          "UPSERT INTO " + table + " (" + statement + ");";
    } else {
      pUpdateStatements.add(statement); // Add to buffer array
      if (pUpdateStatements.size() == pBatchUpdates) {
        // Construct SQL++ query, use UPSERT to simulate UPDATE
        sql = "USE " + pDataverse + ";" +
            "UPSERT INTO " + table + " (" +
            String.join(" UNION ALL ", pUpdateStatements) + ");";
        pUpdateStatements.clear(); // Reset buffer array
      } else {
        // Batching
        return Status.BATCHED_OK;
      }
    }

    printCmd("UPDATE", sql);

    // Execute query without getting result
    if (!pConn.executeUpdate(sql)) {
      System.err.println("Error in processing update to table (" + table + "): " + pConn.error());
      return Status.ERROR;
    }

    return Status.OK;
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

package site.ycsb.db.asterixdb;

import java.io.*;
import java.util.*;

import org.apache.commons.validator.routines.UrlValidator;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import static org.apache.commons.validator.routines.UrlValidator.ALLOW_LOCAL_URLS;

/**
 * Connector for AsterixDB via HTTP API.
 */
public class AsterixDBConnector {
  private CloseableHttpClient pClient = null;
  private JSONParser pParser = null;
  private String pServiceUrl = "";
  private boolean pIsValid = false;
  private String pError;
  private boolean pBeginResults = false;
  private long pElapsed = 0;
  private CloseableHttpResponse pResponse = null;
  private InputStream pStream = null;
  private InputStreamReader pStreamReader = null;
  private BufferedReader pBufferedReader = null;

  /**
   * Constructor. Http URL will be used in the format of http://address:port/query/service
   *
   * @param address Hostnme or IP of the database server.
   * @param port The port used for HTTP connection.
   */
  public AsterixDBConnector(final String address, int port) {
    pServiceUrl = "http://" + address + ":" + port + "/query/service";
    String[] schemes = {"http"};
    UrlValidator urlValidator = new UrlValidator(schemes, ALLOW_LOCAL_URLS);
    pIsValid = urlValidator.isValid(pServiceUrl);
    if (pIsValid) {
      pClient = HttpClients.createDefault();
      pParser = new JSONParser();
    }
  }

  /**
   * Constructor.
   *
   * @param serviceUrl The service URL for HTTP connection.
   */
  public AsterixDBConnector(final String serviceUrl) {
    pServiceUrl = serviceUrl;
    String[] schemes = {"http", "https"};
    UrlValidator urlValidator = new UrlValidator(schemes, ALLOW_LOCAL_URLS);
    pIsValid = urlValidator.isValid(pServiceUrl);
    if (pIsValid) {
      pClient = HttpClients.createDefault();
      pParser = new JSONParser();
    }
  }

  /**
   * @return True if the connector is valid.
   */
  public boolean isValid() {
    return pIsValid;
  }

  /**
   * @return True if an error happened in the last executed function.
   */
  public boolean hasError() {
    return !pError.isEmpty();
  }

  /**
   * @return The error message from the last executed function. Empty if no error.
   */
  public String error() {
    return pError;
  }

  /**
   * @return The time in milliseconds of the last executed function.
   */
  public long elapsedTime() {
    return pElapsed;
  }

  /**
   * @return A JSON parser which can be used by elsewhere.
   */
  public JSONParser parser() {
    return pParser;
  }

  /** Retrieve the error message from the server. */
  private String getServerError() {
    if (pResponse == null) {
      return "";
    }

    try {
      pStream = pResponse.getEntity().getContent();
    } catch (IOException ex) {
      return ex.toString();
    }

    try {
      pStreamReader = new InputStreamReader(pStream, "UTF-8");
      pBufferedReader = new BufferedReader(pStreamReader);
    } catch (UnsupportedEncodingException ex) {
      return ex.toString();
    }

    try {
      String line;
      while ((line = pBufferedReader.readLine()) != null) {
        line = line.replaceAll("[\r\n]]", "").trim();
        if (line.startsWith("\"msg\": ")) {
          line = line.substring(8);
          line = line.substring(0, line.length()-2);
          return line.trim();
        }
      }
    } catch (IOException ex) {
      return ex.toString();
    }

    return "Unknown error.";
  }

  /** Escape a string value to be encapsulated by single quotes. */
  private static String escapeString(final String str) {
    return str.replaceAll("'", "''");
  }

  /**
   * Retrieve all primary keys in order from a given dataset.
   *
   * @param dataverse Name of the dataverse.
   * @param dataset Name of the dataset.
   * @return A list of primary keys in order.
   */
  public List<String> getPrimaryKeys(final String dataverse, final String dataset) {
    if (!pIsValid) {
      pError = "Invalid connector";
      return null;
    }
    if (dataverse.isEmpty()) {
      pError = "dataverse must not be empty";
      return null;
    }
    if (dataset.isEmpty()) {
      pError = "dataset must not be empty";
      return null;
    }

    String sql = "SELECT VALUE InternalDetails.PrimaryKey FROM Metadata.`Dataset` WHERE "
        + "DataverseName='" + escapeString(dataverse)
        + "' AND DatasetName='" + escapeString(dataset)
        + "';";

    if (!execute(sql)) {
      return null;
    }

    while (true) {
      String line = nextResult();
      if (line.isEmpty()) {
        break;
      }

      if (line.startsWith("[") && line.endsWith("]")) {
        JSONArray pks;
        try {
          pks = (JSONArray)(pParser.parse(line));
        } catch (ParseException ex) {
          pError = ex.toString();
          closeCurrentResponse();
          return null;
        }

        if (pks.size() == 1) {
          List<String> ret = new ArrayList<>();
          for (Object key : (JSONArray)(pks.get(0))) {
            ret.add(key.toString());
          }
          closeCurrentResponse();
          return ret;
        } else {
          pError = dataverse + "." + dataset + " has nested primary key(s)";
          closeCurrentResponse();
          return null;
        }
      }
    }
    pError = "Unknown error";
    closeCurrentResponse();
    return null;
  }

  /**
   * Retrieve all fields with the type of each key, including the primary key(s).
   *
   * @param dataverse Name of the dataverse.
   * @param dataset Name of the dataset.
   * @return A map of field names and their types.
   */
  public Map<String, String> getAllFields(final String dataverse, final String dataset) {
    if (!pIsValid) {
      pError = "Invalid connector";
      return null;
    }
    if (dataverse.isEmpty()) {
      pError = "dataverse must not be empty";
      return null;
    }
    if (dataset.isEmpty()) {
      pError = "dataset must not be empty";
      return null;
    }

    String sql = "SELECT VALUE dt.Derived.Record.Fields FROM Metadata.`Dataset` ds, Metadata.`Datatype` dt WHERE "
        + "ds.DataverseName='" + escapeString(dataverse)
        + "' AND ds.DatasetName='" + escapeString(dataset)
        + "' AND ds.DatatypeName=dt.DatatypeName;";

    if (!execute(sql)) {
      return null;
    }

    while (true) {
      String line = nextResult();
      if (line.isEmpty()) {
        break;
      }

      if (line.startsWith("[") && line.endsWith("]")) {
        JSONArray fields;
        try {
          fields = (JSONArray)(pParser.parse(line));
        } catch (ParseException ex) {
          pError = ex.toString();
          closeCurrentResponse();
          return null;
        }
        if (fields.isEmpty()) {
          pError = "No field can be found";
          closeCurrentResponse();
          return null;
        }
        Map<String, String> ret = new HashMap<>();
        for (Object fobj : fields) {
          JSONObject f = (JSONObject)(fobj);
          ret.put(f.get("FieldName").toString(), f.get("FieldType").toString());
        }
        closeCurrentResponse();
        return ret;
      }
    }
    pError = "Unknown error";
    closeCurrentResponse();
    return null;
  }

  /**
   * Execute any SQL++ query that does not fetch record from the database.
   *
   * @param sql SQL++ statement.
   * @return True on success. False on failure. Error message can be checked by error().
   */
  public boolean executeUpdate(final String sql) {
    if (pResponse != null) {
      pError = "Another statement is running.";
      return false;
    }

    pError = "";

    HttpPost post = new HttpPost(pServiceUrl);
    post.setHeader("Content-Type", "application/x-www-form-urlencoded");

    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("statement", sql));
    params.add(new BasicNameValuePair("mode", "immediate"));
    try {
      post.setEntity(new UrlEncodedFormEntity(params));
    } catch (UnsupportedEncodingException ex) {
      pError = ex.toString();
      return false;
    }

    long startTime = System.currentTimeMillis();

    try {
      pResponse = pClient.execute(post);
      pElapsed = System.currentTimeMillis() - startTime;
    } catch (ClientProtocolException ex) {
      pError = ex.toString();
      closeCurrentResponse();
      return false;
    } catch (IOException ex) {
      pError = ex.toString();
      closeCurrentResponse();
      return false;
    }

    StatusLine s = pResponse.getStatusLine();
    if (s.getStatusCode() == 200) {
      pError = "";
      closeCurrentResponse();
      return true;
    } else {
      pError = getServerError();
      closeCurrentResponse();
      return false;
    }
  }

  /**
   * Execute a SELECT SQL++ query. Results can be fetched via nextResult().
   *
   * @param sql SQL++ statement.
   * @return True on success. False on failure. Error message can be checked by error().
   */
  public boolean execute(final String sql) {
    if (pResponse != null) {
      pError = "Another statement is running.";
      return false;
    }

    pError = "";

    HttpPost post = new HttpPost(pServiceUrl);
    post.setHeader("Content-Type", "application/x-www-form-urlencoded");

    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("statement", sql));
    params.add(new BasicNameValuePair("mode", "immediate"));
    try {
      post.setEntity(new UrlEncodedFormEntity(params));
    } catch (UnsupportedEncodingException ex) {
      pError = ex.toString();
      return false;
    }

    long startTime = System.currentTimeMillis();

    try {
      pResponse = pClient.execute(post);
      pElapsed = System.currentTimeMillis() - startTime;
    } catch (ClientProtocolException ex) {
      pError = ex.toString();
      closeCurrentResponse();
      return false;
    } catch (IOException ex) {
      pError = ex.toString();
      closeCurrentResponse();
      return false;
    }

    StatusLine s = pResponse.getStatusLine();
    if (s.getStatusCode() == 200) {
      try {
        pStream = pResponse.getEntity().getContent();
      } catch (IOException ex) {
        pError = ex.toString();
        closeCurrentResponse();
        return false;
      }

      try {
        pStreamReader = new InputStreamReader(pStream, "UTF-8");
        pBufferedReader = new BufferedReader(pStreamReader);
        pBeginResults = false;
      } catch (UnsupportedEncodingException ex) {
        pError = ex.toString();
        closeCurrentResponse();
        return false;
      }

      pError = "";
      return true;
    } else {
      pError = getServerError();
      closeCurrentResponse();
      return false;
    }
  }

  /**
   * Get one result from a SELECT SQL++ query.
   *
   * @return The string content (in JSON format) of one result. Empty if no result or no more result.
   */
  public String nextResult() {
    if (pBufferedReader == null) {
      pError = "No statement executed.";
      return "";
    }
    try {
      String line;
      while ((line = pBufferedReader.readLine()) != null) {
        line = line.replaceAll("[\r\n]]", "").trim();
        if (!pBeginResults) {
          if (line.startsWith("\"results\": ")) {
            pBeginResults = true;
            pError = "";
            line = line.substring(12);
            if (line.endsWith(",")) {
              line = line.substring(0, line.length() - 2);
            }
            line = line.trim();
            if (line.compareTo("]") == 0) {
              pError = "";
              pBeginResults = false;
              closeCurrentResponse();
              return "";
            }
            return line;
          } else {
            continue;
          }
        } else {
          if (line.compareTo("]") == 0) {
            pError = "";
            pBeginResults = false;
            closeCurrentResponse();
            return "";
          } else {
            pError = "";
            if (line.startsWith(",")) {
              line = line.substring(1);
            }
            if (line.endsWith(",")) {
              line = line.substring(0, line.length() - 2);
            }
            return line.trim();
          }
        }
      }
      pError = "";
      closeCurrentResponse();
      return "";
    } catch (IOException ex) {
      pError = ex.toString();
      closeCurrentResponse();
      return "";
    }
  }

  /**
   * Get all results from a SELECT SQL++ query.
   * This function may consume too much memory if there are too many results.
   *
   * @return A list of result strings. Empty string if no result or error.
   */
  public List<String> getAllResults() {
    List<String> ret = new ArrayList<>();
    if (pResponse == null) {
      return ret;
    }

    while (true) {
      String line = nextResult();
      if (line.isEmpty()) {
        return ret;
      } else {
        ret.add(line);
      }
    }
  }

  /**
   * Close the current response from the server.
   */
  public void closeCurrentResponse() {
    if (pBufferedReader != null) {
      try {
        pBufferedReader.close();
      } catch (IOException ex) {
        // pass
      }
      pBufferedReader = null;
    }

    if (pStreamReader != null) {
      try {
        pStreamReader.close();
      } catch (IOException ex) {
        // pass
      }
      pStreamReader = null;
    }

    if (pStream != null) {
      try {
        pStream.close();
      } catch (IOException ex) {
        // pass
      }
      pStream = null;
    }

    if (pResponse != null) {
      try {
        pResponse.close();
      } catch (IOException ex) {
        // pass
      }
      pResponse = null;
    }
  }
}

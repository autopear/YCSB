package com.yahoo.ycsb.db.asterixdb;

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

import static org.apache.commons.validator.routines.UrlValidator.ALLOW_LOCAL_URLS;

/**
 * Connector for AsterixDB via HTTP API.
 */
public class AsterixDBConnector {
  private CloseableHttpClient pClient = null;
  private String pServiceUrl = "";
  private boolean pIsValid = false;
  private String pError;
  private boolean pBeginResults = false;
  private long pElapsed = 0;
  private CloseableHttpResponse pResponse = null;
  private InputStream pStream = null;
  private InputStreamReader pStreamReader = null;
  private BufferedReader pBufferReader = null;

  public AsterixDBConnector(final String hostname, int port) {
    pServiceUrl = "http://" + hostname + ":" + port + "/query/service";
    String[] schemes = {"http"};
    UrlValidator urlValidator = new UrlValidator(schemes, ALLOW_LOCAL_URLS);
    pIsValid = urlValidator.isValid(pServiceUrl);
    if (pIsValid) {
      pClient = HttpClients.createDefault();
    }
  }

  public AsterixDBConnector(final String serviceUrl) {
    pServiceUrl = serviceUrl;
    String[] schemes = {"http", "https"};
    UrlValidator urlValidator = new UrlValidator(schemes, ALLOW_LOCAL_URLS);
    pIsValid = urlValidator.isValid(pServiceUrl);
    if (pIsValid) {
      pClient = HttpClients.createDefault();
    }
  }

  public boolean isValid() {
    return pIsValid;
  }

  public boolean hasError() {
    return !pError.isEmpty();
  }

  public String error() {
    return pError;
  }

  public long elapsedTime() {
    return pElapsed;
  }

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
      closeResponse();
      return false;
    } catch (IOException ex) {
      pError = ex.toString();
      closeResponse();
      return false;
    }

    StatusLine s = pResponse.getStatusLine();
    if (s.getStatusCode() == 200) {
      pError = "";
      closeResponse();
      return true;
    } else {
      pError = s.getReasonPhrase();
      closeResponse();
      return false;
    }
  }

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
      closeResponse();
      return false;
    } catch (IOException ex) {
      pError = ex.toString();
      closeResponse();
      return false;
    }

    StatusLine s = pResponse.getStatusLine();
    if (s.getStatusCode() == 200) {
      try {
        pStream = pResponse.getEntity().getContent();
      } catch (IOException ex) {
        pError = ex.toString();
        closeResponse();
        return false;
      }

      try {
        pStreamReader = new InputStreamReader(pStream, "UTF-8");
        pBufferReader = new BufferedReader(pStreamReader);
        pBeginResults = false;
      } catch (UnsupportedEncodingException ex) {
        pError = ex.toString();
        closeResponse();
        return false;
      }

      pError = "";
      return true;
    } else {
      pError = s.getReasonPhrase();
      closeResponse();
      return false;
    }
  }

  public String nextResult() {
    if (pBufferReader == null) {
      pError = "No statement executed.";
      return "";
    }
    try {
      String line;
      while ((line = pBufferReader.readLine()) != null) {
        line = line.replaceAll("[\r\n]]", "").trim();
        if (!pBeginResults) {
          if (line.startsWith("\"results\": ")) {
            pBeginResults = true;
            pError = "";
            line = line.substring(12);
            if (line.endsWith(",")) {
              line = line.substring(0, line.length() - 2);
            }
            return line.trim();
          } else {
            continue;
          }
        } else {
          if (line.compareTo("]") == 0) {
            pError = "";
            pBeginResults = false;
            closeResponse();
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
      closeResponse();
      return "";
    } catch (IOException ex) {
      pError = ex.toString();
      closeResponse();
      return "";
    }
  }

  private void closeResponse() {
    if (pBufferReader != null) {
      try {
        pBufferReader.close();
      } catch (IOException ex) {
        // pass
      }
      pBufferReader = null;
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

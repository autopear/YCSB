package com.yahoo.ycsb.db;

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

public class AsterixDBConnector {
    private CloseableHttpClient __client = null;
    private String __serviceUrl = "";
    private boolean __isValid = false;
    private String __error;
    private boolean __beginResults = false;
    private long __elapsed = 0;
    private CloseableHttpResponse __response = null;
    private InputStream __stream = null;
    private InputStreamReader __streamReader = null;
    private BufferedReader __bufferReader = null;

    public AsterixDBConnector(final String hostname, int port) {
        __serviceUrl = "http://" + hostname + ":" + port + "/query/service";
        String[] schemes = {"http"};
        UrlValidator urlValidator = new UrlValidator(schemes, ALLOW_LOCAL_URLS);
        __isValid = urlValidator.isValid(__serviceUrl);
        if (__isValid)
            __client = HttpClients.createDefault();
    }

    public AsterixDBConnector(final String serviceUrl) {
        String[] schemes = {"http", "https"};
        UrlValidator urlValidator = new UrlValidator(schemes, ALLOW_LOCAL_URLS);
        __isValid = urlValidator.isValid(__serviceUrl);
        if (__isValid)
            __client = HttpClients.createDefault();
    }

    public boolean isValid() {
        return __isValid;
    }

    public boolean hasError() {
        return !__error.isEmpty();
    }

    public String error() {
        return __error;
    }

    public long elapsedTime() {
        return __elapsed;
    }

    public boolean executeUpdate(final String sql) {
        if (__response != null) {
            __error = "Another statement is running.";
            return false;
        }

        __error = "";

        HttpPost post = new HttpPost(__serviceUrl);
        post.setHeader("Content-Type", "application/x-www-form-urlencoded");

        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("statement", sql));
        params.add(new BasicNameValuePair("mode", "immediate"));
        try {
            post.setEntity(new UrlEncodedFormEntity(params));
        } catch (UnsupportedEncodingException ex) {
            __error = ex.toString();
            return false;
        }

        long startTime = System.currentTimeMillis();

        try {
            __response = __client.execute(post);
            __elapsed = System.currentTimeMillis() - startTime;
        } catch (ClientProtocolException ex) {
            __error = ex.toString();
            closeResponse();
            return false;
        } catch (IOException ex) {
            __error = ex.toString();
            closeResponse();
            return false;
        }

        StatusLine s = __response.getStatusLine();
        if (s.getStatusCode() == 200) {
            __error = "";
            closeResponse();
            return true;
        } else {
            __error = s.getReasonPhrase();
            closeResponse();
            return false;
        }
    }

    public boolean execute(final String sql) {
        if (__response != null) {
            __error = "Another statement is running.";
            return false;
        }

        __error = "";

        HttpPost post = new HttpPost(__serviceUrl);
        post.setHeader("Content-Type", "application/x-www-form-urlencoded");

        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("statement", sql));
        params.add(new BasicNameValuePair("mode", "immediate"));
        try {
            post.setEntity(new UrlEncodedFormEntity(params));
        } catch (UnsupportedEncodingException ex) {
            __error = ex.toString();
            return false;
        }

        long startTime = System.currentTimeMillis();

        try {
            __response = __client.execute(post);
            __elapsed = System.currentTimeMillis() - startTime;
        } catch (ClientProtocolException ex) {
            __error = ex.toString();
            closeResponse();
            return false;
        } catch (IOException ex) {
            __error = ex.toString();
            closeResponse();
            return false;
        }

        StatusLine s = __response.getStatusLine();
        if (s.getStatusCode() == 200) {
            try {
                __stream = __response.getEntity().getContent();
            } catch (IOException ex) {
                __error = ex.toString();
                closeResponse();
                return false;
            }

            try {
                __streamReader = new InputStreamReader(__stream, "UTF-8");
                __bufferReader = new BufferedReader(__streamReader);
                __beginResults = false;
            } catch (UnsupportedEncodingException ex) {
                __error = ex.toString();
                closeResponse();
                return false;
            }

            __error = "";
            return true;
        } else {
            __error = s.getReasonPhrase();
            closeResponse();
            return false;
        }
    }

    public String nextResult() {
        if (__bufferReader == null) {
            __error = "No statement executed.";
            return "";
        }
        try {
            String line;
            while ((line = __bufferReader.readLine()) != null) {
                line = line.replaceAll("[\r\n]]", "").trim();
                if (!__beginResults) {
                    if (line.startsWith("\"results\": ")) {
                        __beginResults = true;
                        __error = "";
                        line = line.substring(12);
                        if (line.endsWith(","))
                            line = line.substring(0, line.length()-2);
                        return line.trim();
                    } else
                        continue;
                } else {
                    if (line.compareTo("]") == 0) {
                        __error = "";
                        __beginResults = false;
                        closeResponse();
                        return "";
                    } else {
                        __error = "";
                        if (line.startsWith(","))
                            line = line.substring(1);
                        if (line.endsWith(","))
                            line = line.substring(0, line.length()-2);
                        return line.trim();
                    }
                }
            }
            __error = "";
            closeResponse();
            return "";
        } catch (IOException ex) {
            __error = ex.toString();
            closeResponse();
            return "";
        }
    }

    private void closeResponse() {
        if (__bufferReader != null) {
            try {
                __bufferReader.close();
            } catch (IOException ex) {
            }
            __bufferReader = null;
        }

        if (__streamReader != null) {
            try {
                __streamReader.close();
            } catch (IOException ex) {
            }
            __streamReader = null;
        }

        if (__stream != null) {
            try {
                __stream.close();
            } catch (IOException ex) {
            }
            __stream = null;
        }

        if (__response != null) {
            try {
                __response.close();
            } catch (IOException ex) {
            }
            __response = null;
        }
    }
}

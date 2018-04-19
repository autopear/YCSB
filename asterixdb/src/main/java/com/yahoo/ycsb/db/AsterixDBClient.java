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

import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class AsterixDBClient extends DB {

  public static final String DB_URL = "db.url";

  private String SERVICE_URL;

  public void init() throws DBException {
    SERVICE_URL = getProperties().getProperty(DB_URL, "http://localhost:19002/query/service");
  }

  private HttpPost generatePost(final String query) throws UnsupportedEncodingException {
    HttpPost post = new HttpPost(SERVICE_URL);
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("statement", query));
    params.add(new BasicNameValuePair("mode", "immediate"));
    post.setEntity(new UrlEncodedFormEntity(params));
    return post;
  }

  private boolean queryWithoutResult(final String query) {
    HttpPost post;
    try {
      post = generatePost(query);
    } catch (UnsupportedEncodingException ex) {
      return false;
    }

    CloseableHttpResponse response;
    CloseableHttpClient client = HttpClients.createDefault();
    try {
      response = client.execute(post);
    } catch (ClientProtocolException ex) {
      return false;
    } catch (IOException ex) {
      return false;
    } finally {
      try {
        client.close();
      } catch (IOException ex) { }
    }

    StatusLine s = response.getStatusLine();
    return (s.getStatusCode() == 200);
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
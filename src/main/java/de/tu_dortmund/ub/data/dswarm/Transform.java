/*
The MIT License (MIT)

Copyright (c) 2015, Hans-Georg Becker, http://orcid.org/0000-0003-0432-294X

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

package de.tu_dortmund.ub.data.dswarm;

import java.io.File;
import java.io.StringWriter;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.Consts;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Export Task for Task Processing Unit for d:swarm
 *
 * @author Jan Polowinski (SLUB Dresden)
 * @version 2015-04-20
 *
 */
public class Transform implements Callable<String> {

    private Properties config = null;
    private Logger logger = null;
	private String	inputDataModelID;
	private String	outputDataModelID;
	private String	projectID;

    public Transform(Properties config, String inputDataModelID, Logger logger) {

        this.config = config;
        this.logger = logger;
        
        // init IDs of the prototype project
        if (null!=inputDataModelID) this.inputDataModelID = inputDataModelID ;
        else this.inputDataModelID = config.getProperty("prototype.dataModelID");
        this.projectID = config.getProperty("prototype.projectID");
        this.outputDataModelID = config.getProperty("prototype.outputDataModelID");
    }

//    @Override
    public String call() {

        // init logger
        PropertyConfigurator.configure(config.getProperty("service.log4j-conf"));

        logger.info("[" + config.getProperty("service.name") + "] " + "Starting 'Transform (Task)' ...");

        // init process values
        String message = null;

        try {

            if (Boolean.parseBoolean(config.getProperty("transform.do"))) {
            	
                // export and save to results folder
                String jsonResponse = executeTask(inputDataModelID, projectID, outputDataModelID);
                System.out.println(jsonResponse);
            }
        }
        catch (Exception e) {

            logger.error("[" + config.getProperty("service.name") + "] Transforming datamodel '" + inputDataModelID + "' to '" + outputDataModelID + "' failed with a " + e.getClass().getSimpleName());
            e.printStackTrace();
        }

        return message;
    }

	/**
	 * configuration and processing of the task
	 *
	 * @param inputDataModelID
	 * @param projectID
	 * @param outputDataModelID
	 * @return
	 */
	private String executeTask(String inputDataModelID, String projectID, String outputDataModelID) throws Exception {
	
	    String jsonResponse = null;
	
	    CloseableHttpClient httpclient = HttpClients.createDefault();
	
	    try {
	
	        // Hole Mappings aus dem Projekt mit 'projectID'
	        HttpGet httpGet = new HttpGet(config.getProperty("engine.dswarm.api") + "projects/" + projectID);
	
	        CloseableHttpResponse httpResponse = httpclient.execute(httpGet);
	
	        logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpGet.getRequestLine());
	
	        String mappings = "";
	
	        try {
	
	            int statusCode = httpResponse.getStatusLine().getStatusCode();
	            HttpEntity httpEntity = httpResponse.getEntity();
	
	            switch (statusCode) {
	
	                case 200: {
	
	                    StringWriter writer = new StringWriter();
	                    IOUtils.copy(httpEntity.getContent(), writer, "UTF-8");
	                    String responseJson = writer.toString();
	
	                    logger.info("[" + config.getProperty("service.name") + "] responseJson : " + responseJson);
	
	                    JsonReader jsonReader = Json.createReader(IOUtils.toInputStream(responseJson, "UTF-8"));
	                    JsonObject jsonObject = jsonReader.readObject();
	
	                    mappings = jsonObject.getJsonArray("mappings").toString();
	
	                    logger.info("[" + config.getProperty("service.name") + "] mappings : " + mappings);
	
	                    break;
	                }
	                default: {
	
	                    logger.error("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());
	                }
	            }
	
	            EntityUtils.consume(httpEntity);
	        } finally {
	            httpResponse.close();
	        }
	
	        // Hole InputDataModel
	        String inputDataModel = "";
	
	        httpGet = new HttpGet(config.getProperty("engine.dswarm.api") + "datamodels/" + inputDataModelID);
	
	        httpResponse = httpclient.execute(httpGet);
	
	        logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpGet.getRequestLine());
	
	        try {
	
	            int statusCode = httpResponse.getStatusLine().getStatusCode();
	            HttpEntity httpEntity = httpResponse.getEntity();
	
	            switch (statusCode) {
	
	                case 200: {
	
	                    StringWriter writer = new StringWriter();
	                    IOUtils.copy(httpEntity.getContent(), writer, "UTF-8");
	                    inputDataModel = writer.toString();
	
	                    logger.info("[" + config.getProperty("service.name") + "] inputDataModel : " + inputDataModel);
	
	                    JsonReader jsonReader = Json.createReader(IOUtils.toInputStream(inputDataModel, "UTF-8"));
	                    JsonObject jsonObject = jsonReader.readObject();
	
	                    String inputResourceID = jsonObject.getJsonObject("data_resource").getString("uuid");
	
	                    logger.info("[" + config.getProperty("service.name") + "] mappings : " + mappings);
	
	                    break;
	                }
	                default: {
	
	                    logger.error("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());
	                }
	            }
	
	            EntityUtils.consume(httpEntity);
	        } finally {
	            httpResponse.close();
	        }
	
	        // Hole OutputDataModel
	        String outputDataModel = "";
	
	        httpGet = new HttpGet(config.getProperty("engine.dswarm.api") + "datamodels/" + outputDataModelID);
	
	        httpResponse = httpclient.execute(httpGet);
	
	        logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpGet.getRequestLine());
	
	        try {
	
	            int statusCode = httpResponse.getStatusLine().getStatusCode();
	            HttpEntity httpEntity = httpResponse.getEntity();
	
	            switch (statusCode) {
	
	                case 200: {
	
	                    StringWriter writer = new StringWriter();
	                    IOUtils.copy(httpEntity.getContent(), writer, "UTF-8");
	                    outputDataModel = writer.toString();
	
	                    logger.info("[" + config.getProperty("service.name") + "] outputDataModel : " + outputDataModel);
	
	                    break;
	                }
	                default: {
	
	                    logger.error("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());
	                }
	            }
	
	            EntityUtils.consume(httpEntity);
	        } finally {
	            httpResponse.close();
	        }
	
	        // erzeuge Task-JSON
	        String task = "{";
	        task += "\"persist\" : " + config.getProperty("results.persistInDMP") + ", ";
	        task += "\"task\": { ";
	        task += "\"name\":\"" + "Task Batch-Prozess 'CrossRef'" + "\",";
	        task += "\"description\":\"" + "Task Batch-Prozess 'CrossRef' zum InputDataModel '" + inputDataModelID + "'\",";
	        task += "\"job\": { " +
	                "\"mappings\": " + mappings + "," +
	                "\"uuid\": \"" + UUID.randomUUID() + "\"" +
	                " },";
	        task += "\"input_data_model\":" + inputDataModel + ",";
	        task += "\"output_data_model\":" + outputDataModel;
	        task += "}";
	        task += "}";
	
	        logger.info("[" + config.getProperty("service.name") + "] task : " + task);
	
	        // POST /dmp/tasks/
	        HttpPost httpPost = new HttpPost(config.getProperty("engine.dswarm.api") + "tasks");
	        StringEntity stringEntity = new StringEntity(task, ContentType.create("application/json", Consts.UTF_8));
	        httpPost.setEntity(stringEntity);
	
	        logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpPost.getRequestLine());
	
	        httpResponse = httpclient.execute(httpPost);
	
	        try {
	
	            int statusCode = httpResponse.getStatusLine().getStatusCode();
	            HttpEntity httpEntity = httpResponse.getEntity();
	
	            switch (statusCode) {
	
	                case 200: {
	
	                    logger.info("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());
	
	                    StringWriter writer = new StringWriter();
	                    IOUtils.copy(httpEntity.getContent(), writer, "UTF-8");
	                    jsonResponse = writer.toString();
	
	                    logger.info("[" + config.getProperty("service.name") + "] jsonResponse : " + jsonResponse);
	
	                    break;
	                }
	                default: {
	
	                    logger.error("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());
	                }
	            }
	
	            EntityUtils.consume(httpEntity);
	        } finally {
	            httpResponse.close();
	        }
	
	    } finally {
	        httpclient.close();
	    }
	
	    return jsonResponse;
	}
}

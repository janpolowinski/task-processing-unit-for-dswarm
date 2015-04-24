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
import java.util.concurrent.Callable;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Ingest-Task for Task Processing Unit for d:swarm
 *
 * @author Dipl.-Math. Hans-Georg Becker (M.L.I.S.)
 * @author Jan Polowinski (SLUB Dresden)
 * @version 2015-04-17
 *
 */
public class Ingest implements Callable<String> {

	public static final String UUID_IDENTIFIER              = "uuid";
	public static final String ENGINE_DSWARM_API_IDENTIFIER = "engine.dswarm.api";
	public static final String DATA_ENDPOINT                = "data";
	public static final String DATAMODELS_ENDPOINT          = DATA_ENDPOINT + "models";
	public static final String SLASH                        = "/";
	public static final String CONFIGURATION_IDENTIFIER     = "configuration";
	public static final String RESOURCES_ENDPOINT           = "resources";
	public static final String RESOURCES_IDENTIFIER = RESOURCES_ENDPOINT;
	public static final String FILE_IDENTIFIER = "file";
	public static final String NAME_IDENTIFIER = "name";
	public static final String DESCRIPTION_IDENTIFIER = "description";
	public static final String UTF_8 = "UTF-8";
	private final Properties config;
	private final Logger     logger;

	private final String resource;
	private final int    cnt;

	public Ingest(final Properties config, final Logger logger, final String resource, final int cnt) {

		this.config = config;
		this.logger = logger;
		this.resource = resource;
		this.cnt = cnt;
	}

	//    @Override
	public String call() {

		// init logger
		PropertyConfigurator.configure(config.getProperty(TPUStatics.SERVICE_LOG4J_CONF_IDENTIFIER));

		final String serviceName = config.getProperty(TPUStatics.SERVICE_NAME_IDENTIFIER);
		final String engineDswarmAPI = config.getProperty(ENGINE_DSWARM_API_IDENTIFIER);

		logger.info(String.format("[%s] Starting 'Ingest (Task)' ...", serviceName));

		// init IDs of the prototype project
		final String dataModelID = config.getProperty(TPUStatics.PROTOTYPE_DATA_MODEL_ID_IDENTIFIER);
		//        String projectID = config.getProperty("prototype.projectID");
		//        String outputDataModelID = config.getProperty("prototype.outputDataModelID"); // Internal Data Model BiboDocument
		//      String updateResourceID = config.getProperty("prototype.resourceID"); // the resource ID to update for each uploaded file
		// use the projects resource as the update-resource for now:
		String updateResourceID = null;
		try {
			updateResourceID = getDataResourceID(dataModelID, serviceName, engineDswarmAPI);
		} catch (final Exception e1) {

			logger.error("something went wrong", e1);
			e1.printStackTrace();

			return null;
		}

		// init process values
		final String message = null;

		try {
			// build a InputDataModel for the resource
			//            String inputResourceJson = uploadFileToDSwarm(resource, "resource for project '" + resource, config.getProperty("project.name") + "' - case " + cnt);
			final String name = String.format("resource for project '%s'", resource);
			final String projectName = config.getProperty(TPUStatics.PROJECT_NAME_IDENTIFIER);
			final String description = String.format("'%s' - case %d", projectName, cnt);
			final String inputResourceJson = uploadFileAndUpdateResource(updateResourceID, resource, name,
					description, serviceName, engineDswarmAPI);
			final JsonReader jsonReader;

			if (inputResourceJson == null) {

				logger.error("something went wrong at resource update");

				return null;
			}

			jsonReader = Json.createReader(IOUtils.toInputStream(inputResourceJson, UTF_8));
			final String inputResourceID = jsonReader.readObject().getString(UUID_IDENTIFIER);
			logger.info(String.format("[%s] inputResourceID = %s", serviceName, inputResourceID));

			if (inputResourceID != null) {

				// update the datamodel (will use it's (update) resource)
				updateDataModelContent(dataModelID, serviceName, engineDswarmAPI);

				// we don't need to transform after each ingest of a slice of records,
				// so transform and export will be done separately
				logger.info(String.format("[%s] (Note: Only ingest, but no transformation or export done.)", serviceName));
			}

			// no need to clean up resources or datamodels anymore

		} catch (final Exception e) {

			logger.error(String.format("[%s] Processing resource '%s' failed with a %s", serviceName, resource, e.getClass().getSimpleName()), e);
			e.printStackTrace();
		}

		return message;
	}

	/**
	 * update the datamodel with the given ID
	 *
	 * @param inputDataModelID
	 * @return
	 * @throws Exception
	 */
	private String updateDataModelContent(final String inputDataModelID, final String serviceName, final String engineDswarmAPI) throws Exception {

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {
			// Update the existing input Data Model (we are simply using the example data model here ... TODO !)
			final String uri = engineDswarmAPI + DATAMODELS_ENDPOINT + SLASH + inputDataModelID + SLASH + DATA_ENDPOINT;
			final HttpPost httpPost = new HttpPost(uri);

			logger.info(String.format("[%s] inputDataModelID : %s", serviceName, inputDataModelID));
			logger.info(String.format("[%s] request : %s", serviceName, httpPost.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPost)) {

				int statusCode = httpResponse.getStatusLine().getStatusCode();

				final String message = String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				switch (statusCode) {

					case 200: {

						logger.info(message);

						break;
					}
					default: {

						logger.error(message);
					}
				}
			}
		}

		return inputDataModelID;
	}

	/**
	 * get the resource id of the resource for the data model for the the prototype project
	 *
	 * @param dataModelID
	 * @return resourceID
	 * @throws Exception
	 */
	private String getDataResourceID(final String dataModelID, final String serviceName, final String engineDswarmAPI) throws Exception {

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			// Hole Mappings aus dem Projekt mit 'projectID'
			final String uri = engineDswarmAPI + DATAMODELS_ENDPOINT + SLASH + dataModelID;
			final HttpGet httpGet = new HttpGet(uri);

			logger.info(String.format("[%s] request : %s", serviceName, httpGet.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpGet)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();
				final HttpEntity httpEntity = httpResponse.getEntity();

				switch (statusCode) {

					case 200: {

						final StringWriter writer = new StringWriter();
						IOUtils.copy(httpEntity.getContent(), writer, UTF_8);
						final String responseJson = writer.toString();

						logger.info(String.format("[%s] responseJson : %s", serviceName, responseJson));

						final JsonReader jsonReader = Json.createReader(IOUtils.toInputStream(responseJson, UTF_8));
						final JsonObject jsonObject = jsonReader.readObject();
						final JsonArray resources = jsonObject.getJsonObject(CONFIGURATION_IDENTIFIER).getJsonArray(RESOURCES_IDENTIFIER);

						final String resourceID = resources.getJsonObject(0).getJsonString(UUID_IDENTIFIER).getString();

						logger.info(String.format("[%s] resourceID : %s", serviceName, resourceID));

						return resourceID;
					}
					default: {

						logger.error(String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine()
								.getReasonPhrase()));
					}
				}

				EntityUtils.consume(httpEntity);
			}
		}

		return null;
	}

	/**
	 * upload a file and update an existing resource with it
	 *
	 * @param resourceUUID
	 * @param filename
	 * @param name
	 * @param description
	 * @return responseJson
	 * @throws Exception
	 */
	private String uploadFileAndUpdateResource(final String resourceUUID, final String filename, final String name, final String description, final String serviceName, final String engineDswarmAPI) throws Exception {

		if (null == resourceUUID)
			throw new Exception("ID of the resource to update was null.");

		final String resourceWatchFolder = config.getProperty(TPUStatics.RESOURCE_WATCHFOLDER_IDENTIFIER);
		final String completeFileName = resourceWatchFolder + File.separatorChar + filename;

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final HttpPut httpPut = new HttpPut(engineDswarmAPI + RESOURCES_ENDPOINT + SLASH + resourceUUID);

			final File file1 = new File(completeFileName);
			final FileBody fileBody = new FileBody(file1);
			final StringBody stringBodyForName = new StringBody(name, ContentType.TEXT_PLAIN);
			final StringBody stringBodyForDescription = new StringBody(description, ContentType.TEXT_PLAIN);

			final HttpEntity reqEntity = MultipartEntityBuilder.create()
					.addPart(FILE_IDENTIFIER, fileBody)
					.addPart(NAME_IDENTIFIER, stringBodyForName)
					.addPart(DESCRIPTION_IDENTIFIER, stringBodyForDescription)
					.build();

			httpPut.setEntity(reqEntity);

			logger.info(String.format("[%s] " + "request : %s", serviceName, httpPut.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPut)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();
				final HttpEntity httpEntity = httpResponse.getEntity();

				final String message = String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				switch (statusCode) {

					case 200: {

						logger.info(message);
						final StringWriter writer = new StringWriter();
						IOUtils.copy(httpEntity.getContent(), writer, UTF_8);
						final String responseJson = writer.toString();

						logger.info(String.format("[%s] responseJson : %s", serviceName, responseJson));

						return responseJson;
					}
					default: {

						logger.error(message);
					}
				}

				EntityUtils.consume(httpEntity);
			}
		}

		return null;
	}
}

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
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonGenerator;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Init-Task for Task Processing Unit for d:swarm<br/>
 * creates a resource + configuration + data model
 *
 * @author Dipl.-Math. Hans-Georg Becker (M.L.I.S.)
 * @author Jan Polowinski (SLUB Dresden)
 * @version 2015-04-17
 *
 */
public class Init implements Callable<String> {

	public static final String UUID_IDENTIFIER                = "uuid";
	public static final String DATAMODELS_ENDPOINT            = "datamodels";
	public static final String CONFIGURATIONS_ENDPOINT        = "configurations";
	public static final String RESOURCES_ENDPOINT             = "resources";
	public static final String FILE_IDENTIFIER                = "file";
	public static final String NAME_IDENTIFIER                = "name";
	public static final String DESCRIPTION_IDENTIFIER         = "description";
	public static final String UTF_8                          = "UTF-8";
	public static final String CONFIGURATION_IDENTIFIER = "configuration";
	public static final String DATA_RESOURCE_IDENTIFIER = "data_resource";
	public static final String APPLICATION_JSON = "application/json";
	public static final String DATA_MODEL_ID = "data_model_id";
	public static final String RESOURCE_ID = "resource_id";

	private final Properties config;
	private final Logger     logger;

	private final int cnt;

	public Init(final Properties config, final Logger logger, final int cnt) {

		this.config = config;
		this.logger = logger;
		this.cnt = cnt;
	}

	//    @Override
	public String call() {

		// init logger
		PropertyConfigurator.configure(config.getProperty(TPUStatics.SERVICE_LOG4J_CONF_IDENTIFIER));

		final String serviceName = config.getProperty(TPUStatics.SERVICE_NAME_IDENTIFIER);
		final String engineDswarmAPI = config.getProperty(TPUStatics.ENGINE_DSWARM_API_IDENTIFIER);

		logger.info(String.format("[%s] Starting 'Init (Task)' ...", serviceName));

		// init process values
		final String resource = config.getProperty(TPUStatics.INIT_RESOURCE_NAME_IDENTIFIER);

		try {
			// build a InputDataModel for the resource
			//            String inputResourceJson = uploadFileToDSwarm(resource, "resource for project '" + resource, config.getProperty("project.name") + "' - case " + cnt);
			final String name = String.format("resource for project '%s'", resource);
			final String description = String.format("'resource does not belong to a project' - case %d", cnt);
			final String inputResourceJson = uploadFileAndCreateResource(resource, name, description, serviceName, engineDswarmAPI);

			if (inputResourceJson == null) {

				logger.error("something went wrong at resource creation");

				return null;
			}

			final JsonReader inputResourceJsonReader = Json.createReader(IOUtils.toInputStream(inputResourceJson, UTF_8));
			final JsonObject inputResourceJSON = inputResourceJsonReader.readObject();
			final String inputResourceID = inputResourceJSON.getString(UUID_IDENTIFIER);
			logger.info(String.format("[%s] input resource id = %s", serviceName, inputResourceID));

			if (inputResourceID == null) {

				logger.error("something went wrong at resource creation, no resource uuid available");

				return null;
			}

			// create configuration
			final String configurationFileName = config.getProperty(TPUStatics.CONFIGURATION_NAME_IDENTIFIER);
			final String configurationJSONString = createConfiguration(configurationFileName, serviceName, engineDswarmAPI);

			if (configurationJSONString == null) {

				logger.error("something went wrong at configuration creation");

				return null;
			}

			final JsonReader configurationJsonReader = Json.createReader(IOUtils.toInputStream(configurationJSONString, UTF_8));
			final JsonObject configurationJSON = configurationJsonReader.readObject();
			final String configurationID = configurationJSON.getString(UUID_IDENTIFIER);
			logger.info(String.format("[%s] configuration id = %s", serviceName, configurationID));

			if (configurationID == null) {

				logger.error("something went wrong at configuration creation, no configuration uuid available");

				return null;
			}

			// create the datamodel (will use it's resource)
			final String dataModelName = String.format("data model %d", cnt);
			final String dataModelDescription = String.format("data model description %d", cnt);
			final String dataModelJSONString = createDataModel(inputResourceJSON, configurationJSON, dataModelName, dataModelDescription, serviceName,
					engineDswarmAPI);

			if (dataModelJSONString == null) {

				logger.error("something went wrong at data model creation");

				return null;
			}

			final JsonReader dataModelJsonReader = Json.createReader(IOUtils.toInputStream(dataModelJSONString, UTF_8));
			final JsonObject dataModelJSON = dataModelJsonReader.readObject();
			final String dataModelID = dataModelJSON.getString(UUID_IDENTIFIER);
			logger.info(String.format("[%s] configuration id = %s", serviceName, dataModelID));

			if (dataModelID == null) {

				logger.error("something went wrong at data model creation, no data model uuid available");

				return null;
			}

			// we don't need to transform after each ingest of a slice of records,
			// so transform and export will be done separately
			logger.info(String.format("[%s] (Note: Only ingest, but no transformation or export done.)", serviceName));

			final StringWriter stringWriter = new StringWriter();
			final JsonGenerator jp = Json.createGenerator(stringWriter);

			jp.writeStartObject();
			jp.write(DATA_MODEL_ID, dataModelID);
			jp.write(RESOURCE_ID, inputResourceID);
			jp.writeEnd();

			jp.flush();
			jp.close();

			final String result = stringWriter.toString();

			stringWriter.flush();
			stringWriter.close();

			return result;
		} catch (final Exception e) {

			logger.error(String.format("[%s] Processing resource '%s' failed with a %s", serviceName, resource, e.getClass().getSimpleName()), e);
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * uploads a file and creates a data resource with it
	 *
	 * @param filename
	 * @param name
	 * @param description
	 * @return responseJson
	 * @throws Exception
	 */
	private String uploadFileAndCreateResource(final String filename, final String name, final String description, final String serviceName,
			final String engineDswarmAPI) throws Exception {

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final HttpPost httpPost = new HttpPost(engineDswarmAPI + RESOURCES_ENDPOINT);

			final File file1 = new File(filename);
			final FileBody fileBody = new FileBody(file1);
			final StringBody stringBodyForName = new StringBody(name, ContentType.TEXT_PLAIN);
			final StringBody stringBodyForDescription = new StringBody(description, ContentType.TEXT_PLAIN);

			final HttpEntity reqEntity = MultipartEntityBuilder.create()
					.addPart(NAME_IDENTIFIER, stringBodyForName)
					.addPart(DESCRIPTION_IDENTIFIER, stringBodyForDescription)
					.addPart(FILE_IDENTIFIER, fileBody)
					.build();

			httpPost.setEntity(reqEntity);

			logger.info(String.format("[%s] request : %s", serviceName, httpPost.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPost)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();
				final HttpEntity httpEntity = httpResponse.getEntity();

				final String message = String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				switch (statusCode) {

					case 201: {

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

	/**
	 * creates a configuration from a given configuration JSON
	 *
	 * @param filename
	 * @return responseJson
	 * @throws Exception
	 */
	private String createConfiguration(final String filename, final String serviceName,
			final String engineDswarmAPI) throws Exception {

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final HttpPost httpPost = new HttpPost(engineDswarmAPI + CONFIGURATIONS_ENDPOINT);
			final String configurationJSONString = readFile(filename, Charsets.UTF_8);

			final StringEntity reqEntity = new StringEntity(configurationJSONString, ContentType.create(APPLICATION_JSON, Consts.UTF_8));

			httpPost.setEntity(reqEntity);

			logger.info(String.format("[%s] request : %s", serviceName, httpPost.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPost)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();
				final HttpEntity httpEntity = httpResponse.getEntity();

				final String message = String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				switch (statusCode) {

					case 201: {

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

	/**
	 * creates a data model from given resource + configuration JSON
	 *
	 * @param resourceJSON
	 * @param configurationJSON
	 * @oaram name
	 * @param description
	 * @return responseJson
	 * @throws Exception
	 */
	private String createDataModel(final JsonObject resourceJSON, final JsonObject configurationJSON, final String name, final String description, final String serviceName,
			final String engineDswarmAPI) throws Exception {


		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final HttpPost httpPost = new HttpPost(engineDswarmAPI + DATAMODELS_ENDPOINT);

			final StringWriter stringWriter = new StringWriter();
			final JsonGenerator jp = Json.createGenerator(stringWriter);

			jp.writeStartObject();
			jp.write(NAME_IDENTIFIER, name);
			jp.write(DESCRIPTION_IDENTIFIER, description);
			jp.write(CONFIGURATION_IDENTIFIER, configurationJSON);
			jp.write(DATA_RESOURCE_IDENTIFIER, resourceJSON);
			jp.writeEnd();

			jp.flush();
			jp.close();

			final StringEntity reqEntity = new StringEntity(stringWriter.toString(), ContentType.create(APPLICATION_JSON, Consts.UTF_8));

			stringWriter.flush();
			stringWriter.close();

			httpPost.setEntity(reqEntity);

			logger.info(String.format("[%s] request : %s", serviceName, httpPost.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPost)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();
				final HttpEntity httpEntity = httpResponse.getEntity();

				final String message = String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				switch (statusCode) {

					case 201: {

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

	private static String readFile(String path, Charset encoding) throws IOException {

		final byte[] encoded = Files.readAllBytes(Paths.get(path));

		return new String(encoded, encoding);
	}
}

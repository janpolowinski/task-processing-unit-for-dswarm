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

import de.tu_dortmund.ub.data.util.TPUUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private static final Logger LOG = LoggerFactory.getLogger(Init.class);

	public static final String FILE_IDENTIFIER          = "file";
	public static final String CONFIGURATION_IDENTIFIER = "configuration";
	public static final String DATA_MODEL_ID            = "data_model_id";
	public static final String RESOURCE_ID              = "resource_id";

	private final Properties config;
	private final String     initResourceFile;
	private final int        cnt;

	public Init(final String initResourceFile, final Properties config, final int cnt) {

		this.initResourceFile = initResourceFile;
		this.config = config;
		this.cnt = cnt;
	}

	//    @Override
	public String call() {

		final String serviceName = config.getProperty(TPUStatics.SERVICE_NAME_IDENTIFIER);
		final String engineDswarmAPI = config.getProperty(TPUStatics.ENGINE_DSWARM_API_IDENTIFIER);

		LOG.info(String.format("[%s][%d] Starting 'Init (Task)' ...", serviceName, cnt));

		try {

			final boolean doIngest;

			final String doIngestString = config.getProperty(TPUStatics.DO_INITIAL_DATA_MODEL_INGEST_IDENTIFIER);

			if (doIngestString != null && !doIngestString.trim().isEmpty()) {

				doIngest = Boolean.valueOf(doIngestString);
			} else {

				// default = true
				doIngest = true;
			}

			if (doIngest) {

				LOG.debug("[{}][{}] do data model creation with data ingest", serviceName, cnt);

				TPUUtil.initSchemaIndices(serviceName, config);
			}

			// build a InputDataModel for the resource
			//            String inputResourceJson = uploadFileToDSwarm(resource, "resource for project '" + resource, config.getProperty("project.name") + "' - case " + cnt);
			final String name = String.format("resource for project '%s'", initResourceFile);
			final String description = String.format("'resource does not belong to a project' - case %d", cnt);
			final String inputResourceJson = uploadFileAndCreateResource(initResourceFile, name, description, serviceName, engineDswarmAPI);

			if (inputResourceJson == null) {

				LOG.error("something went wrong at resource creation");

				return null;
			}

			final JsonReader inputResourceJsonReader = Json.createReader(IOUtils.toInputStream(inputResourceJson, APIStatics.UTF_8));
			final JsonObject inputResourceJSON = inputResourceJsonReader.readObject();
			final String inputResourceID = inputResourceJSON.getString(DswarmBackendStatics.UUID_IDENTIFIER);
			LOG.info(String.format("[%s][%d] input resource id = %s", serviceName, cnt, inputResourceID));

			if (inputResourceID == null) {

				LOG.error("something went wrong at resource creation, no resource uuid available");

				return null;
			}

			// create configuration
			final String configurationFileName = config.getProperty(TPUStatics.CONFIGURATION_NAME_IDENTIFIER);
			final String configurationJSONString = createConfiguration(configurationFileName, serviceName, engineDswarmAPI);

			if (configurationJSONString == null) {

				LOG.error("something went wrong at configuration creation");

				return null;
			}

			final JsonReader configurationJsonReader = Json.createReader(IOUtils.toInputStream(configurationJSONString, APIStatics.UTF_8));
			final JsonObject configurationJSON = configurationJsonReader.readObject();
			final String configurationID = configurationJSON.getString(DswarmBackendStatics.UUID_IDENTIFIER);
			LOG.info(String.format("[%s][%d] configuration id = %s", serviceName, cnt, configurationID));

			if (configurationID == null) {

				LOG.error("something went wrong at configuration creation, no configuration uuid available");

				return null;
			}

			// create the datamodel (will use it's resource)
			final String dataModelName = String.format("data model %d", cnt);
			final String dataModelDescription = String.format("data model description %d", cnt);
			final String dataModelJSONString = createDataModel(inputResourceJSON, configurationJSON, dataModelName, dataModelDescription, serviceName,
					engineDswarmAPI, doIngest);

			if (dataModelJSONString == null) {

				LOG.error("something went wrong at data model creation");

				return null;
			}

			final JsonReader dataModelJsonReader = Json.createReader(IOUtils.toInputStream(dataModelJSONString, APIStatics.UTF_8));
			final JsonObject dataModelJSON = dataModelJsonReader.readObject();
			final String dataModelID = dataModelJSON.getString(DswarmBackendStatics.UUID_IDENTIFIER);
			LOG.info(String.format("[%s][%d] data model id = %s", serviceName, cnt, dataModelID));

			if (dataModelID == null) {

				LOG.error("something went wrong at data model creation, no data model uuid available");

				return null;
			}

			// we don't need to transform after each ingest of a slice of records,
			// so transform and export will be done separately
			LOG.info(String.format("[%s][%d] (Note: Only ingest, but no transformation or export done.)", serviceName, cnt));

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

			LOG.error(String.format("[%s][%d] Processing resource '%s' failed with a %s", serviceName, cnt, initResourceFile,
							e.getClass().getSimpleName()),
					e);
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

			final HttpPost httpPost = new HttpPost(engineDswarmAPI + DswarmBackendStatics.RESOURCES_ENDPOINT);

			final File file1 = new File(filename);
			final FileBody fileBody = new FileBody(file1);
			final StringBody stringBodyForName = new StringBody(name, ContentType.TEXT_PLAIN);
			final StringBody stringBodyForDescription = new StringBody(description, ContentType.TEXT_PLAIN);

			final HttpEntity reqEntity = MultipartEntityBuilder.create()
					.addPart(DswarmBackendStatics.NAME_IDENTIFIER, stringBodyForName)
					.addPart(DswarmBackendStatics.DESCRIPTION_IDENTIFIER, stringBodyForDescription)
					.addPart(FILE_IDENTIFIER, fileBody)
					.build();

			httpPost.setEntity(reqEntity);

			LOG.info(String.format("[%s][%d] request : %s", serviceName, cnt, httpPost.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPost)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();
				final HttpEntity httpEntity = httpResponse.getEntity();

				final String message = String.format("[%s][%d] %d : %s", serviceName, cnt, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				switch (statusCode) {

					case 201: {

						LOG.info(message);
						final StringWriter writer = new StringWriter();
						IOUtils.copy(httpEntity.getContent(), writer, APIStatics.UTF_8);
						final String responseJson = writer.toString();
						writer.flush();
						writer.close();

						LOG.debug(String.format("[%s][%d] responseJson : %s", serviceName, cnt, responseJson));

						return responseJson;
					}
					default: {

						LOG.error(message);
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

			final HttpPost httpPost = new HttpPost(engineDswarmAPI + DswarmBackendStatics.CONFIGURATIONS_ENDPOINT);
			final String configurationJSONString = readFile(filename, Charsets.UTF_8);

			final StringEntity reqEntity = new StringEntity(configurationJSONString,
					ContentType.create(APIStatics.APPLICATION_JSON_MIMETYPE, Consts.UTF_8));

			httpPost.setEntity(reqEntity);

			LOG.info(String.format("[%s][%d] request : %s", serviceName, cnt, httpPost.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPost)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();
				final HttpEntity httpEntity = httpResponse.getEntity();

				final String message = String.format("[%s][%d] %d : %s", serviceName, cnt, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				switch (statusCode) {

					case 201: {

						LOG.info(message);
						final StringWriter writer = new StringWriter();
						IOUtils.copy(httpEntity.getContent(), writer, APIStatics.UTF_8);
						final String responseJson = writer.toString();
						writer.flush();
						writer.close();

						LOG.debug(String.format("[%s][%d] responseJson : %s", serviceName, cnt, responseJson));

						return responseJson;
					}
					default: {

						LOG.error(message);
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
	private String createDataModel(final JsonObject resourceJSON, final JsonObject configurationJSON, final String name, final String description,
			final String serviceName, final String engineDswarmAPI, final boolean doIngest) throws Exception {

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final String uri = engineDswarmAPI + DswarmBackendStatics.DATAMODELS_ENDPOINT + APIStatics.QUESTION_MARK
					+ DswarmBackendStatics.DO_DATA_MODEL_INGEST_IDENTIFIER + APIStatics.EQUALS + doIngest;

			final HttpPost httpPost = new HttpPost(uri);

			final StringWriter stringWriter = new StringWriter();
			final JsonGenerator jp = Json.createGenerator(stringWriter);

			jp.writeStartObject();
			jp.write(DswarmBackendStatics.NAME_IDENTIFIER, name);
			jp.write(DswarmBackendStatics.DESCRIPTION_IDENTIFIER, description);
			jp.write(CONFIGURATION_IDENTIFIER, configurationJSON);
			jp.write(DswarmBackendStatics.DATA_RESOURCE_IDENTIFIER, resourceJSON);
			jp.writeEnd();

			jp.flush();
			jp.close();

			final StringEntity reqEntity = new StringEntity(stringWriter.toString(),
					ContentType.create(APIStatics.APPLICATION_JSON_MIMETYPE, Consts.UTF_8));

			stringWriter.flush();
			stringWriter.close();

			httpPost.setEntity(reqEntity);

			LOG.info(String.format("[%s][%d] request : %s", serviceName, cnt, httpPost.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPost)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();
				final HttpEntity httpEntity = httpResponse.getEntity();

				final String message = String.format("[%s][%d] %d : %s", serviceName, cnt, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				switch (statusCode) {

					case 201: {

						LOG.info(message);
						final StringWriter writer = new StringWriter();
						IOUtils.copy(httpEntity.getContent(), writer, APIStatics.UTF_8);
						final String responseJson = writer.toString();
						writer.flush();
						writer.close();

						LOG.debug(String.format("[%s][%d] responseJson : %s", serviceName, cnt, responseJson));

						return responseJson;
					}
					default: {

						LOG.error(message);
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

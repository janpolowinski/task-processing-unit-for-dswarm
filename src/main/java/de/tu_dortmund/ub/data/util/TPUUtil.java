package de.tu_dortmund.ub.data.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import de.tu_dortmund.ub.data.TPUException;
import de.tu_dortmund.ub.data.dswarm.APIStatics;
import de.tu_dortmund.ub.data.dswarm.Init;
import de.tu_dortmund.ub.data.dswarm.TPUStatics;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public final class TPUUtil {

	private static final Logger LOG = LoggerFactory.getLogger(TPUUtil.class);

	public static final String EXPORT_FILE_NAME_PREFIX = "export-of-";
	public static final String DOT                     = ".";
	public static final String XML_FILE_ENDING         = "xml";
	public static final String UTF_8                   = "UTF-8";
	public static final String MAINTAIN_ENDPOINT       = "maintain";
	public static final String SCHEMA_INDICES_ENDPOINT = "schemaindices";
	public static final String TEXT_PLAIN_MIMETYPE     = "text/plain";
	public static final int    MAX_BUFFER_LENGTH       = 10000;
	public static final String ERROR_MESSAGE_START = "{\"error\":{\"message";

	public static Optional<Boolean> getBooleanConfigValue(final String configKey, final Properties config) {

		final String configValue = config.getProperty(configKey);

		final Optional<Boolean> optionalConfigValue;

		if (configValue != null && !configValue.trim().isEmpty()) {

			optionalConfigValue = Optional.of(Boolean.valueOf(configValue));
		} else {

			optionalConfigValue = Optional.empty();
		}

		return optionalConfigValue;
	}

	public static Optional<String> getStringConfigValue(final String configKey, final Properties config) {

		final String configValue = config.getProperty(configKey);

		final Optional<String> optionalConfigValue;

		if (configValue != null && !configValue.trim().isEmpty()) {

			optionalConfigValue = Optional.of(configValue);
		} else {

			optionalConfigValue = Optional.empty();
		}

		return optionalConfigValue;
	}

	public static String writeResultToFile(final CloseableHttpResponse httpResponse, final Properties config, final String exportDataModelID)
			throws IOException, TPUException {

		LOG.info("try to write result to file");

		final String persistInFolderString = config.getProperty(TPUStatics.PERSIST_IN_FOLDER_IDENTIFIER);
		final boolean persistInFolder = Boolean.parseBoolean(persistInFolderString);
		final HttpEntity entity = httpResponse.getEntity();

		final String fileName;

		if (persistInFolder) {

			final InputStream responseStream = entity.getContent();
			final BufferedInputStream bis = new BufferedInputStream(responseStream, 1024);

			final String resultsFolder = config.getProperty(TPUStatics.RESULTS_FOLDER_IDENTIFIER);
			fileName = resultsFolder + File.separatorChar + EXPORT_FILE_NAME_PREFIX + exportDataModelID + DOT + XML_FILE_ENDING;

			LOG.info(String.format("start writing result to file '%s'", fileName));

			final FileOutputStream outputStream = new FileOutputStream(fileName);
			final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

			IOUtils.copy(bis, bufferedOutputStream);
			bufferedOutputStream.flush();
			outputStream.flush();
			bis.close();
			responseStream.close();
			bufferedOutputStream.close();
			outputStream.close();

			checkResultForError(fileName);
		} else {

			fileName = "[no file name available]";
		}

		EntityUtils.consume(entity);

		return fileName;
	}

	private static void checkResultForError(String fileName) throws IOException, TPUException {

		final Path filePath = Paths.get(fileName);
		final char[] buffer = new char[MAX_BUFFER_LENGTH];
		final BufferedReader bufferedReader = Files.newBufferedReader(filePath, Charsets.UTF_8);
		final int readCharacters = bufferedReader.read(buffer, 0, MAX_BUFFER_LENGTH);

		if(readCharacters <= -1) {

			LOG.debug("couldn't check file for errors; no file content in file '{}'", fileName);

			return;
		}

		final String bufferString = String.valueOf(buffer);

		if(bufferString.startsWith(ERROR_MESSAGE_START)) {


			throw new TPUException(bufferString);
		}
	}

	public static JsonObject doInit(final String resourceWatchFolder, final String initResourceFileName, final String serviceName,
			final Integer engineThreads, final Properties config, final int cnt)
			throws Exception {

		final String initResourceFile = resourceWatchFolder + File.separatorChar + initResourceFileName;

		final String initResultJSONString = TPUUtil.executeInit(initResourceFile, serviceName, engineThreads, config, cnt);

		if (initResultJSONString == null) {

			final String message = "couldn't create data model";

			LOG.error(message);

			throw new Exception(message);
		}

		final JsonReader initResultJsonReader = Json.createReader(IOUtils.toInputStream(initResultJSONString, UTF_8));
		final JsonObject initResultJSON = initResultJsonReader.readObject();

		if (initResultJSON == null) {

			final String message = "couldn't create data model";

			LOG.error(message);

			throw new Exception(message);
		}

		return initResultJSON;
	}

	public static String executeInit(final String initResourceFile, final String serviceName, final Integer engineThreads, final Properties config,
			final int cnt)
			throws Exception {

		// create job
		final Callable<String> initTask = new Init(initResourceFile, config, cnt);

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		try {

			final List<Callable<String>> tasks = new LinkedList<>();
			tasks.add(initTask);

			final List<Future<String>> futureList = pool.invokeAll(tasks);
			final Iterator<Future<String>> iterator = futureList.iterator();

			if (iterator.hasNext()) {

				final Future<String> f = iterator.next();

				final String initResult = f.get();

				final String message1 = String.format("[%s][%d] initResult = '%s'", serviceName, cnt, initResult);

				LOG.info(message1);

				return initResult;
			}

		} catch (final Exception e) {

			LOG.error("[{]][{}] something went wrong at init part execution", serviceName, cnt, e);

			throw e;
		} finally {

			pool.shutdown();
		}

		return null;
	}

	/**
	 * inits schema indices or ensures that they are there
	 *
	 * @param serviceName
	 * @throws Exception
	 */
	public static String initSchemaIndices(final String serviceName, final Properties config) throws Exception {

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final String engineDswarmGraphAPI = config.getProperty(TPUStatics.ENGINE_DSWARM_GRAPH_API_IDENTIFIER);

			final HttpPost httpPost = new HttpPost(engineDswarmGraphAPI + MAINTAIN_ENDPOINT + APIStatics.SLASH + SCHEMA_INDICES_ENDPOINT);
			final StringEntity reqEntity = new StringEntity("", ContentType.create(TEXT_PLAIN_MIMETYPE, Consts.UTF_8));

			httpPost.setEntity(reqEntity);

			LOG.info(String.format("[%s] request : '%s'", serviceName, httpPost.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPost)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();

				final String message = String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				final String response = TPUUtil.getResponseMessage(httpResponse);

				switch (statusCode) {

					case 200: {

						LOG.info(message);

						LOG.info(String.format("[%s] response : '%s'", serviceName, response));

						return response;
					}
					default: {

						LOG.error(message);

						throw new Exception("something went wrong at schema indices initialisation: " + message + " " + response);
					}
				}
			}
		}
	}

	public static JsonObject getJsonObject(final String jsonString) throws IOException {

		final JsonReader jsonReader = Json.createReader(IOUtils.toInputStream(jsonString, APIStatics.UTF_8));
		final JsonObject jsonObject = jsonReader.readObject();

		jsonReader.close();

		return jsonObject;
	}

	public static String getResponseMessage(final CloseableHttpResponse httpResponse) throws IOException {

		final HttpEntity httpEntity = httpResponse.getEntity();

		final String response = getResponseMessage(httpEntity);

		EntityUtils.consume(httpEntity);

		return response;
	}

	public static String getResponseMessage(final HttpEntity httpEntity) throws IOException {

		final StringWriter writer = new StringWriter();
		IOUtils.copy(httpEntity.getContent(), writer, APIStatics.UTF_8);
		final String response = writer.toString();
		writer.flush();
		writer.close();

		return response;
	}
}

package de.tu_dortmund.ub.data.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import de.tu_dortmund.ub.data.dswarm.Init;
import de.tu_dortmund.ub.data.dswarm.TPUStatics;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.log4j.Logger;

/**
 * @author tgaengler
 */
public final class TPUUtil {

	private static Logger logger = Logger.getLogger(TPUUtil.class.getName());

	public static final String EXPORT_FILE_NAME_PREFIX = "export-of-";
	public static final String DOT                     = ".";
	public static final String XML_FILE_ENDING         = "xml";
	public static final String UTF_8                   = "UTF-8";

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

	public static void writeResultToFile(final CloseableHttpResponse httpResponse, final Properties config, final String exportDataModelID)
			throws IOException {

		logger.info("try to write result to file");

		final String persistInFolderString = config.getProperty(TPUStatics.PERSIST_IN_FOLDER_IDENTIFIER);
		final boolean persistInFolder = Boolean.parseBoolean(persistInFolderString);

		if (persistInFolder) {

			final InputStream responseStream = httpResponse.getEntity().getContent();
			final BufferedInputStream bis = new BufferedInputStream(responseStream, 1024);

			final String resultsFolder = config.getProperty(TPUStatics.RESULTS_FOLDER_IDENTIFIER);
			final String fileName = resultsFolder + File.separatorChar + EXPORT_FILE_NAME_PREFIX + exportDataModelID + DOT + XML_FILE_ENDING;

			logger.info(String.format("start writing result to file '%s'", fileName));

			final FileOutputStream outputStream = new FileOutputStream(fileName);
			final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

			IOUtils.copy(bis, bufferedOutputStream);
			bufferedOutputStream.flush();
			outputStream.flush();
			bis.close();
			responseStream.close();
			bufferedOutputStream.close();
			outputStream.close();
		}
	}

	public static JsonObject doInit(final String resourceWatchFolder, final String initResourceFileName, final String serviceName,
			final Integer engineThreads, final Properties config)
			throws Exception {

		final String initResourceFile = resourceWatchFolder + File.separatorChar + initResourceFileName;

		final String initResultJSONString = TPUUtil.executeInit(initResourceFile, serviceName, engineThreads, config);

		if (initResultJSONString == null) {

			final String message = "couldn't create data model";

			logger.error(message);

			throw new Exception(message);
		}

		final JsonReader initResultJsonReader = Json.createReader(IOUtils.toInputStream(initResultJSONString, UTF_8));
		final JsonObject initResultJSON = initResultJsonReader.readObject();

		if (initResultJSON == null) {

			final String message = "couldn't create data model";

			logger.error(message);

			throw new Exception(message);
		}

		return initResultJSON;
	}

	public static String executeInit(final String initResourceFile, final String serviceName, final Integer engineThreads, final Properties config) throws Exception {

		// create job
		final int cnt = 0;
		final Callable<String> initTask = new Init(initResourceFile, config, logger, cnt);

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

				final String message1 = String.format("[%s] initResult = '%s'", serviceName, initResult);

				logger.info(message1);

				return initResult;
			}

		} catch (final InterruptedException | ExecutionException e) {

			logger.error("something went wrong", e);
		} finally {

			pool.shutdown();
		}

		return null;
	}
}

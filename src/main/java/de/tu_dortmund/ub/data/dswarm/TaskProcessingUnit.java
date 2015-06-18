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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Task Processing Unit for d:swarm
 *
 * @author Dipl.-Math. Hans-Georg Becker, M.L.I.S. (UB Dortmund)
 * @version 2015-04-20
 *
 */
public final class TaskProcessingUnit {

	private static final String     CONFIG_PROPERTIES_FILE_NAME = "config.properties";
	private static final String     CONF_FOLDER_NAME            = "conf";
	public static final  String     UTF_8                       = "UTF-8";
	private static       Properties config                      = new Properties();

	private static Logger logger = Logger.getLogger(TaskProcessingUnit.class.getName());

	public static void main(final String[] args) throws Exception {

		// config
		String conffile = CONF_FOLDER_NAME + File.separatorChar + CONFIG_PROPERTIES_FILE_NAME;

		// read program parameters
		if (args.length > 0) {

			for (final String arg : args) {

				logger.info("arg = " + arg);

				if (arg.startsWith("-conf=")) {

					conffile = arg.split("=")[1];
				}
			}
		}

		// Init properties
		try {

			try (final InputStream inputStream = new FileInputStream(conffile)) {

				try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, UTF_8))) {

					config.load(reader);
				}
			}
		} catch (final IOException e) {

			logger.error("something went wrong", e);
			logger.error(String.format("FATAL ERROR: Could not read '%s'!", conffile));
		}

		// init logger
		PropertyConfigurator.configure(config.getProperty("service.log4j-conf"));

		final String serviceName = config.getProperty(TPUStatics.SERVICE_NAME_IDENTIFIER);

		logger.info(String.format("[%s] Starting 'Task Processing Unit' ...", serviceName));
		logger.info(String.format("[%s] conf-file = %s", serviceName, conffile));

		final String log4jConfFile = config.getProperty(TPUStatics.SERVICE_LOG4J_CONF_IDENTIFIER);

		logger.info(String.format("[%s] log4j-conf-file = %s", serviceName, log4jConfFile));

		final String resourceWatchFolder = config.getProperty(TPUStatics.RESOURCE_WATCHFOLDER_IDENTIFIER);
		String[] watchFolderFiles = new File(resourceWatchFolder).list();
		Arrays.sort(watchFolderFiles);

		final String filesMessage = String.format("[%s] Files in %s", serviceName, resourceWatchFolder);

		logger.info(filesMessage);
		logger.info(Arrays.toString(watchFolderFiles));

		// Init time counter
		final long global = System.currentTimeMillis();

		final Integer engineThreads = Integer.parseInt(config.getProperty(TPUStatics.ENGINE_THREADS_IDENTIFIER));

		final String doInitString = config.getProperty(TPUStatics.DO_INIT_IDENTIFIER);
		final boolean doInit = Boolean.parseBoolean(doInitString);

		// TODO: go multi thread, if ingest on-the-fly + export-on-the-fly is enabled as well
		final String allowMultipleDataModelsString = config.getProperty(TPUStatics.ALLOW_MULTIPLE_DATA_MODELS_IDENTIFIER, Boolean.FALSE.toString());
		final boolean allowMultipleDataModels = Boolean.parseBoolean(allowMultipleDataModelsString);

		// keys = input data models; values = related data resources
		final Map<String, String> inputDataModelsAndResources = new HashMap<>();

		// init
		if (doInit) {

			if (allowMultipleDataModels) {

				for (int i = 0; i < watchFolderFiles.length; i++) {

					final String initResourceFileName = watchFolderFiles[i];

					doInit(resourceWatchFolder, initResourceFileName, serviceName, engineThreads, inputDataModelsAndResources);

					// remove the file already processed during init from the files list to avoid duplicates
					watchFolderFiles = ArrayUtils.removeElement(watchFolderFiles, initResourceFileName);
				}
			} else {

				// use the first file in the folder for init
				final String initResourceFileName = watchFolderFiles[0];

				doInit(resourceWatchFolder, initResourceFileName, serviceName, engineThreads, inputDataModelsAndResources);

				// remove the file already processed during init from the files list to avoid duplicates
				watchFolderFiles = ArrayUtils.removeElement(watchFolderFiles, initResourceFileName);
			}
		} else {

			final String inputDataModelID = config.getProperty(TPUStatics.PROTOTYPE_INPUT_DATA_MODEL_ID_IDENTIFIER);
			final String resourceID = config.getProperty(TPUStatics.PROTOTYPE_RESOURCE_ID_INDENTIFIER);

			inputDataModelsAndResources.put(inputDataModelID, resourceID);

			logger.info("skip init part");
		}

		final String doIngestString = config.getProperty(TPUStatics.DO_INGEST_IDENTIFIER);
		final boolean doIngest = Boolean.parseBoolean(doIngestString);

		// ingest
		if (doIngest) {

			final String projectName = config.getProperty(TPUStatics.PROJECT_NAME_IDENTIFIER);

			if (!allowMultipleDataModels) {

				final Set<Map.Entry<String, String>> entries = inputDataModelsAndResources.entrySet();
				final Iterator<Map.Entry<String, String>> iterator = entries.iterator();
				final Map.Entry<String, String> entry = iterator.next();

				final String inputDataModelID = entry.getKey();
				final String resourceID = entry.getValue();

				executeIngests(watchFolderFiles, inputDataModelID, resourceID, projectName, serviceName, engineThreads);
			}
		} else {

			logger.info("skip ingest");
		}

		final String outputDataModelID = config.getProperty(TPUStatics.PROTOTYPE_OUTPUT_DATA_MODEL_ID_IDENTIFIER);

		final String doTransformationsString = config.getProperty(TPUStatics.DO_TRANSFORMATIONS_IDENTIFIER);
		final boolean doTransformations = Boolean.parseBoolean(doTransformationsString);

		// task execution
		if (doTransformations) {

			if (allowMultipleDataModels) {

				final Set<Map.Entry<String, String>> entries = inputDataModelsAndResources.entrySet();

				for (final Map.Entry<String, String> entry : entries) {

					final String inputDataModelID = entry.getKey();

					executeTransform(inputDataModelID, outputDataModelID, engineThreads, serviceName);
				}
			} else {

				final Set<Map.Entry<String, String>> entries = inputDataModelsAndResources.entrySet();
				final Iterator<Map.Entry<String, String>> iterator = entries.iterator();
				final Map.Entry<String, String> entry = iterator.next();

				final String inputDataModelID = entry.getKey();

				executeTransform(inputDataModelID, outputDataModelID, engineThreads, serviceName);
			}
		} else {

			logger.info("skip transformations");
		}

		final String doExportString = config.getProperty(TPUStatics.DO_EXPORT_IDENTIFIER);
		final boolean doExport = Boolean.parseBoolean(doExportString);

		// export
		if (doExport) {

			if (!allowMultipleDataModels) {

				final String exportDataModelID;

				if (outputDataModelID != null && !outputDataModelID.trim().isEmpty()) {

					exportDataModelID = outputDataModelID;
				} else {

					final Set<Map.Entry<String, String>> entries = inputDataModelsAndResources.entrySet();
					final Iterator<Map.Entry<String, String>> iterator = entries.iterator();
					final Map.Entry<String, String> entry = iterator.next();

					exportDataModelID = entry.getKey();
				}

				executeExport(exportDataModelID, engineThreads, serviceName);
			}
		} else {

			logger.info("skip export");
		}

		final String tasksExecutedMessage = String
				.format("[%s] d:swarm tasks executed. (Processing time: %d s)", serviceName, (
						(System.currentTimeMillis() - global) / 1000));
		logger.info(tasksExecutedMessage);
	}

	private static String executeInit(final String initResourceFile, final String serviceName, final Integer engineThreads) throws Exception {

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

	private static void executeIngests(final String[] files, final String dataModelID, final String resourceID, final String projectName,
			final String serviceName, final Integer engineThreads) throws Exception {

		// create job list
		final LinkedList<Callable<String>> filesToPush = new LinkedList<>();

		int cnt = 0;
		for (final String file : files) {

			cnt++;
			filesToPush.add(new Ingest(config, logger, file, dataModelID, resourceID, projectName, cnt));
		}

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		try {

			final List<Future<String>> futureList = pool.invokeAll(filesToPush);

			for (final Future<String> f : futureList) {

				final String message = f.get();

				final String message1 = String.format("[%s] %s", serviceName, message);

				logger.info(message1);
			}

		} catch (final InterruptedException | ExecutionException e) {

			logger.error("something went wrong", e);

		} finally {

			pool.shutdown();
		}
	}

	private static void executeTransform(final String inputDataModelID, final String outputDataModelID, final Integer engineThreads,
			final String serviceName) throws Exception {

		// create job list
		final LinkedList<Callable<String>> transforms = new LinkedList<>();
		transforms.add(new Transform(config, inputDataModelID, outputDataModelID, logger));

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		try {

			final List<Future<String>> futureList = pool.invokeAll(transforms);

			for (final Future<String> f : futureList) {

				final String message = f.get();

				final String message1 = String.format("[%s] %s", serviceName, message);

				logger.info(message1);
			}

		} catch (final InterruptedException | ExecutionException e) {

			logger.error("something went wrong", e);

		} finally {

			pool.shutdown();
		}
	}

	private static void executeExport(final String exportDataModelID, final Integer engineThreads, final String serviceName) throws Exception {

		// create job list
		final LinkedList<Callable<String>> exports = new LinkedList<>();
		exports.add(new Export(exportDataModelID, config, logger));

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		try {

			final List<Future<String>> futureList = pool.invokeAll(exports);

			for (final Future<String> f : futureList) {

				final String message = f.get();

				final String message1 = String.format("[%s] %s", serviceName, message);

				logger.info(message1);
			}

		} catch (final InterruptedException | ExecutionException e) {

			logger.error("something went wrong", e);

		} finally {

			pool.shutdown();
		}
	}

	private static void doInit(final String resourceWatchFolder, final String initResourceFileName, final String serviceName,
			final Integer engineThreads, final Map<String, String> inputDataModelsAndResources)
			throws Exception {

		final String initResourceFile = resourceWatchFolder + File.separatorChar + initResourceFileName;

		final String initResultJSONString = executeInit(initResourceFile, serviceName, engineThreads);

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

		final String inputDataModelID = initResultJSON.getString(Init.DATA_MODEL_ID);
		final String resourceID = initResultJSON.getString(Init.RESOURCE_ID);

		inputDataModelsAndResources.put(inputDataModelID, resourceID);
	}
}

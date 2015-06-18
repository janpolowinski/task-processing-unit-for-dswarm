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
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.json.JsonObject;

import de.tu_dortmund.ub.data.util.TPUUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task Processing Unit for d:swarm
 *
 * @author Dipl.-Math. Hans-Georg Becker, M.L.I.S. (UB Dortmund)
 * @version 2015-04-20
 *
 */
public final class TaskProcessingUnit {

	private static final Logger LOG = LoggerFactory.getLogger(TaskProcessingUnit.class);

	private static final String     CONFIG_PROPERTIES_FILE_NAME = "config.properties";
	private static final String     CONF_FOLDER_NAME            = "conf";
	private static       Properties config                      = new Properties();

	public static void main(final String[] args) throws Exception {

		// config
		String conffile = CONF_FOLDER_NAME + File.separatorChar + CONFIG_PROPERTIES_FILE_NAME;

		// read program parameters
		if (args.length > 0) {

			for (final String arg : args) {

				LOG.info("arg = " + arg);

				if (arg.startsWith("-conf=")) {

					conffile = arg.split("=")[1];
				}
			}
		}

		// Init properties
		try {

			try (final InputStream inputStream = new FileInputStream(conffile)) {

				try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, TPUUtil.UTF_8))) {

					config.load(reader);
				}
			}
		} catch (final IOException e) {

			LOG.error("something went wrong", e);
			LOG.error(String.format("FATAL ERROR: Could not read '%s'!", conffile));
		}

		final String serviceName = config.getProperty(TPUStatics.SERVICE_NAME_IDENTIFIER);

		LOG.info(String.format("[%s] Starting 'Task Processing Unit' ...", serviceName));
		LOG.info(String.format("[%s] conf-file = %s", serviceName, conffile));

		final String resourceWatchFolder = config.getProperty(TPUStatics.RESOURCE_WATCHFOLDER_IDENTIFIER);
		String[] watchFolderFiles = new File(resourceWatchFolder).list();
		Arrays.sort(watchFolderFiles);

		final String filesMessage = String.format("[%s] Files in %s", serviceName, resourceWatchFolder);

		LOG.info(filesMessage);
		LOG.info(Arrays.toString(watchFolderFiles));

		// Init time counter
		final long global = System.currentTimeMillis();

		final Integer engineThreads = Integer.parseInt(config.getProperty(TPUStatics.ENGINE_THREADS_IDENTIFIER));

		final Optional<Boolean> optionalDoInit = TPUUtil.getBooleanConfigValue(TPUStatics.DO_INIT_IDENTIFIER, config);
		final Optional<Boolean> optionalDoTransformations = TPUUtil.getBooleanConfigValue(TPUStatics.DO_TRANSFORMATIONS_IDENTIFIER, config);

		// TODO: go multi threaded, if ingest on-the-fly + export-on-the-fly is enabled as well
		final Optional<Boolean> optionalAllowMultipleDataModels = TPUUtil
				.getBooleanConfigValue(TPUStatics.ALLOW_MULTIPLE_DATA_MODELS_IDENTIFIER, config);

		final Optional<Boolean> optionalDoIngestOnTheFly = TPUUtil.getBooleanConfigValue(TPUStatics.DO_INGEST_ON_THE_FLY_IDENTIFIER, config);
		final Optional<Boolean> optionalDoExportOnTheFly = TPUUtil.getBooleanConfigValue(TPUStatics.DO_EXPORT_ON_THE_FLY_IDENTIFIER, config);

		if (optionalDoInit.isPresent() && optionalDoInit.get() &&
				optionalAllowMultipleDataModels.isPresent() && optionalAllowMultipleDataModels.get() &&
				optionalDoTransformations.isPresent() && optionalDoTransformations.get() &&
				optionalDoIngestOnTheFly.isPresent() && optionalDoIngestOnTheFly.get() &&
				optionalDoExportOnTheFly.isPresent() && optionalDoExportOnTheFly.get()) {

			executeTPUTask(watchFolderFiles, resourceWatchFolder, engineThreads, serviceName);
		} else {

			executeTPUPartsOnDemand(optionalDoInit, optionalAllowMultipleDataModels, watchFolderFiles, resourceWatchFolder, serviceName,
					engineThreads, optionalDoTransformations, optionalDoIngestOnTheFly, optionalDoExportOnTheFly);
		}

		final String tasksExecutedMessage = String
				.format("[%s] d:swarm tasks executed. (Processing time: %d s)", serviceName, (
						(System.currentTimeMillis() - global) / 1000));
		LOG.info(tasksExecutedMessage);
	}

	private static void executeTPUTask(final String[] watchFolderFiles, final String resourceWatchFolder, final Integer engineThreads,
			final String serviceName) throws Exception {

		// create job list
		final LinkedList<Callable<String>> transforms = new LinkedList<>();

		int cnt = 1;

		for (final String watchFolderFile : watchFolderFiles) {

			LOG.info("[{}][{}] do TPU task execution '{}' for file '{}'", serviceName, cnt, cnt, watchFolderFile);

			transforms.add(new TPUTask(config, watchFolderFile, resourceWatchFolder, serviceName, cnt));

			cnt++;
		}

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		try {

			final List<Future<String>> futureList = pool.invokeAll(transforms);

			for (final Future<String> f : futureList) {

				final String message = f.get();

				final String message1 = String.format("[%s] %s", serviceName, message);

				LOG.info(message1);
			}

		} catch (final InterruptedException | ExecutionException e) {

			LOG.error("something went wrong", e);

		} finally {

			pool.shutdown();
		}
	}

	private static void executeTPUPartsOnDemand(final Optional<Boolean> optionalDoInit, final Optional<Boolean> optionalAllowMultipleDataModels,
			String[] watchFolderFiles, final String resourceWatchFolder, final String serviceName, final Integer engineThreads,
			final Optional<Boolean> optionalDoTransformations, final Optional<Boolean> optionalDoIngestOnTheFly,
			final Optional<Boolean> optionalDoExportOnTheFly) throws Exception {

		// keys = input data models; values = related data resources
		final Map<String, String> inputDataModelsAndResources = new HashMap<>();

		// init
		if (optionalDoInit.isPresent() && optionalDoInit.get()) {

			if (optionalAllowMultipleDataModels.isPresent() && optionalAllowMultipleDataModels.get()) {

				for (int i = 0; i < watchFolderFiles.length; i++) {

					final String initResourceFileName = watchFolderFiles[i];

					doInit(resourceWatchFolder, initResourceFileName, serviceName, engineThreads, config, inputDataModelsAndResources);

					// remove the file already processed during init from the files list to avoid duplicates
					watchFolderFiles = ArrayUtils.removeElement(watchFolderFiles, initResourceFileName);
				}
			} else {

				// use the first file in the folder for init
				final String initResourceFileName = watchFolderFiles[0];

				doInit(resourceWatchFolder, initResourceFileName, serviceName, engineThreads, config, inputDataModelsAndResources);

				// remove the file already processed during init from the files list to avoid duplicates
				watchFolderFiles = ArrayUtils.removeElement(watchFolderFiles, initResourceFileName);
			}
		} else {

			final String inputDataModelID = config.getProperty(TPUStatics.PROTOTYPE_INPUT_DATA_MODEL_ID_IDENTIFIER);
			final String resourceID = config.getProperty(TPUStatics.PROTOTYPE_RESOURCE_ID_INDENTIFIER);

			inputDataModelsAndResources.put(inputDataModelID, resourceID);

			LOG.info("skip init part");
		}

		final Optional<Boolean> optionalDoIngest = TPUUtil.getBooleanConfigValue(TPUStatics.DO_INGEST_IDENTIFIER, config);

		// ingest
		if (optionalDoIngest.isPresent() && optionalDoIngest.get()) {

			final String projectName = config.getProperty(TPUStatics.PROJECT_NAME_IDENTIFIER);

			if (!optionalAllowMultipleDataModels.isPresent() || !optionalAllowMultipleDataModels.get()) {

				final Set<Map.Entry<String, String>> entries = inputDataModelsAndResources.entrySet();
				final Iterator<Map.Entry<String, String>> iterator = entries.iterator();
				final Map.Entry<String, String> entry = iterator.next();

				final String inputDataModelID = entry.getKey();
				final String resourceID = entry.getValue();

				executeIngests(watchFolderFiles, inputDataModelID, resourceID, projectName, serviceName, engineThreads);
			}
		} else {

			LOG.info("skip ingest");
		}

		final String outputDataModelID = config.getProperty(TPUStatics.PROTOTYPE_OUTPUT_DATA_MODEL_ID_IDENTIFIER);

		// task execution
		if (optionalDoTransformations.isPresent() && optionalDoTransformations.get()) {

			if (optionalAllowMultipleDataModels.isPresent() && optionalAllowMultipleDataModels.get()) {

				final Set<Map.Entry<String, String>> entries = inputDataModelsAndResources.entrySet();

				for (final Map.Entry<String, String> entry : entries) {

					final String inputDataModelID = entry.getKey();

					executeTransform(inputDataModelID, outputDataModelID, optionalDoIngestOnTheFly, optionalDoExportOnTheFly, engineThreads,
							serviceName);
				}
			} else {

				final Set<Map.Entry<String, String>> entries = inputDataModelsAndResources.entrySet();
				final Iterator<Map.Entry<String, String>> iterator = entries.iterator();
				final Map.Entry<String, String> entry = iterator.next();

				final String inputDataModelID = entry.getKey();

				executeTransform(inputDataModelID, outputDataModelID, optionalDoIngestOnTheFly, optionalDoExportOnTheFly, engineThreads, serviceName);
			}
		} else {

			LOG.info("skip transformations");
		}

		final Optional<Boolean> optionalDoExport = TPUUtil.getBooleanConfigValue(TPUStatics.DO_EXPORT_IDENTIFIER, config);

		// export
		if (optionalDoExport.isPresent() && optionalDoExport.get()) {

			if (!optionalAllowMultipleDataModels.isPresent() || !optionalAllowMultipleDataModels.get()) {

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

			LOG.info("skip export");
		}
	}

	private static void executeIngests(final String[] files, final String dataModelID, final String resourceID, final String projectName,
			final String serviceName, final Integer engineThreads) throws Exception {

		// create job list
		final LinkedList<Callable<String>> filesToPush = new LinkedList<>();

		int cnt = 0;
		for (final String file : files) {

			cnt++;
			filesToPush.add(new Ingest(config, file, dataModelID, resourceID, projectName, cnt));
		}

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		try {

			final List<Future<String>> futureList = pool.invokeAll(filesToPush);

			for (final Future<String> f : futureList) {

				final String message = f.get();

				final String message1 = String.format("[%s] %s", serviceName, message);

				LOG.info(message1);
			}

		} catch (final InterruptedException | ExecutionException e) {

			LOG.error("something went wrong", e);

		} finally {

			pool.shutdown();
		}
	}

	private static void executeTransform(final String inputDataModelID, final String outputDataModelID,
			final Optional<Boolean> optionalDoIngestOnTheFly, final Optional<Boolean> optionalDoExportOnTheFly, final Integer engineThreads,
			final String serviceName) throws Exception {

		// create job list
		final LinkedList<Callable<String>> transforms = new LinkedList<>();
		transforms.add(new Transform(config, inputDataModelID, outputDataModelID, optionalDoIngestOnTheFly, optionalDoExportOnTheFly, 0));

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		try {

			final List<Future<String>> futureList = pool.invokeAll(transforms);

			for (final Future<String> f : futureList) {

				final String message = f.get();

				final String message1 = String.format("[%s] %s", serviceName, message);

				LOG.info(message1);
			}

		} catch (final InterruptedException | ExecutionException e) {

			LOG.error("something went wrong", e);

		} finally {

			pool.shutdown();
		}
	}

	private static void executeExport(final String exportDataModelID, final Integer engineThreads, final String serviceName) throws Exception {

		// create job list
		final LinkedList<Callable<String>> exports = new LinkedList<>();
		exports.add(new Export(exportDataModelID, config));

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		try {

			final List<Future<String>> futureList = pool.invokeAll(exports);

			for (final Future<String> f : futureList) {

				final String message = f.get();

				final String message1 = String.format("[%s] %s", serviceName, message);

				LOG.info(message1);
			}

		} catch (final InterruptedException | ExecutionException e) {

			LOG.error("something went wrong", e);

		} finally {

			pool.shutdown();
		}
	}

	private static void doInit(final String resourceWatchFolder, final String initResourceFileName, final String serviceName,
			final Integer engineThreads, final Properties config, final Map<String, String> inputDataModelsAndResources)
			throws Exception {

		final JsonObject initResultJSON = TPUUtil.doInit(resourceWatchFolder, initResourceFileName, serviceName, engineThreads, config, 0);

		final String inputDataModelID = initResultJSON.getString(Init.DATA_MODEL_ID);
		final String resourceID = initResultJSON.getString(Init.RESOURCE_ID);

		inputDataModelsAndResources.put(inputDataModelID, resourceID);
	}
}

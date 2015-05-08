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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Task Processing Unit for d:swarm
 *
 * @author Dipl.-Math. Hans-Georg Becker, M.L.I.S. (UB Dortmund)
 * @version 2015-04-20
 *
 */
public class TaskProcessingUnit {

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

				System.out.println("arg = " + arg);

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
			System.out.println(String.format("FATAL ERROR: Could not read '%s'!", conffile));
		}

		// init logger
		PropertyConfigurator.configure(config.getProperty("service.log4j-conf"));

		final String serviceName = config.getProperty(TPUStatics.SERVICE_NAME_IDENTIFIER);

		logger.info(String.format("[%s] Starting 'Task Processing Unit' ...", serviceName));
		logger.info(String.format("[%s] conf-file = %s", serviceName, conffile));

		final String log4jConfFile = config.getProperty(TPUStatics.SERVICE_LOG4J_CONF_IDENTIFIER);

		logger.info(String.format("[%s] log4j-conf-file = %s", serviceName, log4jConfFile));
		System.out.println(String.format("[%s] Starting 'Task Processing Unit' ...", serviceName));
		System.out.println(String.format("[%s] conf-file = %s", serviceName, conffile));
		System.out.println(String.format("[%s] log4j-conf-file = %s", serviceName, log4jConfFile));

		final String resourceWatchFolder = config.getProperty(TPUStatics.RESOURCE_WATCHFOLDER_IDENTIFIER);
		final String[] files = new File(resourceWatchFolder).list();

		final String filesMessage = String.format("[%s] Files in %s", serviceName, resourceWatchFolder);

		logger.info(filesMessage);
		logger.info(Arrays.toString(files));
		System.out.println(filesMessage);
		System.out.println(Arrays.toString(files));

		// Init time counter
		final long global = System.currentTimeMillis();

		final Integer engineThreads = Integer.parseInt(config.getProperty(TPUStatics.ENGINE_THREADS_IDENTIFIER));

		final String initResultJSONString = executeInit(serviceName, engineThreads);

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

		final String dataModelID = initResultJSON.getString(Init.DATA_MODEL_ID);
		final String resourceID = initResultJSON.getString(Init.RESOURCE_ID);

		final String projectName = config.getProperty(TPUStatics.PROJECT_NAME_IDENTIFIER);

		// ingest
		executeIngests(files, dataModelID, resourceID, projectName, serviceName, engineThreads);

		final String outputDataModelID = config.getProperty(TPUStatics.PROTOTYPE_OUTPUT_DATA_MODEL_ID_IDENTIFIER);

		final String doTransformationsString = config.getProperty(TPUStatics.DO_TRANSFORMATIONS_IDENTIFIER);
		final boolean doTransformations = Boolean.parseBoolean(doTransformationsString);

		// task execution
		if (doTransformations) {

			executeTransform(dataModelID, outputDataModelID, engineThreads, serviceName);
		}

		final String doExportString = config.getProperty(TPUStatics.DO_EXPORT_IDENTIFIER);
		final boolean doExport = Boolean.parseBoolean(doExportString);

		// export
		if (doExport) {

			final String exportDataModelID;

			if (outputDataModelID != null && !outputDataModelID.trim().isEmpty()) {

				exportDataModelID = outputDataModelID;
			} else {

				exportDataModelID = dataModelID;
			}

			executeExport(exportDataModelID, engineThreads, serviceName);
		}
		final String tasksExecutedMessage = String
				.format("[%s] d:swarm tasks executed. (Processing time: %d s)", serviceName, (
						(System.currentTimeMillis() - global) / 1000));
		logger.info(tasksExecutedMessage);
		System.out.println(tasksExecutedMessage);
	}

	private static String executeInit(final String serviceName, final Integer engineThreads) throws Exception {

		// create job
		final int cnt = 0;
		Callable<String> initTask = new Init(config, logger, cnt);

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>());

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
				System.out.println(message1);

				return initResult;
			}

			pool.shutdown();

		} catch (final InterruptedException | ExecutionException e) {

			logger.error("something went wrong", e);
			e.printStackTrace();

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
				new LinkedBlockingQueue<Runnable>());

		try {

			final List<Future<String>> futureList = pool.invokeAll(filesToPush);

			for (final Future<String> f : futureList) {

				final String message = f.get();

				final String message1 = String.format("[%s] %s", serviceName, message);

				logger.info(message1);
				System.out.println(message1);
			}

			pool.shutdown();

		} catch (final InterruptedException | ExecutionException e) {

			logger.error("something went wrong", e);
			e.printStackTrace();

		}
	}

	private static void executeTransform(final String initDataModelID, final String outputDataModelID, final Integer engineThreads,
			final String serviceName) throws Exception {

		// create job list
		final LinkedList<Callable<String>> transforms = new LinkedList<>();
		transforms.add(new Transform(config, initDataModelID, outputDataModelID, logger));

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>());

		try {

			final List<Future<String>> futureList = pool.invokeAll(transforms);

			for (final Future<String> f : futureList) {

				final String message = f.get();

				final String message1 = String.format("[%s] %s", serviceName, message);

				logger.info(message1);
				System.out.println(message1);
			}

			pool.shutdown();

		} catch (final InterruptedException | ExecutionException e) {

			logger.error("something went wrong", e);
			e.printStackTrace();

		}
	}

	private static void executeExport(final String exportDataModelID, final Integer engineThreads, final String serviceName) throws Exception {

		// create job list
		final LinkedList<Callable<String>> exports = new LinkedList<>();
		exports.add(new Export(exportDataModelID, config, logger));

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>());

		try {

			final List<Future<String>> futureList = pool.invokeAll(exports);

			for (final Future<String> f : futureList) {

				final String message = f.get();

				final String message1 = String.format("[%s] %s", serviceName, message);

				logger.info(message1);
				System.out.println(message1);
			}

			pool.shutdown();

		} catch (final InterruptedException | ExecutionException e) {

			logger.error("something went wrong", e);
			e.printStackTrace();

		}
	}
}

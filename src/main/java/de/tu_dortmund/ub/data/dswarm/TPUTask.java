package de.tu_dortmund.ub.data.dswarm;

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

import javax.json.JsonObject;

import de.tu_dortmund.ub.data.util.TPUUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class TPUTask implements Callable<String> {

	private static final Logger LOG = LoggerFactory.getLogger(TPUTask.class);

	private final Properties config;
	private final String     watchFolderFile;
	private final String     resourceWatchFolder;
	private final String     serviceName;
	private final int        cnt;

	public TPUTask(final Properties config, final String watchFolderFile, final String resourceWatchFolder, final String serviceName, final int cnt) {

		this.config = config;
		this.watchFolderFile = watchFolderFile;
		this.resourceWatchFolder = resourceWatchFolder;
		this.serviceName = serviceName;
		this.cnt = cnt;
	}

	@Override public String call() throws Exception {

		try {

			final Integer engineThreads = 1;
			final JsonObject initResultJSON = TPUUtil.doInit(resourceWatchFolder, watchFolderFile, serviceName, engineThreads, config, cnt);

			final String inputDataModelID = initResultJSON.getString(Init.DATA_MODEL_ID);

			// input data model = output data model, i.e., for each data model a separate export file will be created
			executeTransformation(inputDataModelID, inputDataModelID, engineThreads, config, serviceName, cnt);

			return String.format("[%s][%d] TPU task execution '%d' succeeded for source file '%s' and data model '%s'", serviceName, cnt, cnt, watchFolderFile,
					inputDataModelID);
		} catch (final Exception e) {

			final String message = String.format("[%s][%d] TPU task execution '%d' failed for source file '%s'", serviceName, cnt, cnt, watchFolderFile);

			LOG.error(message, e);

			return message;
		}
	}

	private static void executeTransformation(final String inputDataModelID, final String outputDataModelID, final Integer engineThreads,
			final Properties config,
			final String serviceName, final int cnt) {

		// create job
		final Optional<Boolean> optionalDoExportOnTheFly = Optional.of(Boolean.FALSE);
		final Optional<Boolean> optionalDoIngestOnTheFly = Optional.of(Boolean.TRUE);
		final Callable<String> transformTask = new Transform(config, inputDataModelID, outputDataModelID, optionalDoIngestOnTheFly,
				optionalDoExportOnTheFly, cnt);

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		try {

			final List<Callable<String>> tasks = new LinkedList<>();
			tasks.add(transformTask);

			final List<Future<String>> futureList = pool.invokeAll(tasks);
			final Iterator<Future<String>> iterator = futureList.iterator();

			if (iterator.hasNext()) {

				final Future<String> f = iterator.next();

				final String message = f.get();

				final String message1 = String.format("[%s][%d] %s", serviceName, cnt, message);

				LOG.info(message1);
			}

		} catch (final InterruptedException | ExecutionException e) {

			LOG.error("[{}][{}] something went wrong", serviceName, cnt, e);

		} finally {

			pool.shutdown();
		}
	}
}

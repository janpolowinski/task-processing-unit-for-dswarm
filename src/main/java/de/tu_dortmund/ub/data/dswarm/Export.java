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

import java.util.Properties;
import java.util.concurrent.Callable;

import de.tu_dortmund.ub.data.util.TPUUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
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
public class Export implements Callable<String> {

	public static final String EXPORT_IDENTIFIER        = "export";
	private final String     exportDataModelID;
	private final Properties config;
	private final Logger     logger;

	public Export(final String exportDataModelIDArg, final Properties config, final Logger logger) {

		exportDataModelID = exportDataModelIDArg;
		this.config = config;
		this.logger = logger;
	}

	//    @Override
	public String call() {

		// init logger
		PropertyConfigurator.configure(config.getProperty(TPUStatics.SERVICE_LOG4J_CONF_IDENTIFIER));

		final String serviceName = config.getProperty(TPUStatics.SERVICE_NAME_IDENTIFIER);

		logger.info(String.format("[%s] Starting 'XML-Export (Task)' ...", serviceName));

		// init process values
		String message = null;

		try {

			// export and save to results folder
			exportDataModel(exportDataModelID, serviceName);


		} catch (final Exception e) {

			logger.error(String.format("[%s] Exporting and saving datamodel '%s' failed with a %s", serviceName, exportDataModelID, e.getClass()
					.getSimpleName()), e);
		}

		return message;
	}

	/**
	 * export the data model with the given ID as XML
	 *
	 * @param dataModelID
	 * @return xml input stream
	 * @throws Exception
	 */
	private void exportDataModel(final String dataModelID, final String serviceName) throws Exception {

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final String engineDswarmAPI = config.getProperty(TPUStatics.ENGINE_DSWARM_API_IDENTIFIER);

			final String uri =
					engineDswarmAPI + DswarmBackendStatics.DATAMODELS_ENDPOINT + APIStatics.SLASH + dataModelID + APIStatics.SLASH + EXPORT_IDENTIFIER
							+ APIStatics.QUESTION_MARK + DswarmBackendStatics.FORMAT_IDENTIFIER + APIStatics.EQUALS + APIStatics.APPLICATION_XML_MIMETYPE;
			final HttpGet httpGet = new HttpGet(uri);

			//			httpGet.setHeader(name, value);

			logger.info(String.format("[%s] dataModelID : %s", serviceName, dataModelID));
			logger.info(String.format("[%s] request : %s", serviceName, httpGet.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpGet)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();

				switch (statusCode) {

					case 200: {

						logger.info(String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine().getReasonPhrase()));

						break;
					}
					default: {

						logger.error(String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine().getReasonPhrase()));
					}
				}

				TPUUtil.writeResultToFile(httpResponse, config, exportDataModelID);
			}
		}
	}
}

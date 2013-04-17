package com.bitmixr;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

abstract public class DefaultExceptionHandler {
	final static Logger logger = LoggerFactory.getLogger(DefaultExceptionHandler.class);

	private static JSONException statusAndLog(final Exception anException, final HttpServletResponse aResponse, final int aStatus) {
		aResponse.setStatus(aStatus);
		if (logger.isDebugEnabled()) {
			logger.debug(ExceptionUtils.getStackTrace(anException));
		}
		return new JSONException(anException);
	}

	@ExceptionHandler(Exception.class)
	public @ResponseBody
	JSONException unknownError(final Exception anException, final HttpServletResponse aResponse) {
		return statusAndLog(anException, aResponse, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	}

	@ExceptionHandler(NotFoundException.class)
	JSONException notFoundException(final NotFoundException anException, final HttpServletResponse aResponse) {
		return statusAndLog(anException, aResponse, HttpServletResponse.SC_NOT_FOUND);
	}
}

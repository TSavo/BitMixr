package com.bitmixr;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JSONException implements Serializable{
	Logger logger = LoggerFactory.getLogger(JSONException.class);
	private static final long serialVersionUID=210821032222L;
	String message;
	JSONException cause;
	List<StackTraceElement> stackTraceElement;
	
	public JSONException(final Throwable aThrowable){
		this.message=aThrowable.getMessage();
		if(null!=aThrowable.getCause()){
			this.cause=new JSONException(aThrowable.getCause());
		}
		this.stackTraceElement=Arrays.asList(aThrowable.getStackTrace());
		logger.error(ExceptionUtils.getStackTrace(aThrowable));
	}

	public List<StackTraceElement> getStackTraceElement() {
		return stackTraceElement;
	}

	public void setStackTraceElement(List<StackTraceElement> stackTraceElement) {
		this.stackTraceElement = stackTraceElement;
	}

	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public JSONException getCause() {
		return cause;
	}
	public void setCause(JSONException cause) {
		this.cause = cause;
	}
}

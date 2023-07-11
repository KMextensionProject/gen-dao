package com.gratex.gendao.db;

public class InconvertableTypeException extends RuntimeException{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5575208642077978131L;

	public InconvertableTypeException(Object value, Class<?> type) {
		super( value +  " cannot be converted to " + type);
	}

}

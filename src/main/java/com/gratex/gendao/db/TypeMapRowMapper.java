package com.gratex.gendao.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TypeMapRowMapper {

	public TypeMap mapRow(ResultSet rs) throws SQLException {
		ResultSetMetaData rsMeta = rs.getMetaData();
		int columnCount = rsMeta.getColumnCount();
		TypeMap mapOfColumnValues = new TypeMap();
		for (int i = 1; i <= columnCount; i++) {
			String name = removeDatabaseColumnPrefix(rsMeta.getColumnName(i));
			Object value = rs.getObject(i);
			mapOfColumnValues.put(name, resolveDateTimeValueType(value));
		}
		return mapOfColumnValues;
	}

	public static String removeDatabaseColumnPrefix(String columnName) {
		int colNameStartIndex = columnName.indexOf('_') + 1;
		if (colNameStartIndex < 0) {
			return columnName;
		}
		return columnName.substring(colNameStartIndex);
	}

	private Object resolveDateTimeValueType(Object value) {
		if (value instanceof java.sql.Timestamp) {
			return ((java.sql.Timestamp) value).toLocalDateTime();
		} else if (value instanceof java.sql.Date) {
			return ((java.sql.Date) value).toLocalDate();
		} else {
			return value;
		}
	}
}

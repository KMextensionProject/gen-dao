package com.gratex.gendao.db;

public enum QueryOperator {
	EQUALS(" = "),
	LESS_THAN(" < "),
	LESS_THAN_EQUAL(" <= "),
	GREATER_THAN(" > "),
	GREATER_THAN_EQUAL(" >= "),
	NOT_EQUAL(" <> "),
	LIKE(" LIKE '%"),
	NOT_LIKE(" NOT LIKE '%"),
	IS_NULL(" IS NULL"),
	IS_NOT_NULL(" IS NOT NULL");
	// TODO: add between?

	private String operatorValue;

	private QueryOperator(String operatorValue) {
		this.operatorValue = operatorValue;
	}

	public String getOperatorValue() {
		return this.operatorValue;
	}

	public String applyOperationOnValue(Object value) {
		switch(name()) {
		case "LIKE":
		case "NOT LIKE":
			return this.operatorValue + value + "%'";
		case "IS_NULL":
		case "IS_NOT_NULL":
			return this.operatorValue;
		default:
			if (value instanceof CharSequence) {
				value = "'" + value + "'";
			}
			return this.operatorValue + value;
		}
	}
}

package com.gratex.gendao.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// TODO: add documentation with hints
public final class Select {

	private static final Logger LOG = Logger.getAnonymousLogger();

	private StringBuilder selectBuilder;
	private StringBuilder whereBuilder;
	private boolean isWhereClauseApplied;
	private boolean distinctAlreadyApplied;
	private int requestedColumnsCount;
	private int limit;
	private Set<OrderBy> orderByColumns;
	private TypeMapRowMapper typeMapRowMapper;
	private String mainTableAlias;
	private Connection connection;

	Select(Connection connection, String... columns) {
		this.selectBuilder = new StringBuilder("SELECT ");
		this.whereBuilder = new StringBuilder();
		this.requestedColumnsCount = columns.length;
		this.orderByColumns = new LinkedHashSet<>();
		this.typeMapRowMapper = new TypeMapRowMapper();
		this.connection = connection;
		this.mainTableAlias = "";
		select(columns);
	}

	// TODO: test this
	public Select(Select otherSelect) {
		this.selectBuilder = new StringBuilder(otherSelect.selectBuilder);
		this.whereBuilder = new StringBuilder(otherSelect.whereBuilder);
		this.isWhereClauseApplied = otherSelect.isWhereClauseApplied;
		this.requestedColumnsCount = otherSelect.requestedColumnsCount;
		this.orderByColumns = otherSelect.orderByColumns;
		this.mainTableAlias = otherSelect.mainTableAlias;
	}

	private Select select(String... columns) {
		if (columns.length != 0) {
			selectBuilder.append(String.join(", ", columns));

			int aliasEndIndex = columns[0].indexOf('.');
			mainTableAlias = aliasEndIndex > 0 ? columns[0].substring(0, aliasEndIndex) : mainTableAlias;
		} else {
			selectBuilder.append("*");
		}
		return this;
	}

	public Select distinct() {
		if (distinctAlreadyApplied) {
			return this;
		}
		distinctAlreadyApplied = true;
		selectBuilder.insert(7, "DISTINCT ");
		return this;
	}

	public Select from(String tableName) {
		selectBuilder.append(" FROM ")
					 .append(tableName);
		return this;
	}
	
	public Select from(String tableSchema, String tableName) {
		selectBuilder.append(" FROM ")
					 .append(tableSchema)
					 .append(".")
		 			 .append(tableName);
		return this;
	}

	/**
	 * Will automatically append the input select statement in its final state to
	 * FROM clause.<br>
	 * This method makes its own copy of the inner select and so no further changes
	 * will be applied to the select statement provided as input.
	 */
	public Select from(Select select) {
		Select copySelect = new Select(select);
		copySelect.appendRemainingParts();

		selectBuilder.append(" FROM (")
					 .append(copySelect)
					 .append(") AS SUBQUERY_RESULT");
		return this;
	}

	public Select where(String column, Object value) {
		if (value instanceof QueryOperator) {
			QueryOperator operator = (QueryOperator) value;
			if (QueryOperator.IS_NOT_NULL.equals(operator) 
				|| QueryOperator.IS_NULL.equals(operator)) {
				return where(column, operator, null);
			} else {
				throw new IllegalArgumentException("Cannot use " + operator + " without right-hand side value");
			}
		}
		return where(column, QueryOperator.EQUALS, value);
	}

	/*
	 * TODO: sanitize the inner select = must contain one return column
	 * must return one column even if there is "WHERE IN"
	 * 
	 * ... or let the db propagate the error?
	 * ... I think we should not call db if we can find out there is 
	 * something wrong with the statement that we can check
	 * 
	 * select * from ref.t_formular where n_ziadost_id = (one column + where)
	 * select * from ref.t_formular where n_ziadost_id in (one column)
	 */
	public Select where(String column, Select innerSelect) {
		appendWhereClauseConnector();
		whereBuilder.append(column)
					.append(" = (")
					.append(innerSelect.selectBuilder)
					.append(")");
		return this;
	}

	public Select where(String customWhere) {
		appendWhereClauseConnector();
		if (customWhere.contains("WHERE ")) {
			customWhere = customWhere.replace("WHERE ", "");
		}
		whereBuilder.append(customWhere);
		return this;
	}

	public Select where(String column, QueryOperator queryOperator, Object value) {
		appendWhereClauseConnector();
		whereBuilder.append(column)
					.append(queryOperator.applyOperationOnValue(value));
		return this;
	}

	public <V> Select whereIn(String column, List<V> values) {
		return whereIn(column, values, false);
	}

	public <V> Select whereIn(String column, List<V> values, boolean negate) {
		if (values.isEmpty()) {
			// TODO: log that the method is not going to do anything on empty values
			return this;
		}
		if (values.size() < 2) {
			// TODO: log that the method is using basic equality where (or do something else?)
			return where(column, values.get(0));
		}

		appendWhereClauseConnector();
		whereBuilder.append(column)
					.append(negate ? " NOT" : "")
					.append(" IN (");

		Stream<String> valuesJoiningStream = values.stream().map(String::valueOf);
		if (values.get(0) instanceof CharSequence) {
			valuesJoiningStream = valuesJoiningStream
					.map(val -> val.replace('\n', ' '))
					.map(val -> val.replace("'", "\'"))
					.map(val -> val.replace('"', '\''))
					.map(val -> "\'".concat(val).concat("\'")); // is this right to use double quotes?
		}
		whereBuilder.append(valuesJoiningStream.collect(Collectors.joining(",")))
					.append(")");
		return this;
	}

	// TODO: implement
	public Select whereBetween(String column, Object leftValue, Object rightValue) {
		return this;
	}

	private void appendWhereClauseConnector() {
		if (isWhereClauseApplied) {
			whereBuilder.append(" AND ");
		} else {
			whereBuilder.append(" WHERE ");
			isWhereClauseApplied = true;
		}
	}

	public Join join(String table) {
		return new Join(table);
	}

	public Join leftJoin(String table) {
		return new Join(table, Join.LEFT);
	}

	public Join leftOuterJoin(String table) {
		return new Join(table, Join.LEFT_OUTER);
	}

	public Join rightJoin(String table) {
		return new Join(table, Join.RIGHT);
	}

	public Join rightOuterJoin(String table) {
		return new Join(table, Join.RIGHT_OUTER);
	}

	public class Join {

		static final String INNER_JOIN = " JOIN ";
		static final String INNER = " INNER";
		static final String LEFT = " LEFT";
		static final String RIGHT = " RIGHT";
		static final String LEFT_OUTER = " LEFT OUTER";
		static final String RIGHT_OUTER = " RIGHT OUTER";

		public Join(String table) {
			this(table, "");
		}

		public Join(String table, String joinType) {
			selectBuilder.append(joinType)
						 .append(INNER_JOIN)
						 .append(table);
		}

		public Select on(String leftColumn, String rightColumn) {
			selectBuilder.append(" ON ")
						 .append(leftColumn)
						 .append(" = ")
						 .append(rightColumn);
			return Select.this;
		}

		public Select on(String leftColumn, String rightColumn, String... moreColumns) {
			on(leftColumn, rightColumn);

			int moreColumnsLength = moreColumns.length;
			if (moreColumnsLength != 0 && moreColumnsLength % 2 == 0) {
				for (int i = 0, j = 1; j < moreColumnsLength; i++, j++) {
					selectBuilder.append(" AND ")
								 .append(moreColumns[i])
								 .append(" = ")
								 .append(moreColumns[j]);
				}
			}
			// add some exception if moreColumns is not empty?
			return Select.this;
		}
	}

	public Select limit(int howMany) {
		this.limit = howMany;
		return this;
	}

	public class OrderBy {

		private String column;
		private OrderByDirection direction;

		public OrderBy(String column, OrderByDirection direction) {
			this.column = column;
			this.direction = direction;
		}

		public String getColumn() {
			return this.column;
		}

		public OrderByDirection getDirection() {
			return this.direction;
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(column);
		}

		@Override
		public boolean equals(Object other) {
			if (other instanceof OrderBy) {
				return this.column.equals(((OrderBy)other).getColumn());
			}
			return false;
		}
	}

	public enum OrderByDirection {
		ASC("ASC"),
		DESC("DESC");

		private String direction;

		private OrderByDirection(String direction) {
			this.direction = direction;
		}

		public String getValue() {
			return this.direction;
		}
	}

	public Select orderBy(String column) {
		return orderBy(column, OrderByDirection.ASC);
	}

	public Select orderBy(String column, OrderByDirection direction) {
		this.orderByColumns.add(new OrderBy(column, direction));
		return this;
	}

	/**
	 * Applies the lock for queried rows
	 */
//	public Select lockForUpdate() {
//		this.lockForUpdate = true;
//		return this;
//	}

	// TODO: add wherePreparedStatement() - dynamic values
	// TODO: if it returns nothing, let it do it without throwing an exception but return empty map instead
	public TypeMap asMap() {
		appendRemainingParts();
//		logSelect();
		try (ResultSet rs = connection.createStatement().executeQuery(selectBuilder.toString())) {
			if (rs.next()) { 
				return typeMapRowMapper.mapRow(rs);
			}
		} catch (SQLException e) {
			throw new RuntimeException("Unable to execute statement: {" + selectBuilder + "}, Cause: " + e.getMessage());
		}
		return new TypeMap();
	}

	// TODO: if it if nothing returns, let it do it without throwing an exception but an empty list
	public List<TypeMap> asList() {
		appendRemainingParts();
//		logSelect();

		try (ResultSet rs = connection.createStatement().executeQuery(selectBuilder.toString())) {
			List<TypeMap> resultList = new ArrayList<>();
			while (rs.next()) {
				resultList.add(typeMapRowMapper.mapRow(rs));
			}
			return resultList;
		} catch (SQLException e) {
			throw new RuntimeException("Unable to execute statement: {" + selectBuilder + "}, Cause: " + e.getMessage());
		}
	}

	public Boolean asBoolean() {
		appendRemainingParts();
//		logSelect();
		return queryForObject(Boolean.class);
	}

	public Long asLong() {
		appendRemainingParts();
//		logSelect();
		return queryForObject(Long.class);
	}

	public String asString() {
		appendRemainingParts();
//		logSelect();
		return queryForObject(String.class);
	}

	private void appendRemainingParts() {
		appendWhereStatements();

		if (!orderByColumns.isEmpty()) {
			appendOrderBy();
		}

		if (limit > 0) {
			appendLimit();
		}
	}

	private void appendWhereStatements() {
		selectBuilder.append(whereBuilder);
	}

	private void appendOrderBy() {
		selectBuilder.append(" ORDER BY ");

		String columns = orderByColumns.stream()
			.map(e -> e.getColumn() + " " + e.getDirection().getValue())
			.collect(Collectors.joining(", "));

		selectBuilder.append(columns);
	}

	private void appendLimit() {
		selectBuilder.append(" FETCH FIRST ")
					 .append(limit)
					 .append(" ROWS ONLY");
	}

	@SuppressWarnings("unchecked")
	private <T> T queryForObject(Class<T> classObj) {
		// 0 means all and above 1 is also impossible to parse as one object
		if (requestedColumnsCount != 1) {
			throw new IllegalArgumentException("Exactly one column must appear in the"
				+ " select clause to directly ask for its value in this manner.");
		}
		return (T) asMap().entrySet().iterator().next().getValue();
	}

	private void logSelect() {
		LOG.info(() -> selectBuilder.toString());
	}

	@Override
	public String toString() {
		return this.selectBuilder.toString();
	}
}
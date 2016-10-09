/*
 * ao-dao-dbc - Simple data access objects framework implementation leveraging ao-dbc.
 * Copyright (C) 2011, 2013, 2015, 2016  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-dao-dbc.
 *
 * ao-dao-dbc is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-dao-dbc is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-dao-dbc.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.dao.dbc;

import com.aoindustries.dao.Report;
import com.aoindustries.dbc.Database;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.sql.WrappedSQLException;
import com.aoindustries.util.AoCollections;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A report that is obtained from a SQL query database.
 */
public abstract class QueryReport
	implements Report
{

	public static class QueryColumn implements Report.Column {

		private final QueryReport report;
		private final String name;
		private final Report.Alignment alignment;

		QueryColumn(QueryReport report, String name, Report.Alignment alignment) {
			this.report = report;
			this.name = name;
			this.alignment = alignment;
		}

		@Override
		public String getName() {
			return name;
		}

		/**
		 * Gets the label from the report.
		 *
		 * @see Report#getColumnLabel
		 */
		@Override
		public String getLabel() {
			return report.getColumnLabel(this);
		}

		@Override
		public Report.Alignment getAlignment() {
			return alignment;
	   }
	}

	public static class ReportResult implements Report.Result {

		private final List<QueryColumn> columns;
		private final List<List<Object>> tableData;

		ReportResult(List<QueryColumn> columns, List<List<Object>> tableData) {
			this.columns = columns;
			this.tableData = tableData;
		}

		@Override
		public List<QueryColumn> getColumns() {
			return columns;
		}

		@Override
		public List<List<Object>> getTableData() {
			return tableData;
		}
	}

	private final Database database;
	private final String name;
	private final String sql;
	private final Object[] params;

	/**
	 * @param params to substitute a parameter, provide the Parameter object.
	 */
	public QueryReport(Database database, String name, String sql, Object... params) {
		this.database = database;
		this.name = name;
		this.sql = sql;
		this.params = params;
	}

	/**
	 * @param params to substitute a parameter, provide the Parameter object.
	 */
	public QueryReport(Database database, String name, String sql, Collection<?> params) {
		this(database, name, sql, params.toArray());
	}

	@Override
	public String getName() {
		return name;
	}

	/**
	 * Defaults to calling getTitle().
	 */
	@Override
	public String getTitle(Map<String, ? extends Object> parameterValues) {
		return getTitle();
	}

	/**
	 * Defaults to calling getDescription().
	 */
	@Override
	public String getDescription(Map<String, ? extends Object> parameterValues) {
		return getDescription();
	}

	/**
	 * Gets the label for the provided column.  Defaults to column name.
	 *
	 * @see QueryColumn#getLabel()
	 */
	public String getColumnLabel(QueryColumn column) {
		return column.name;
	}

	/**
	 * Get default, no parameters are required.
	 */
	@Override
	public Iterable<? extends Parameter> getParameters() {
		return Collections.emptyList();
	}

	/**
	 * Checks if this is a read-only report.  No temp tables or views may be created on
	 * a read-only report.  Defaults to true.
	 */
	public boolean isReadOnly() {
		return true;
	}

	@Override
	public ReportResult executeReport(Map<String,? extends Object> parameterValues) throws SQLException {
		Connection conn = database.getConnection(Connection.TRANSACTION_READ_COMMITTED, isReadOnly(), 1);
		try {
			try {
				beforeQuery(parameterValues, conn);
				try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
					try {
						/**
						 * Substitute any parameters with the values provided.
						 */
						Object[] sqlParams = new Object[params.length];
						for(int i=0; i<params.length; i++) {
							Object param = params[i];
							if(param instanceof Parameter) {
								// Replace placeholder with value
								Parameter reportParam = (Parameter)param;
								String paramName = reportParam.getName();
								param = parameterValues.get(paramName);
								if(param==null) throw new IllegalArgumentException("Parameter required: " + paramName);
							}
							sqlParams[i] = param;
						}

						DatabaseConnection.setParams(conn, pstmt, sqlParams);
						try (ResultSet results = pstmt.executeQuery()) {
							ResultSetMetaData meta = results.getMetaData();
							int numColumns = meta.getColumnCount();
							List<QueryColumn> columns = new ArrayList<>();
							for(int columnIndex=1; columnIndex<=numColumns; columnIndex++) {
								final Alignment alignment;
								switch(meta.getColumnType(columnIndex)) {
									case Types.BIGINT :
									case Types.DECIMAL :
									case Types.DOUBLE :
									case Types.FLOAT :
									case Types.INTEGER :
									case Types.NUMERIC :
									case Types.REAL :
									case Types.SMALLINT :
									case Types.TINYINT :
										alignment = Alignment.right;
										break;
									case Types.BOOLEAN :
									case Types.BIT :
										alignment = Alignment.center;
										break;
									default :
										alignment = Alignment.left;
								}
								columns.add(new QueryColumn(this, meta.getColumnName(columnIndex), alignment));
							}
							List<List<Object>> tableData = new ArrayList<>();
							while(results.next()) {
								List<Object> row = new ArrayList<>(numColumns);
								for(int columnIndex=1; columnIndex<=numColumns; columnIndex++) {
									// Convert arrays to lists
									Object value = results.getObject(columnIndex);
									if(value instanceof Array) {
										List<Object> values = new ArrayList<>();
										try (ResultSet arrayResults = ((Array)value).getResultSet()) {
											while(arrayResults.next()) values.add(arrayResults.getObject(2));
										}
										value = AoCollections.optimalUnmodifiableList(values);
									}
									row.add(value);
								}
								tableData.add(Collections.unmodifiableList(row));
							}
							return new ReportResult(
								Collections.unmodifiableList(columns),
								Collections.unmodifiableList(tableData)
							);
						}
					} catch(SQLException e) {
						throw new WrappedSQLException(e, pstmt);
					}
				}
			} finally {
				afterQuery(parameterValues, conn);
			}
		} finally {
			database.releaseConnection(conn);
		}
	}

	/**
	 * Called before the query is executed, this may setup any temp tables or views that are required
	 * by the main query.
	 *
	 * This default implementation does nothing.
	 */
	public void beforeQuery(Map<String,? extends Object> parameterValues, Connection conn) throws SQLException {
		// Do nothing
	}

	/**
	 * Called in try/finally after the query is executed, this may release any temp tables or views that were setup
	 * by beforeQuery.  This will be called even when the beforeQuery does not complete fully.
	 *
	 * This default implementation does nothing.
	 */
	public void afterQuery(Map<String,? extends Object> parameterValues, Connection conn) throws SQLException {
		// Do nothing
	}
}

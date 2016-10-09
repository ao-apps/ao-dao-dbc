/*
 * ao-dao - Simple data access objects framework.
 * Copyright (C) 2011, 2013, 2015, 2016  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-dao.
 *
 * ao-dao is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-dao is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-dao.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.dao.dbc;

import com.aoindustries.dao.impl.AbstractModel;
import com.aoindustries.dbc.Database;
import com.aoindustries.dbc.DatabaseCallable;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.dbc.DatabaseRunnable;
import java.sql.SQLException;

/**
 * A base implementation of <code>DaoDatabase</code>.
 */
abstract public class DatabaseModel
	extends AbstractModel
{

	/**
	 * Gets the underlying database that should be used at this moment in time.
	 * It is possible that the database will change in an fail-over state.
	 * Within a single transaction, however, the database returned must be the
	 * same.
	 */
	abstract protected Database getDatabase() throws SQLException;

	/**
	 * Uses a ThreadLocal to make sure an entire transaction is executed against the same
	 * underlying database.  This way, nothing funny will happen if master/slave databases
	 * are switched mid-transaction.
	 */
	protected final ThreadLocal<Database> transactionDatabase = new ThreadLocal<>();

	@SuppressWarnings("overloads")
	protected <V> V executeTransaction(DatabaseCallable<V> callable) throws SQLException {
		Database database = transactionDatabase.get();
		if(database!=null) {
			// Reuse current database
			return database.executeTransaction(callable);
		} else {
			// Get database
			database=getDatabase();
			transactionDatabase.set(database);
			try {
				return database.executeTransaction(callable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	@SuppressWarnings("overloads")
	protected void executeTransaction(DatabaseRunnable runnable) throws SQLException {
		Database database = transactionDatabase.get();
		if(database!=null) {
			// Reuse current database
			database.executeTransaction(runnable);
		} else {
			// Get database
			database=getDatabase();
			transactionDatabase.set(database);
			try {
				database.executeTransaction(runnable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	@Override
	public void executeTransaction(final Runnable runnable) throws SQLException {
		executeTransaction((DatabaseConnection db) -> {
			runnable.run();
		});
	}
}

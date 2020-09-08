/*
 * ao-dao-dbc - Simple data access objects framework implementation leveraging ao-dbc.
 * Copyright (C) 2011, 2013, 2015, 2016, 2020  AO Industries, Inc.
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

import com.aoindustries.dao.impl.AbstractModel;
import com.aoindustries.dbc.Database;
import com.aoindustries.dbc.DatabaseCallable;
import com.aoindustries.dbc.DatabaseCallableE;
import com.aoindustries.dbc.DatabaseRunnable;
import com.aoindustries.dbc.DatabaseRunnableE;
import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * A base implementation of <code>DaoDatabase</code>.
 */
abstract public class DatabaseModel
	extends AbstractModel
{

	/**
	 * Gets the underlying database that should be used at this moment in time.
	 * It is possible that the database will change in a fail-over state.
	 * Within a single transaction, however, the database returned must be the
	 * same.
	 */
	abstract public Database getDatabase() throws SQLException;

	/**
	 * Uses a {@link ThreadLocal} to make sure an entire transaction is executed against the same
	 * underlying database.  This way, nothing funny will happen if master/slave databases
	 * are switched mid-transaction.
	 */
	protected final ThreadLocal<Database> transactionDatabase = new ThreadLocal<>();

	@Override
	@SuppressWarnings("overloads")
	public void transaction(Runnable runnable) throws SQLException {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			database.transaction(runnable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				database.transaction(runnable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	@SuppressWarnings("overloads")
	public void transaction(DatabaseRunnable runnable) throws SQLException {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			database.transaction(runnable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				database.transaction(runnable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	@SuppressWarnings("overloads")
	public <E extends Exception> void transaction(Class<E> eClass, DatabaseRunnableE<E> runnable) throws SQLException, E {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			database.transaction(eClass, runnable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				database.transaction(eClass, runnable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	@Override
	@SuppressWarnings("overloads")
	public <V> V transaction(Callable<V> callable) throws SQLException {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			return database.transaction(callable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				return database.transaction(callable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	@SuppressWarnings("overloads")
	public <V> V transaction(DatabaseCallable<V> callable) throws SQLException {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			return database.transaction(callable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				return database.transaction(callable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	@SuppressWarnings("overloads")
	public <V,E extends Exception> V transaction(Class<E> eClass, DatabaseCallableE<V,E> callable) throws SQLException, E {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			return database.transaction(eClass, callable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				return database.transaction(eClass, callable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}
}

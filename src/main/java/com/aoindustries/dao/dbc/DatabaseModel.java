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
import com.aoindustries.lang.RunnableE;
import com.aoindustries.util.concurrent.CallableE;
import java.sql.SQLException;

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

	/**
	 * @see  Database#call(com.aoindustries.util.concurrent.CallableE)
	 */
	@Override
	public <V> V call(CallableE<? extends V,? extends SQLException> callable) throws SQLException {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			return database.call(callable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				return database.call(callable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	/**
	 * @see  Database#call(java.lang.Class, com.aoindustries.util.concurrent.CallableE)
	 */
	@Override
	public <V,E extends Throwable> V call(Class<? extends E> eClass, CallableE<? extends V,? extends E> callable) throws SQLException, E {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			return database.call(eClass, callable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				return database.call(eClass, callable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	/**
	 * @see  Database#call(com.aoindustries.dbc.DatabaseCallable)
	 */
	public <V> V call(DatabaseCallable<? extends V> callable) throws SQLException {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			return database.call(callable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				return database.call(callable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	/**
	 * @deprecated  Please use {@link #call(com.aoindustries.dbc.DatabaseCallable)}
	 */
	@Deprecated
	@SuppressWarnings("overloads")
	protected <V> V executeTransaction(DatabaseCallable<V> callable) throws SQLException {
		return call(callable);
	}

	/**
	 * @see  Database#call(java.lang.Class, com.aoindustries.dbc.DatabaseCallableE)
	 */
	public <V,E extends Exception> V call(Class<? extends E> eClass, DatabaseCallableE<? extends V,? extends E> callable) throws SQLException, E {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			return database.call(eClass, callable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				return database.call(eClass, callable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	/**
	 * @see  Database#run(com.aoindustries.lang.RunnableE)
	 */
	@Override
	public void run(RunnableE<? extends SQLException> runnable) throws SQLException {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			database.run(runnable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				database.run(runnable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	/**
	 * @see  Database#run(java.lang.Class, com.aoindustries.lang.RunnableE)
	 */
	@Override
	public <E extends Throwable> void run(Class<? extends E> eClass, RunnableE<? extends E> runnable) throws SQLException, E {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			database.run(eClass, runnable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				database.run(eClass, runnable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	/**
	 * @see  Database#run(com.aoindustries.dbc.DatabaseRunnable)
	 */
	public void run(DatabaseRunnable runnable) throws SQLException {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			database.run(runnable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				database.run(runnable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}

	/**
	 * @deprecated  Please use {@link #run(com.aoindustries.dbc.DatabaseRunnable)}
	 */
	@Deprecated
	@SuppressWarnings("overloads")
	protected void executeTransaction(DatabaseRunnable runnable) throws SQLException {
		run(runnable);
	}

	/**
	 * @see  Database#run(java.lang.Class, com.aoindustries.dbc.DatabaseRunnableE)
	 */
	public <E extends Exception> void run(Class<? extends E> eClass, DatabaseRunnableE<? extends E> runnable) throws SQLException, E {
		Database database = transactionDatabase.get();
		if(database != null) {
			// Reuse current database
			database.run(eClass, runnable);
		} else {
			// Get database
			database = getDatabase();
			try {
				transactionDatabase.set(database);
				database.run(eClass, runnable);
			} finally {
				transactionDatabase.remove();
			}
		}
	}
}

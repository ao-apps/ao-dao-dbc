/*
 * ao-dao-dbc - Simple data access objects framework implementation leveraging ao-dbc.
 * Copyright (C) 2011, 2013, 2015, 2016, 2020, 2021, 2022  AO Industries, Inc.
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
 * along with ao-dao-dbc.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoapps.dao.dbc;

import com.aoapps.dao.base.AbstractModel;
import com.aoapps.dbc.Database;
import com.aoapps.dbc.DatabaseCallable;
import com.aoapps.dbc.DatabaseCallableE;
import com.aoapps.dbc.DatabaseRunnable;
import com.aoapps.dbc.DatabaseRunnableE;
import com.aoapps.lang.RunnableE;
import com.aoapps.lang.concurrent.CallableE;
import java.sql.SQLException;

/**
 * A base implementation of <code>DaoDatabase</code>.
 */
public abstract class DatabaseModel
  extends AbstractModel
{

  /**
   * Gets the underlying database that should be used at this moment in time.
   * It is possible that the database will change in a fail-over state.
   * Within a single transaction, however, the database returned must be the
   * same.
   */
  public abstract Database getDatabase() throws SQLException;

  /**
   * Uses a {@link ThreadLocal} to make sure an entire transaction is executed against the same
   * underlying database.  This way, nothing funny will happen if master/slave databases
   * are switched mid-transaction.
   */
  protected final ThreadLocal<Database> transactionDatabase = new ThreadLocal<>();

  /**
   * @see  Database#transactionCall(com.aoapps.lang.concurrent.CallableE)
   */
  @Override
  public <V> V transactionCall(CallableE<? extends V, ? extends SQLException> callable) throws SQLException {
    Database database = transactionDatabase.get();
    if (database != null) {
      // Reuse current database
      return database.transactionCall(callable);
    } else {
      // Get database
      database = getDatabase();
      try {
        transactionDatabase.set(database);
        return database.transactionCall(callable);
      } finally {
        transactionDatabase.remove();
      }
    }
  }

  /**
   * @param  <Ex>  An arbitrary exception type that may be thrown
   *
   * @see  Database#transactionCall(java.lang.Class, com.aoapps.lang.concurrent.CallableE)
   */
  @Override
  public <V, Ex extends Throwable> V transactionCall(Class<? extends Ex> eClass, CallableE<? extends V, ? extends Ex> callable) throws SQLException, Ex {
    Database database = transactionDatabase.get();
    if (database != null) {
      // Reuse current database
      return database.transactionCall(eClass, callable);
    } else {
      // Get database
      database = getDatabase();
      try {
        transactionDatabase.set(database);
        return database.transactionCall(eClass, callable);
      } finally {
        transactionDatabase.remove();
      }
    }
  }

  /**
   * @see  Database#transactionCall(com.aoapps.dbc.DatabaseCallable)
   */
  public <V> V transactionCall(DatabaseCallable<? extends V> callable) throws SQLException {
    Database database = transactionDatabase.get();
    if (database != null) {
      // Reuse current database
      return database.transactionCall(callable);
    } else {
      // Get database
      database = getDatabase();
      try {
        transactionDatabase.set(database);
        return database.transactionCall(callable);
      } finally {
        transactionDatabase.remove();
      }
    }
  }

  /**
   * @deprecated  Please use {@link #transactionCall(com.aoapps.dbc.DatabaseCallable)}
   */
  @Deprecated(forRemoval = true)
  @SuppressWarnings("overloads")
  protected <V> V executeTransaction(DatabaseCallable<V> callable) throws SQLException {
    return DatabaseModel.this.transactionCall(callable);
  }

  /**
   * @param  <Ex>  An arbitrary exception type that may be thrown
   *
   * @see  Database#transactionCall(java.lang.Class, com.aoapps.dbc.DatabaseCallableE)
   */
  // TODO: Ex extends Throwable
  public <V, Ex extends Exception> V transactionCall(Class<? extends Ex> eClass, DatabaseCallableE<? extends V, ? extends Ex> callable) throws SQLException, Ex {
    Database database = transactionDatabase.get();
    if (database != null) {
      // Reuse current database
      return database.transactionCall(eClass, callable);
    } else {
      // Get database
      database = getDatabase();
      try {
        transactionDatabase.set(database);
        return database.transactionCall(eClass, callable);
      } finally {
        transactionDatabase.remove();
      }
    }
  }

  /**
   * @see  Database#transactionRun(com.aoapps.lang.RunnableE)
   */
  @Override
  public void transactionRun(RunnableE<? extends SQLException> runnable) throws SQLException {
    Database database = transactionDatabase.get();
    if (database != null) {
      // Reuse current database
      database.transactionRun(runnable);
    } else {
      // Get database
      database = getDatabase();
      try {
        transactionDatabase.set(database);
        database.transactionRun(runnable);
      } finally {
        transactionDatabase.remove();
      }
    }
  }

  /**
   * @param  <Ex>  An arbitrary exception type that may be thrown
   *
   * @see  Database#transactionRun(java.lang.Class, com.aoapps.lang.RunnableE)
   */
  @Override
  public <Ex extends Throwable> void transactionRun(Class<? extends Ex> eClass, RunnableE<? extends Ex> runnable) throws SQLException, Ex {
    Database database = transactionDatabase.get();
    if (database != null) {
      // Reuse current database
      database.transactionRun(eClass, runnable);
    } else {
      // Get database
      database = getDatabase();
      try {
        transactionDatabase.set(database);
        database.transactionRun(eClass, runnable);
      } finally {
        transactionDatabase.remove();
      }
    }
  }

  /**
   * @see  Database#transactionRun(com.aoapps.dbc.DatabaseRunnable)
   */
  public void transactionRun(DatabaseRunnable runnable) throws SQLException {
    Database database = transactionDatabase.get();
    if (database != null) {
      // Reuse current database
      database.transactionRun(runnable);
    } else {
      // Get database
      database = getDatabase();
      try {
        transactionDatabase.set(database);
        database.transactionRun(runnable);
      } finally {
        transactionDatabase.remove();
      }
    }
  }

  /**
   * @deprecated  Please use {@link #transactionRun(com.aoapps.dbc.DatabaseRunnable)}
   */
  @Deprecated(forRemoval = true)
  @SuppressWarnings("overloads")
  protected void executeTransaction(DatabaseRunnable runnable) throws SQLException {
    DatabaseModel.this.transactionRun(runnable);
  }

  /**
   * @param  <Ex>  An arbitrary exception type that may be thrown
   *
   * @see  Database#transactionRun(java.lang.Class, com.aoapps.dbc.DatabaseRunnableE)
   */
  // TODO: Ex extends Throwable
  public <Ex extends Exception> void transactionRun(Class<? extends Ex> eClass, DatabaseRunnableE<? extends Ex> runnable) throws SQLException, Ex {
    Database database = transactionDatabase.get();
    if (database != null) {
      // Reuse current database
      database.transactionRun(eClass, runnable);
    } else {
      // Get database
      database = getDatabase();
      try {
        transactionDatabase.set(database);
        database.transactionRun(eClass, runnable);
      } finally {
        transactionDatabase.remove();
      }
    }
  }
}

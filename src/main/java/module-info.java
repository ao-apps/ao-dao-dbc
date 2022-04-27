/*
 * ao-dao-dbc - Simple data access objects framework implementation leveraging ao-dbc.
 * Copyright (C) 2021, 2022  AO Industries, Inc.
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
module com.aoapps.dao.dbc {
  exports com.aoapps.dao.dbc;
  // Direct
  requires com.aoapps.collections; // <groupId>com.aoapps</groupId><artifactId>ao-collections</artifactId>
  requires com.aoapps.dao.api; // <groupId>com.aoapps</groupId><artifactId>ao-dao-api</artifactId>
  requires com.aoapps.dao.base; // <groupId>com.aoapps</groupId><artifactId>ao-dao-base</artifactId>
  requires com.aoapps.dbc; // <groupId>com.aoapps</groupId><artifactId>ao-dbc</artifactId>
  requires com.aoapps.lang; // <groupId>com.aoapps</groupId><artifactId>ao-lang</artifactId>
  // Java SE
  requires java.sql;
} // TODO: Avoiding rewrite-maven-plugin-4.22.2 truncation

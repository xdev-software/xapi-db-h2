/*
 * SqlEngine Database Adapter H2 - XAPI SqlEngine Database Adapter for H2
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package xdev.db.h2.jdbc;


import com.xdev.jadoth.sqlengine.dbms.standard.StandardRetrospectionAccessor;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineException;
import com.xdev.jadoth.sqlengine.internal.tables.SqlIndex;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


/**
 * The Class Hsql20RetrospectionAccessor.
 */
public class H2RetrospectionAccessor extends StandardRetrospectionAccessor<H2Dbms>
{
	
	/**
	 * Instantiates a new hsql20 retrospection accessor.
	 * 
	 * @param dbmsadaptor
	 *            the dbmsadaptor
	 */
	public H2RetrospectionAccessor(final H2Dbms dbmsadaptor)
	{
		super(dbmsadaptor);
	}
	
	
	/**
	 * @param table
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#createSelect_INFORMATION_SCHEMA_COLUMNS(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public String createSelect_INFORMATION_SCHEMA_COLUMNS(final SqlTableIdentity table)
	{
		throw new RuntimeException("Retrospection not implemented yet!");
	}
	
	
	/**
	 * @param table
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#createSelect_INFORMATION_SCHEMA_INDICES(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public String createSelect_INFORMATION_SCHEMA_INDICES(final SqlTableIdentity table)
	{
		throw new RuntimeException("Retrospection not implemented yet!");
	}
	
	
	/**
	 * @param table
	 * @return
	 * @throws SQLEngineException
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#loadIndices(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public SqlIndex[] loadIndices(final SqlTableIdentity table) throws SQLEngineException
	{
		throw new RuntimeException("Retrospection not implemented yet!");
	}
	
}

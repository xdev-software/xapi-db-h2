package xdev.db.h2.jdbc;

/*-
 * #%L
 * SqlEngine Database Adapter H2
 * %%
 * Copyright (C) 2003 - 2021 XDEV Software
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-3.0.html>.
 * #L%
 */


import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

import xdev.db.jdbc.JDBCConnection;


public class H2JDBCConnection extends JDBCConnection<H2JDBCDataSource, H2Dbms>
{
	public H2JDBCConnection(H2JDBCDataSource dataSource)
	{
		super(dataSource);
	}
	
	
	@Override
	public void createTable(String tableName, String primaryKey, Map<String, String> columnMap,
			boolean isAutoIncrement, Map<String, String> foreignKeys) throws Exception
	{
		
		if(!columnMap.containsKey(primaryKey))
		{
			columnMap.put(primaryKey,"INTEGER"); //$NON-NLS-1$
		}
		StringBuffer createStatement = null;
		
		if(isAutoIncrement)
		{
			createStatement = new StringBuffer(
					"CREATE TABLE IF NOT EXISTS \"" + tableName + "\"(\"" //$NON-NLS-1$ //$NON-NLS-2$
							+ primaryKey
							+ "\" " + columnMap.get(primaryKey) + " NOT NULL AUTO_INCREMENT,"); //$NON-NLS-1$ //$NON-NLS-2$
		}
		else
		{
			createStatement = new StringBuffer("CREATE TABLE IF NOT EXISTS " + tableName + "(" //$NON-NLS-1$ //$NON-NLS-2$
					+ primaryKey + " " + columnMap.get(primaryKey) + ","); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		for(String keySet : columnMap.keySet())
		{
			if(!keySet.equals(primaryKey))
			{
				createStatement.append("\"" + keySet + "\" " + columnMap.get(keySet) + ","); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
		
		createStatement.append(" PRIMARY KEY (\"" + primaryKey + "\"))"); //$NON-NLS-1$ //$NON-NLS-2$
		
		if(log.isDebugEnabled())
		{
			log.debug("SQL Statement to create a table: " + createStatement.toString()); //$NON-NLS-1$
		}
		
		Connection connection = super.getConnection();
		Statement statement = connection.createStatement();
		try
		{
			statement.execute(createStatement.toString());
		}
		catch(Exception e)
		{
			throw e;
		}
		finally
		{
			statement.close();
			connection.close();
		}
	}
	
}

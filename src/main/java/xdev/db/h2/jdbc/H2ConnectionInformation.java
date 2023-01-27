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

import xdev.db.ConnectionInformation;


public class H2ConnectionInformation extends ConnectionInformation<H2Dbms>
{
	private final boolean	embedded;
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	/**
	 * Instantiates a new h2 connection information.
	 * 
	 * @param user
	 *            the user
	 * @param password
	 *            the password
	 * @param database
	 *            the database
	 * @param urlExtension
	 *            the extended url properties
	 * @param dbmsAdaptor
	 *            the dbms adaptor
	 */
	public H2ConnectionInformation(final String host, final int port, final boolean embedded,
			final String user, final String password, final String database,
			final String urlExtension, final H2Dbms dbmsAdaptor)
	{
		super(host,port,user,password,database,urlExtension,dbmsAdaptor);
		
		this.embedded = embedded;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// getters //
	// ///////////////////
	/**
	 * Gets the database.
	 * 
	 * @return the database
	 */
	public String getDatabase()
	{
		return this.getCatalog();
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// setters //
	// ///////////////////
	/**
	 * Sets the database.
	 * 
	 * @param database
	 *            the database to set
	 */
	public void setDatabase(final String database)
	{
		this.setCatalog(database);
	}
	
	
	public boolean isEmbedded()
	{
		return this.embedded;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation#createJdbcConnectionUrl(java.lang.String)
	 */
	@Override
	public String createJdbcConnectionUrl()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("jdbc:h2:");
		
		if(this.embedded)
		{
			String database = getDatabase();
			if(!(database.startsWith("~") || database.startsWith("file:")))
			{
				String projectHome = System.getProperty("project.home",null);
				if(projectHome != null && projectHome.length() > 0)
				{
					if(!projectHome.endsWith("/"))
					{
						projectHome = projectHome + "/";
					}
					database = projectHome + database;
				}
			}
			
			sb.append(database);
		}
		else
		{
			sb.append("tcp://");
			sb.append(getHost());
			sb.append(":");
			sb.append(getPort());
			sb.append("/");
			sb.append(getCatalog());
			sb.append(";IFEXISTS=TRUE");
		}
		
		String url = sb.toString();
		return appendUrlExtension(url);
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation#getJdbcDriverClassName()
	 */
	@Override
	public String getJdbcDriverClassName()
	{
		return "org.h2.Driver";
	}
	
}

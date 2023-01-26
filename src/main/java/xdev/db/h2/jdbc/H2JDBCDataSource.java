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


import xdev.db.DBException;
import xdev.db.jdbc.JDBCDataSource;


public class H2JDBCDataSource extends JDBCDataSource<H2JDBCDataSource, H2Dbms>
{
	public H2JDBCDataSource()
	{
		super(new H2Dbms());
		getDbmsAdaptor().dataSource = this;
	}
	
	
	@Override
	public Parameter[] getDefaultParameters()
	{
		return new Parameter[]{EMBEDDED.clone(),HOST.clone(),PORT.clone(5435),USERNAME.clone("SA"),
				PASSWORD.clone(),CATALOG.clone(),URL_EXTENSION.clone(),
				IS_SERVER_DATASOURCE.clone(),SERVER_URL.clone(),AUTH_KEY.clone()};
	}
	
	
	@Override
	protected H2ConnectionInformation getConnectionInformation()
	{
		return new H2ConnectionInformation(getHost(),getPort(),isEmbedded(),getUserName(),
				getPassword().getPlainText(),getCatalog(),getUrlExtension(),getDbmsAdaptor());
	}
	
	
	@Override
	public H2JDBCConnection openConnectionImpl() throws DBException
	{
		return new H2JDBCConnection(this);
	}
	
	
	@Override
	public H2JDBCMetaData getMetaData() throws DBException
	{
		return new H2JDBCMetaData(this);
	}
	
	
	@Override
	public boolean canExport()
	{
		return true;
	}
}

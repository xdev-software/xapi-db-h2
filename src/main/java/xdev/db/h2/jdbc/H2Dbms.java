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


import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.internal.DatabaseGateway;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


public class H2Dbms
		extends
		DbmsAdaptor.Implementation<H2Dbms, H2DMLAssembler, H2DDLMapper, H2RetrospectionAccessor, H2Syntax>
{
	H2JDBCDataSource				dataSource;
	
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	
	/** The Constant MAX_VARCHAR_LENGTH. */
	protected static final int		MAX_VARCHAR_LENGTH		= Integer.MAX_VALUE;
	
	protected static final char		IDENTIFIER_DELIMITER	= '"';
	
	public static final H2Syntax	SYNTAX					= new H2Syntax();
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	/**
	 * Instantiates a new hsql20 dbms.
	 */
	public H2Dbms()
	{
		this(new SQLExceptionParser.Body());
	}
	
	
	/**
	 * Instantiates a new hsql20 dbms.
	 * 
	 * @param sqlExceptionParser
	 *            the sql exception parser
	 */
	public H2Dbms(final SQLExceptionParser sqlExceptionParser)
	{
		super(sqlExceptionParser,false);
		this.setRetrospectionAccessor(new H2RetrospectionAccessor(this));
		this.setDMLAssembler(new H2DMLAssembler(this));
		this.setSyntax(SYNTAX);
	}
	
	
	/**
	 * @param host
	 * @param port
	 * @param user
	 * @param password
	 * @param catalog
	 * @param properties
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#createConnectionInformation(java.lang.String,
	 *      int, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public H2ConnectionInformation createConnectionInformation(final String host, final int port,
			final String user, final String password, final String catalog, final String properties)
	{
		return new H2ConnectionInformation(
			host,
			port,
			this.dataSource != null ? this.dataSource.isEmbedded() : false,
		    user,
		    password,
		    catalog,
		    properties, 
		    this);
	}
	
	
	/**
	 * HSQL does not support any means of calculating table columns selectivity
	 * as far as it is known.
	 * 
	 * @param table
	 *            the table
	 * @return the object
	 */
	@Override
	public Object updateSelectivity(final SqlTableIdentity table)
	{
		return null;
	}
	
	
	/**
	 * @param bytes
	 * @param sb
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#assembleTransformBytes(byte[],
	 *      java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder assembleTransformBytes(final byte[] bytes, final StringBuilder sb)
	{
		return null;
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor.Implementation#getRetrospectionAccessor()
	 */
	@Override
	public H2RetrospectionAccessor getRetrospectionAccessor()
	{
		throw new RuntimeException("HSQL Retrospection not implemented yet!");
	}
	
	
	/**
	 * @param dbc
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#initialize(com.xdev.jadoth.sqlengine.internal.DatabaseGateway)
	 */
	@Override
	public void initialize(final DatabaseGateway<H2Dbms> dbc)
	{
		// No initialization needed
	}
	
	
	/**
	 * @param fullQualifiedTableName
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#rebuildAllIndices(java.lang.String)
	 */
	@Override
	public Object rebuildAllIndices(final String fullQualifiedTableName)
	{
		return null;
	}
	
	
	@Override
	public boolean supportsOFFSET_ROWS()
	{
		return true;
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#getMaxVARCHARlength()
	 */
	@Override
	public int getMaxVARCHARlength()
	{
		return MAX_VARCHAR_LENGTH;
	}
	
	
	@Override
	public char getIdentifierDelimiter()
	{
		return IDENTIFIER_DELIMITER;
	}
}

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


import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import xdev.db.ColumnMetaData;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.Index.IndexType;
import xdev.db.Result;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCDataSource;
import xdev.db.jdbc.JDBCMetaData;
import xdev.db.sql.Functions;
import xdev.db.sql.SELECT;
import xdev.db.sql.Table;


public class H2JDBCMetaData extends JDBCMetaData
{
	private static final long		serialVersionUID	= 2862594319338582561L;
	private static final DateFormat	dateFormat			= new SimpleDateFormat("yyyy-MM-dd");
	private static final DateFormat	timeFormat			= new SimpleDateFormat("HH:mm:ss");
	private static final DateFormat	timestampFormat		= new SimpleDateFormat(
																"yyyy-MM-dd HH:mm:ss");
	
	
	public H2JDBCMetaData(H2JDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	
	@Override
	protected String getSchema(JDBCDataSource dataSource)
	{
		return null;
	}
	
	
	@Override
	protected String getCatalog(JDBCDataSource dataSource)
	{
		return null;
	}
	
	
	@Override
	protected TableMetaData getTableMetaData(JDBCConnection jdbcConnection, DatabaseMetaData meta,
			int flags, TableInfo table) throws DBException, SQLException
	{
		String catalog = getCatalog(this.dataSource);
		String schema = getSchema(this.dataSource);
		
		String tableName = table.getName();
		Table tableIdentity = new Table(tableName,"META_DUMMY");
		
		Map<String, Object> defaultValues = new HashMap<>();
		ResultSet rs = meta.getColumns(catalog,schema,tableName,null);
		while(rs.next())
		{
			String columnName = rs.getString("COLUMN_NAME");
			Object defaultValue = rs.getObject("COLUMN_DEF");
			defaultValues.put(columnName,defaultValue);
		}
		rs.close();
		
		Map<String, ColumnMetaData> columnMap = new HashMap<>();
		
		SELECT select = new SELECT().FROM(tableIdentity).WHERE("1 = 0");
		Result result = jdbcConnection.query(select);
		int cc = result.getColumnCount();
		ColumnMetaData[] columns = new ColumnMetaData[cc];
		for(int i = 0; i < cc; i++)
		{
			ColumnMetaData column = result.getMetadata(i);
			
			Object defaultValue = defaultValues.get(column.getName());
			defaultValue = checkDefaultValue(defaultValue,column);
			
			columns[i] = new ColumnMetaData(tableName,column.getName(),column.getCaption(),
					column.getType(),column.getLength(),column.getScale(),null,column.isNullable(),
					column.isAutoIncrement());
			columnMap.put(columns[i].getName(),columns[i]);
		}
		result.close();
		
		StringBuilder sb = new StringBuilder();
		for(String columnName : defaultValues.keySet())
		{
			Object defaultValue = defaultValues.get(columnName);
			ColumnMetaData column = columnMap.get(columnName);
			if(column.isAutoIncrement() || String.valueOf(defaultValue).startsWith("?"))
			{
				continue;
			}
			
			if(sb.length() > 0)
			{
				sb.append(", ");
			}
			sb.append(defaultValue);
			sb.append(" AS \"");
			sb.append(columnName);
			sb.append("\"");
		}
		
		if(sb.length() > 0)
		{
			try
			{
				String defaultValueQuery = "SELECT " + sb.toString();
				result = jdbcConnection.query(defaultValueQuery);
				if(result.next())
				{
					cc = result.getColumnCount();
					for(int i = 0; i < cc; i++)
					{
						String columnName = result.getMetadata(i).getName();
						ColumnMetaData column = columnMap.get(columnName);
						if(column != null)
						{
							if(column.isAutoIncrement())
							{
								column.setDefaultValue(null);
							}
							else
							{
								Object defaultValue = result.getObject(i);
								defaultValue = checkDefaultValue(defaultValue,column);
								column.setDefaultValue(defaultValue);
							}
						}
					}
				}
				result.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		
		Map<IndexInfo, Set<String>> indexMap = new Hashtable<>();
		int count = UNKNOWN_ROW_COUNT;
		
		if(table.getType() == TableType.TABLE)
		{
			Set<String> primaryKeyColumns = new HashSet<>();
			rs = meta.getPrimaryKeys(catalog,schema,tableName);
			while(rs.next())
			{
				primaryKeyColumns.add(rs.getString("COLUMN_NAME"));
			}
			rs.close();
			
			if((flags & INDICES) != 0)
			{
				if(primaryKeyColumns.size() > 0)
				{
					indexMap.put(new IndexInfo("PRIMARY_KEY",IndexType.PRIMARY_KEY),
							primaryKeyColumns);
				}
				
				rs = meta.getIndexInfo(catalog,schema,tableName,false,true);
				while(rs.next())
				{
					String indexName = rs.getString("INDEX_NAME");
					String columnName = rs.getString("COLUMN_NAME");
					if(indexName != null && columnName != null
							&& !primaryKeyColumns.contains(columnName))
					{
						boolean unique = !rs.getBoolean("NON_UNIQUE");
						IndexInfo info = new IndexInfo(indexName,unique ? IndexType.UNIQUE
								: IndexType.NORMAL);
						Set<String> columnNames = indexMap.get(info);
						if(columnNames == null)
						{
							columnNames = new HashSet<>();
							indexMap.put(info,columnNames);
						}
						columnNames.add(columnName);
					}
				}
				rs.close();
			}
			
			if((flags & ROW_COUNT) != 0)
			{
				try
				{
					result = jdbcConnection.query(new SELECT().columns(Functions.COUNT()).FROM(
							tableIdentity));
					if(result.next())
					{
						count = result.getInt(0);
					}
					result.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
		Index[] indices = new Index[indexMap.size()];
		int i = 0;
		for(IndexInfo indexInfo : indexMap.keySet())
		{
			Set<String> columnList = indexMap.get(indexInfo);
			String[] indexColumns = columnList.toArray(new String[columnList.size()]);
			indices[i++] = new Index(indexInfo.name,indexInfo.type,indexColumns);
		}
		
		return new TableMetaData(table,columns,indices,count);
	}
	
	
	@SuppressWarnings("incomplete-switch")
	@Override
	public boolean equalsType(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		DataType clientType = clientColumn.getType();
		DataType dbType = dbColumn.getType();
		
		if(clientType == dbType)
		{
			switch(clientType)
			{
				case TINYINT:
				case SMALLINT:
				case INTEGER:
				case BIGINT:
				case REAL:
				case FLOAT:
				case DOUBLE:
				case DATE:
				case TIME:
				case TIMESTAMP:
				case BOOLEAN:
				case BINARY:
				case VARBINARY:
				case LONGVARCHAR:
				case LONGVARBINARY:
				case CLOB:
				case BLOB:
				{
					return true;
				}
				
				case NUMERIC:
				case DECIMAL:
				{
					return clientColumn.getLength() == dbColumn.getLength()
							&& clientColumn.getScale() == dbColumn.getScale();
				}
				
				case CHAR:
				case VARCHAR:
				{
					return clientColumn.getLength() == dbColumn.getLength();
				}
			}
		}
		
		Boolean match = getTypeMatch(clientColumn,dbColumn);
		if(match != null)
		{
			return match;
		}
		
		match = getTypeMatch(dbColumn,clientColumn);
		if(match != null)
		{
			return match;
		}
		
		return false;
	}
	
	
	@SuppressWarnings("incomplete-switch")
	private Boolean getTypeMatch(ColumnMetaData thisColumn, ColumnMetaData thatColumn)
	{
		DataType thisType = thisColumn.getType();
		DataType thatType = thatColumn.getType();
		
		switch(thisType)
		{
			case LONGVARCHAR:
			{
				return thatType == DataType.VARCHAR;
			}
			case BINARY:
			case VARBINARY:
			case LONGVARBINARY:
			{
				return 	thatType == DataType.VARBINARY 
						&& thisColumn.getLength() == thatColumn.getLength();
			}
			case FLOAT:
			{
				return 	thatType == DataType.DOUBLE;
			}
			case NUMERIC:
			{
				return 	thatType == DataType.DECIMAL
						&& thisColumn.getLength() == thatColumn.getLength()
						&& thisColumn.getScale() == thatColumn.getScale();
			}
			case BOOLEAN:
			{
				return 	thatType == DataType.TINYINT 
						&& thatColumn.getLength() == 1;
			}
			
			case TINYINT:
			{
				return thatType == DataType.BOOLEAN && thisColumn.getLength() == 1;
			}
		}
		
		return null;
	}
	
	
	@Override
	protected void createTable(JDBCConnection jdbcConnection, TableMetaData table)
			throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE CACHED TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" (");
		
		ColumnMetaData[] columns = table.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			
			ColumnMetaData column = columns[i];
			appendEscapedName(column.getName(),sb);
			sb.append(" ");
			appendColumnDefinition(column,sb,params);
		}
		
		for(Index index : table.getIndices())
		{
			if(isSupported(index))
			{
				sb.append(", ");
				appendIndexDefinition(index,sb);
			}
		}
		
		sb.append(")");
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	
	
	@Override
	protected void addColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData columnBefore, ColumnMetaData columnAfter)
			throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD COLUMN ");
		appendEscapedName(column.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb,params);
		if(columnAfter != null)
		{
			sb.append(" BEFORE ");
			appendEscapedName(columnBefore.getName(),sb);
		}
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	
	
	@Override
	protected void alterColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData existing) throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ALTER COLUMN ");
		appendEscapedName(existing.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb,params);
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	

	@SuppressWarnings("incomplete-switch")
	private void appendColumnDefinition(ColumnMetaData column, StringBuilder sb, List params)
	{
		DataType type = column.getType();
		switch(type)
		{
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case REAL:
			case FLOAT:
			case DOUBLE:
			case DATE:
			case TIME:
			case TIMESTAMP:
			case BOOLEAN:
			{
				sb.append(type.name());
			}
			break;
			
			case BINARY:
			case VARBINARY:
			case LONGVARBINARY:
			case CHAR:
			case VARCHAR:
			case LONGVARCHAR:
			{
				sb.append(type.name());
				sb.append("(");
				sb.append(column.getLength());
				sb.append(")");
			}
			break;
			
			case NUMERIC:
			case DECIMAL:
			{
				sb.append(type.name());
				sb.append("(");
				sb.append(column.getLength());
				sb.append(",");
				sb.append(column.getScale());
				sb.append(")");
			}
			break;
			
			case CLOB:
			{
				sb.append("CLOB");
			}
			break;
			
			case BLOB:
			{
				sb.append("BLOB");
			}
			break;
		}
		
		if(!column.isAutoIncrement())
		{
			Object defaultValue = column.getDefaultValue();
			if(!(defaultValue == null && !column.isNullable()))
			{
				sb.append(" DEFAULT ");
				if(defaultValue == null)
				{
					sb.append("NULL");
				}
				else if(defaultValue instanceof String)
				{
					sb.append('\'');
					for(char ch : defaultValue.toString().toCharArray())
					{
						switch(ch)
						{
							case '\'':
								sb.append('\'');
							break;
						}
						sb.append(ch);
					}
					sb.append('\'');
				}
				else if(defaultValue instanceof Date)
				{
					DateFormat format = null;
					switch(type)
					{
						case DATE:
							format = dateFormat;
						break;
						case TIME:
							format = timeFormat;
						break;
						case TIMESTAMP:
							format = timestampFormat;
						break;
					}
					
					if(format != null)
					{
						sb.append('\'');
						sb.append(format.format((Date)defaultValue));
						sb.append('\'');
					}
				}
				else
				{
					sb.append(defaultValue.toString());
				}
			}
		}
		
		if(column.isNullable())
		{
			sb.append(" NULL");
		}
		else
		{
			sb.append(" NOT NULL");
		}
		
		if(column.isAutoIncrement())
		{
			sb.append(" AUTO_INCREMENT");
		}
	}
	
	
	@Override
	protected void dropColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column) throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" DROP COLUMN ");
		appendEscapedName(column.getName(),sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	@Override
	protected void createIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		if(!isSupported(index))
		{
			return;
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD ");
		appendIndexDefinition(index,sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private boolean isSupported(Index index)
	{
		return index.getType() != IndexType.NORMAL;
	}
	
	
	private void appendIndexDefinition(Index index, StringBuilder sb) throws DBException
	{
		switch(index.getType())
		{
			case PRIMARY_KEY:
			{
				sb.append("PRIMARY KEY");
			}
			break;
			
			case UNIQUE:
			{
				sb.append("UNIQUE");
			}
			break;
			
			default:
			{
				throw new DBException(this.dataSource,
						"Only primary keys and unique indices are supported.");
			}
		}
		
		sb.append(" (");
		String[] columns = index.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			appendEscapedName(columns[i],sb);
		}
		sb.append(")");
	}
	
	
	@Override
	protected void dropIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" DROP ");
		
		if(index.getType() == IndexType.PRIMARY_KEY)
		{
			sb.append("PRIMARY KEY");
		}
		else
		{
			sb.append("CONSTRAINT ");
			appendEscapedName(getValidIndexName(index),sb);
		}
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private String getValidIndexName(Index index)
	{
		String name = index.getName();

		return name;
	}
}

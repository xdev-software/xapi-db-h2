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


import static com.xdev.jadoth.sqlengine.internal.QueryPart.ASEXPRESSION;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.indent;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isSingleLine;

import com.xdev.jadoth.sqlengine.SELECT;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler;

public class H2DMLAssembler extends StandardDMLAssembler<H2Dbms>
{
	public H2DMLAssembler(final H2Dbms dbms)
	{
		super(dbms);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	/**
	 * @param query
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @return
	 * @see net.jadoth.sqlengine.dbmsAdaptor.standard.StandardDMLAssembler#assembleSELECT(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, int, java.lang.String,
	 *      java.lang.String)
	 */
	@Override
	protected StringBuilder assembleSELECT(final SELECT query, final StringBuilder sb,
			final int indentLevel, final int flags, final String clauseSeperator,
			final String newLine)
	{
		indent(sb,indentLevel,isSingleLine(flags)).append(query.keyword());
		this.assembleSelectDISTINCT(query,sb,indentLevel,flags);
		this.assembleSelectItems(query,sb,flags,indentLevel,newLine);
		this.assembleSelectSqlClauses(query,sb,indentLevel,flags | ASEXPRESSION,clauseSeperator,
				newLine);
		this.assembleAppendSELECTs(query,sb,indentLevel,flags,clauseSeperator,newLine);
		this.assembleSelectRowLimit(query,sb,flags,clauseSeperator,newLine,indentLevel);
		return sb;
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @param indentLevel
	 * @return
	 * @see net.jadoth.sqlengine.dbmsAdaptor.standard.StandardDMLAssembler#assembleSelectRowLimit(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, java.lang.String, java.lang.String,
	 *      int)
	 */
	@Override
	protected StringBuilder assembleSelectRowLimit(final SELECT query, final StringBuilder sb,
			final int flags, final String clauseSeperator, final String newLine,
			final int indentLevel)
	{
		final Integer offset = query.getOffsetSkipCount();
		final Integer limit = query.getFetchFirstRowCount();
		
		if(offset != null && limit != null)
		{
			sb.append(newLine).append(clauseSeperator).append("LIMIT ").append(limit)
					.append(" OFFSET ").append(offset);
		}
		else if(limit != null)
		{
			sb.append(newLine).append(clauseSeperator).append("LIMIT ").append(limit);
		}
		return sb;
	}
}

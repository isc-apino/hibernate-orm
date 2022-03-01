package org.hibernate.community.dialect;

/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */


/**
 * A Hibernate dialect for InterSystems IRIS
 *
 * intended for  Hibernate 5.2+  and jdk 1.8
 *
 *  Hibernate works with intersystems-jdbc-3.2.0.jar, located in the dev\java\lib\JDK18 sub-directory
 *  of the InterSystems IRIS installation directory.
 * 	Hibernate properties example
 * 		hibernate.dialect org.hibernate.dialect.ISCDialect
 *		hibernate.connection.driver_class com.intersystems.jdbc.IRISDriver
 *		hibernate.connection.url jdbc:IRIS://127.0.0.1:1972/USER/*
 *		hibernate.connection.username _SYSTEM*
 *		hibernate.connection.password SYS*
 *   Change items marked by '*' to correspond to your system.
 *
 *
 * @author Jonathan Levinson, Ralph Vater, Dmitry Umansky
 *
 **/

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

import org.hibernate.LockMode;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.function.CastingConcatFunction;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.query.spi.Limit;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.sqm.NullPrecedence;
import org.hibernate.ScrollMode;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.ConditionalParenthesisFunction;
import org.hibernate.dialect.function.ConvertFunction;
import org.hibernate.dialect.function.NoArgSQLFunction;
import org.hibernate.dialect.function.NvlFunction;
import org.hibernate.dialect.function.SQLFunctionTemplate;
import org.hibernate.dialect.function.StandardJDBCEscapeFunction;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.function.VarArgsSQLFunction;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.community.dialect.identity.InterSystemsIRISIdentityColumnSupport;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.dialect.lock.OptimisticForceIncrementLockingStrategy;
import org.hibernate.dialect.lock.OptimisticLockingStrategy;
import org.hibernate.dialect.lock.PessimisticForceIncrementLockingStrategy;
import org.hibernate.dialect.lock.PessimisticReadSelectLockingStrategy;
import org.hibernate.dialect.lock.PessimisticReadUpdateLockingStrategy;
import org.hibernate.dialect.lock.PessimisticWriteSelectLockingStrategy;
import org.hibernate.dialect.lock.PessimisticWriteUpdateLockingStrategy;
import org.hibernate.dialect.lock.SelectLockingStrategy;
import org.hibernate.dialect.lock.UpdateLockingStrategy;
import org.hibernate.dialect.pagination.AbstractLimitHandler;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.exception.internal.InterSystemsIRISSQLExceptionConversionDelegate;
import org.hibernate.exception.spi.SQLExceptionConversionDelegate;
import org.hibernate.exception.spi.TemplatedViolatedConstraintNameExtracter;
import org.hibernate.exception.spi.ViolatedConstraintNameExtracter;
import org.hibernate.hql.spi.id.IdTableSupportStandardImpl;
import org.hibernate.hql.spi.id.MultiTableBulkIdStrategy;
import org.hibernate.hql.spi.id.global.GlobalTemporaryTableBulkIdStrategy;
import org.hibernate.hql.spi.id.local.AfterUseAction;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.query.sqm.function.JdbcEscapeFunctionDescriptor;
import org.hibernate.sql.InterSystemsIRISJoinFragment;
import org.hibernate.sql.JoinFragment;
import org.hibernate.sql.ast.SqlAstNodeRenderingMode;
import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.StandardBasicTypes;

public class InterSystemsIRISDialect extends Dialect {

	private static final LimitHandler IRISLimitHandler = new AbstractLimitHandler() {
		// adapted from TopLimitHandler
		@Override
		public String processSql(String sql, Limit limit) {
			// This does not support the InterSystems IRIS SQL 'DISTINCT BY (comma-list)'
			// extensions, but this extension is not supported through Hibernate anyway.
			final boolean hasOffset = hasFirstRow( limit );
			String lowersql = sql.toLowerCase( Locale.ROOT );
			final int selectIndex = lowersql.indexOf( "select" );

			if (hasOffset) {
				// insert clause after SELECT
				return new StringBuilder( sql.length() + 27 )
						.append( sql )
						.insert( selectIndex + 6, " %ROWOFFSET ? %ROWLIMIT ? " )
						.toString();
			}
			else {
				// insert clause after SELECT (and DISTINCT, if present)
				final int selectDistinctIndex = lowersql.indexOf( "select distinct" );
				final int insertionPoint = selectIndex + (selectDistinctIndex == selectIndex ? 15 : 6);

				return new StringBuilder( sql.length() + 8 )
						.append( sql )
						.insert( insertionPoint, " TOP ? " )
						.toString();
			}
		}

		@Override
		public boolean supportsLimit() {
			return true;
		}

		@Override
		public boolean bindLimitParametersFirst() {
			return true;
		}
	};

	private LimitHandler limitHandler;

	/**
	 * Creates new <code>InterSystemsIRISDialect</code> instance. Sets up the JDBC /
	 * Cach&eacute; type mappings.
	 */
	public InterSystemsIRISDialect(DatabaseVersion version) {
		super(version);
		commonRegistration();
		this.limitHandler = IRISLimitHandler;
	}

	public InterSystemsIRISDialect(DialectResolutionInfo info) {
		super(info);
		commonRegistration();
		this.limitHandler = IRISLimitHandler;
	}

	@Override
	public void initializeFunctionRegistry(QueryEngine queryEngine) {
		super.initializeFunctionRegistry( queryEngine );

		BasicTypeRegistry basicTypeRegistry = queryEngine.getTypeConfiguration().getBasicTypeRegistry();

		queryEngine.getSqmFunctionRegistry().register( "abs", new StandardSQLFunction( "abs" ) );
		queryEngine.getSqmFunctionRegistry().register( "acos", new JdbcEscapeFunctionDescriptor( "acos", new StandardSQLFunction( "acos", StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "%alphaup", new StandardSQLFunction( "%alphaup", StandardBasicTypes.STRING ) );
		queryEngine.getSqmFunctionRegistry().register( "ascii", new StandardSQLFunction( "ascii", StandardBasicTypes.STRING ) );
		queryEngine.getSqmFunctionRegistry().register( "asin", new JdbcEscapeFunctionDescriptor( "asin", new StandardSQLFunction( "asin",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "atan", new JdbcEscapeFunctionDescriptor( "atan", new StandardSQLFunction( "atan",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().registerPattern( "bit_length", "($length(?1)*8)", basicTypeRegistry.resolve( StandardBasicTypes.INTEGER ) );
		queryEngine.getSqmFunctionRegistry().register( "ceiling", new StandardSQLFunction( "ceiling", StandardBasicTypes.INTEGER ) );
		queryEngine.getSqmFunctionRegistry().register( "char", new JdbcEscapeFunctionDescriptor( "char", new StandardSQLFunction( "char",  StandardBasicTypes.CHARACTER ) ) );
		queryEngine.getSqmFunctionRegistry().register( "character_length", new StandardSQLFunction( "character_length", StandardBasicTypes.INTEGER ) );
		queryEngine.getSqmFunctionRegistry().register( "char_length", new StandardSQLFunction( "char_length", StandardBasicTypes.INTEGER ) );
		queryEngine.getSqmFunctionRegistry().register( "cos", new JdbcEscapeFunctionDescriptor( "cos", new StandardSQLFunction( "cos",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "cot", new JdbcEscapeFunctionDescriptor( "cot", new StandardSQLFunction( "cot",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "coalesce", new VarArgsSQLFunction( "coalesce(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register(
				"concat",
				new CastingConcatFunction(
						this,
						"||",
						SqlAstNodeRenderingMode.DEFAULT,
						queryEngine.getTypeConfiguration()
				)
		);
		queryEngine.getSqmFunctionRegistry().register( "convert", new ConvertFunction() );
		queryEngine.getSqmFunctionRegistry().register( "curdate", new JdbcEscapeFunctionDescriptor( "curdate", new StandardSQLFunction( "curdate",  StandardBasicTypes.DATE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "current_date", new NoArgSQLFunction( "current_date", StandardBasicTypes.DATE, false ) );
		queryEngine.getSqmFunctionRegistry().register( "current_time", new NoArgSQLFunction( "current_time", StandardBasicTypes.TIME, false ) );
		queryEngine.getSqmFunctionRegistry().register( "current_timestamp", new ConditionalParenthesisFunction( "current_timestamp", StandardBasicTypes.TIMESTAMP ) );
		queryEngine.getSqmFunctionRegistry().register( "curtime", new JdbcEscapeFunctionDescriptor( "curtime", new StandardSQLFunction( "curtime",  StandardBasicTypes.TIME ) ) );
		queryEngine.getSqmFunctionRegistry().register( "database", new JdbcEscapeFunctionDescriptor( "database", new StandardSQLFunction( "database",  StandardBasicTypes.STRING ) ) );
		queryEngine.getSqmFunctionRegistry().register( "dateadd", new VarArgsSQLFunction( StandardBasicTypes.TIMESTAMP, "dateadd(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "datediff", new VarArgsSQLFunction( StandardBasicTypes.INTEGER, "datediff(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "datename", new VarArgsSQLFunction( StandardBasicTypes.STRING, "datename(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "datepart", new VarArgsSQLFunction( StandardBasicTypes.INTEGER, "datepart(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "day", new StandardSQLFunction( "day", StandardBasicTypes.INTEGER ) );
		queryEngine.getSqmFunctionRegistry().register( "dayname", new JdbcEscapeFunctionDescriptor( "dayname", new StandardSQLFunction( "dayname",  StandardBasicTypes.STRING ) ) );
		queryEngine.getSqmFunctionRegistry().register( "dayofmonth", new JdbcEscapeFunctionDescriptor( "dayofmonth", new StandardSQLFunction( "dayofmonth",  StandardBasicTypes.INTEGER ) ) );
		queryEngine.getSqmFunctionRegistry().register( "dayofweek", new JdbcEscapeFunctionDescriptor( "dayofweek", new StandardSQLFunction( "dayofweek",  StandardBasicTypes.INTEGER ) ) );
		queryEngine.getSqmFunctionRegistry().register( "dayofyear", new JdbcEscapeFunctionDescriptor( "dayofyear", new StandardSQLFunction( "dayofyear",  StandardBasicTypes.INTEGER ) ) );
		// is it necessary to register %exact since it can only appear in a where clause?
		queryEngine.getSqmFunctionRegistry().register( "%exact", new StandardSQLFunction( "%exact", StandardBasicTypes.STRING ) );
		queryEngine.getSqmFunctionRegistry().register( "exp", new JdbcEscapeFunctionDescriptor( "exp", new StandardSQLFunction( "exp",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "%external", new StandardSQLFunction( "%external", StandardBasicTypes.STRING ) );
		queryEngine.getSqmFunctionRegistry().register( "$extract", new VarArgsSQLFunction( StandardBasicTypes.INTEGER, "$extract(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "$find", new VarArgsSQLFunction( StandardBasicTypes.INTEGER, "$find(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "floor", new StandardSQLFunction( "floor", StandardBasicTypes.INTEGER ) );
		queryEngine.getSqmFunctionRegistry().register( "getdate", new StandardSQLFunction( "getdate", StandardBasicTypes.TIMESTAMP ) );
		queryEngine.getSqmFunctionRegistry().register( "hour", new JdbcEscapeFunctionDescriptor( "hour", new StandardSQLFunction( "hour",  StandardBasicTypes.INTEGER ) ) );
		queryEngine.getSqmFunctionRegistry().register( "ifnull", new VarArgsSQLFunction( "ifnull(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "%internal", new StandardSQLFunction( "%internal" ) );
		queryEngine.getSqmFunctionRegistry().register( "isnull", new VarArgsSQLFunction( "isnull(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "isnumeric", new StandardSQLFunction( "isnumeric", StandardBasicTypes.INTEGER ) );
		queryEngine.getSqmFunctionRegistry().register( "lcase", new JdbcEscapeFunctionDescriptor( "lcase", new StandardSQLFunction( "lcase",  StandardBasicTypes.STRING ) ) );
		queryEngine.getSqmFunctionRegistry().register( "left", new JdbcEscapeFunctionDescriptor( "left", new StandardSQLFunction( "left",  StandardBasicTypes.STRING ) ) );
		queryEngine.getSqmFunctionRegistry().register( "len", new StandardSQLFunction( "len", StandardBasicTypes.INTEGER ) );
		queryEngine.getSqmFunctionRegistry().register( "$length", new VarArgsSQLFunction( "$length(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "$list", new VarArgsSQLFunction( "$list(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "$listdata", new VarArgsSQLFunction( "$listdata(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "$listfind", new VarArgsSQLFunction( "$listfind(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "$listget", new VarArgsSQLFunction( "$listget(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "$listlength", new StandardSQLFunction( "$listlength", StandardBasicTypes.INTEGER ) );
		queryEngine.getSqmFunctionRegistry().register( "locate", new StandardSQLFunction( "$FIND", StandardBasicTypes.INTEGER ) );
		queryEngine.getSqmFunctionRegistry().register( "log", new JdbcEscapeFunctionDescriptor( "log", new StandardSQLFunction( "log",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "log10", new JdbcEscapeFunctionDescriptor( "log", new StandardSQLFunction( "log",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "lower", new StandardSQLFunction( "lower" ) );
		queryEngine.getSqmFunctionRegistry().register( "ltrim", new StandardSQLFunction( "ltrim" ) );
		queryEngine.getSqmFunctionRegistry().register( "minute", new JdbcEscapeFunctionDescriptor( "minute", new StandardSQLFunction( "minute",  StandardBasicTypes.INTEGER ) ) );
		queryEngine.getSqmFunctionRegistry().register( "mod", new JdbcEscapeFunctionDescriptor( "mod", new StandardSQLFunction( "mod",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "month", new JdbcEscapeFunctionDescriptor( "month", new StandardSQLFunction( "month",  StandardBasicTypes.INTEGER ) ) );
		queryEngine.getSqmFunctionRegistry().register( "monthname", new JdbcEscapeFunctionDescriptor( "monthname", new StandardSQLFunction( "monthname",  StandardBasicTypes.STRING ) ) );
		queryEngine.getSqmFunctionRegistry().register( "now", new JdbcEscapeFunctionDescriptor( "monthname", new StandardSQLFunction( "monthname",  StandardBasicTypes.TIMESTAMP ) ) );
		queryEngine.getSqmFunctionRegistry().register( "nullif", new VarArgsSQLFunction( "nullif(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "nvl", new NvlFunction() );
		queryEngine.getSqmFunctionRegistry().register( "%odbcin", new StandardSQLFunction( "%odbcin" ) );
		queryEngine.getSqmFunctionRegistry().register( "%odbcout", new StandardSQLFunction( "%odbcin" ) );
		queryEngine.getSqmFunctionRegistry().register( "%pattern", new VarArgsSQLFunction( StandardBasicTypes.STRING, "", "%pattern", "" ) );
		queryEngine.getSqmFunctionRegistry().register( "pi", new JdbcEscapeFunctionDescriptor( "pi", new StandardSQLFunction( "pi",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "$piece", new VarArgsSQLFunction( StandardBasicTypes.STRING, "$piece(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "position", new VarArgsSQLFunction( StandardBasicTypes.INTEGER, "position(", " in ", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "power", new VarArgsSQLFunction( StandardBasicTypes.STRING, "power(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "quarter", new JdbcEscapeFunctionDescriptor( "quarter", new StandardSQLFunction( "quarter",  StandardBasicTypes.INTEGER ) ) );
		queryEngine.getSqmFunctionRegistry().register( "repeat", new VarArgsSQLFunction( StandardBasicTypes.STRING, "repeat(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "replicate", new VarArgsSQLFunction( StandardBasicTypes.STRING, "replicate(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "right", new JdbcEscapeFunctionDescriptor( "right", new StandardSQLFunction( "right",  StandardBasicTypes.STRING ) ) );
		queryEngine.getSqmFunctionRegistry().register( "round", new VarArgsSQLFunction( StandardBasicTypes.FLOAT, "round(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "rtrim", new StandardSQLFunction( "rtrim", StandardBasicTypes.STRING ) );
		queryEngine.getSqmFunctionRegistry().register( "second", new JdbcEscapeFunctionDescriptor( "second", new StandardSQLFunction( "second",  StandardBasicTypes.INTEGER ) ) );
		queryEngine.getSqmFunctionRegistry().register( "sign", new StandardSQLFunction( "sign", StandardBasicTypes.INTEGER ) );
		queryEngine.getSqmFunctionRegistry().register( "sin", new JdbcEscapeFunctionDescriptor( "sin", new StandardSQLFunction( "sin",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "space", new StandardSQLFunction( "space", StandardBasicTypes.STRING ) );
		queryEngine.getSqmFunctionRegistry().register( "%sqlstring", new VarArgsSQLFunction( StandardBasicTypes.STRING, "%sqlstring(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "%sqlupper", new VarArgsSQLFunction( StandardBasicTypes.STRING, "%sqlupper(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "sqrt", new JdbcEscapeFunctionDescriptor( "SQRT", new StandardSQLFunction( "SQRT",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "%startswith", new VarArgsSQLFunction( StandardBasicTypes.STRING, "", "%startswith", "" ) );
		queryEngine.getSqmFunctionRegistry().registerPattern( "str", "cast(?1 as char varying)", basicTypeRegistry.resolve( StandardBasicTypes.STRING ) );
		queryEngine.getSqmFunctionRegistry().register( "string", new VarArgsSQLFunction( StandardBasicTypes.STRING, "string(", ",", ")" ) );
		// note that %string is deprecated
		queryEngine.getSqmFunctionRegistry().register( "%string", new VarArgsSQLFunction( StandardBasicTypes.STRING, "%string(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "substr", new VarArgsSQLFunction( StandardBasicTypes.STRING, "substr(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "substring", new VarArgsSQLFunction( StandardBasicTypes.STRING, "substring(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "sysdate", new NoArgSQLFunction( "sysdate", StandardBasicTypes.TIMESTAMP, false ) );
		queryEngine.getSqmFunctionRegistry().register( "tan", new JdbcEscapeFunctionDescriptor( "tan", new StandardSQLFunction( "tan",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "timestampadd", new JdbcEscapeFunctionDescriptor( "timestampadd", new StandardSQLFunction( "timestampadd",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "timestampdiff", new JdbcEscapeFunctionDescriptor( "timestampdiff", new StandardSQLFunction( "timestampdiff",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "tochar", new VarArgsSQLFunction( StandardBasicTypes.STRING, "tochar(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "to_char", new VarArgsSQLFunction( StandardBasicTypes.STRING, "to_char(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "todate", new VarArgsSQLFunction( StandardBasicTypes.STRING, "todate(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "to_date", new VarArgsSQLFunction( StandardBasicTypes.STRING, "todate(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "tonumber", new StandardSQLFunction( "tonumber" ) );
		queryEngine.getSqmFunctionRegistry().register( "to_number", new StandardSQLFunction( "tonumber" ) );
		// TRIM(end_keyword string-expression-1 FROM string-expression-2)
		// use Hibernate implementation "From" is one of the parameters they pass in position ?3
		//queryEngine.getSqmFunctionRegistry().registerPattern( "trim", "trim(?1 ?2 from ?3)", basicTypeRegistry.resolve( StandardBasicTypes.STRING ) );
		queryEngine.getSqmFunctionRegistry().register( "truncate", new JdbcEscapeFunctionDescriptor( "truncate", new StandardSQLFunction( "truncate",  StandardBasicTypes.STRING ) ) );
		queryEngine.getSqmFunctionRegistry().register( "ucase", new JdbcEscapeFunctionDescriptor( "ucase", new StandardSQLFunction( "ucase",  StandardBasicTypes.STRING ) ) );
		queryEngine.getSqmFunctionRegistry().register( "upper", new StandardSQLFunction( "upper" ) );
		// %upper is deprecated
		queryEngine.getSqmFunctionRegistry().register( "%upper", new StandardSQLFunction( "%upper" ) );
		queryEngine.getSqmFunctionRegistry().register( "user", new JdbcEscapeFunctionDescriptor( "user", new StandardSQLFunction( "user",  StandardBasicTypes.STRING ) ) );
		queryEngine.getSqmFunctionRegistry().register( "week", new JdbcEscapeFunctionDescriptor( "user", new StandardSQLFunction( "user",  StandardBasicTypes.INTEGER ) ) );
		queryEngine.getSqmFunctionRegistry().register( "xmlconcat", new VarArgsSQLFunction( StandardBasicTypes.STRING, "xmlconcat(", ",", ")" ) );
		queryEngine.getSqmFunctionRegistry().register( "xmlelement", new VarArgsSQLFunction( StandardBasicTypes.STRING, "xmlelement(", ",", ")" ) );
		// xmlforest requires a new kind of function constructor
		queryEngine.getSqmFunctionRegistry().register( "year", new JdbcEscapeFunctionDescriptor( "year", new StandardSQLFunction( "year",  StandardBasicTypes.INTEGER ) ) );
	}

	protected final void commonRegistration() {
		// Note: For object <-> SQL datatype mappings see:
		//	 Configuration Manager | Advanced | SQL | System DDL Datatype Mappings
		//
		//	TBD	registerColumnType(Types.BINARY,        "binary($1)");
		// changed 08-11-2005, jsl
		registerColumnType( Types.BINARY, "varbinary($1)" );
		registerColumnType( Types.BIGINT, "BigInt" );
		registerColumnType( Types.BIT, "bit" );
		registerColumnType( Types.CHAR, "char(1)" );
		registerColumnType( Types.DATE, "date" );
		registerColumnType( Types.DECIMAL, "decimal" );
		registerColumnType( Types.DOUBLE, "double" );
		registerColumnType( Types.FLOAT, "float" );
		registerColumnType( Types.INTEGER, "integer" );
		registerColumnType( Types.LONGVARBINARY, "longvarbinary" );
		registerColumnType( Types.LONGVARCHAR, "longvarchar" );
		registerColumnType( Types.NUMERIC, "numeric($p,$s)" );
		registerColumnType( Types.REAL, "real" );
		registerColumnType( Types.SMALLINT, "smallint" );
		registerColumnType( Types.TIMESTAMP, "timestamp" );
		registerColumnType( Types.TIME, "time" );
		registerColumnType( Types.TINYINT, "tinyint" );
		registerColumnType( Types.VARBINARY, "longvarbinary" );
		registerColumnType( Types.VARCHAR, "varchar($l)" );
		registerColumnType( Types.BLOB, "longvarbinary" );
		registerColumnType( Types.CLOB, "longvarchar" );
		registerColumnType( Types.BOOLEAN, "integer" );
		registerColumnType( Types.BINARY, "varbinary($l)" );

		getDefaultProperties().setProperty( Environment.USE_STREAMS_FOR_BINARY, "false" );
		getDefaultProperties().setProperty( Environment.STATEMENT_BATCH_SIZE, DEFAULT_BATCH_SIZE );

		getDefaultProperties().setProperty( Environment.USE_SQL_COMMENTS, "false" );
	}

	protected final void queryEngine.getSqmFunctionRegistry().registers() {
		this.queryEngine.getSqmFunctionRegistry().register( "str", new VarArgsSQLFunction( StandardBasicTypes.STRING, "str(", ",", ")" ) );
		//new overwrites
		queryEngine.getSqmFunctionRegistry().register( "year", new StandardSQLFunction( "year", StandardBasicTypes.INTEGER ) );
		queryEngine.getSqmFunctionRegistry().register( "sqrt", new StandardSQLFunction( "sqrt", StandardBasicTypes.DOUBLE ) );
		queryEngine.getSqmFunctionRegistry().register( "log10", new JdbcEscapeFunctionDescriptor( "log10", new StandardSQLFunction( "log10",  StandardBasicTypes.DOUBLE ) ) );
		queryEngine.getSqmFunctionRegistry().register( "current_timestamp", new NoArgSQLFunction( "current_timestamp", StandardBasicTypes.TIMESTAMP, false ) );
	}

	// DDL support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public boolean hasAlterTable() {
		// Does this dialect support the ALTER TABLE syntax?
		return true;
	}

	@Override
	public boolean qualifyIndexName() {
		// Do we need to qualify index names with the schema name?
		return false;
	}

	@Override
	@SuppressWarnings("StringBufferReplaceableByString")
	public String getAddForeignKeyConstraintString(
			String constraintName,
			String[] foreignKey,
			String referencedTable,
			String[] primaryKey,
			boolean referencesPrimaryKey) {
		// The syntax used to add a foreign key constraint to a table.
		return new StringBuilder( 300 )
				.append( " ADD CONSTRAINT " )
				.append( constraintName )
				.append( " FOREIGN KEY " )
				.append( constraintName )
				.append( " (" )
				.append( String.join( ", ", foreignKey ) )
				.append( ") REFERENCES " )
				.append( referencedTable )
				.append( " (" )
				.append( String.join( ", ", primaryKey ) )
				.append( ") " )
				.toString();
	}

	/**
	 * Does this dialect support check constraints?
	 *
	 * @return {@code false} (InterSystemsIRIS does not support check constraints)
	 */
	@SuppressWarnings("UnusedDeclaration")
	public boolean supportsCheck() {
		return false;
	}

	@Override
	public String getAddColumnString() {
		// The syntax used to add a column to a table
		return " add column";
	}

	@Override
	public String getCascadeConstraintsString() {
		// Completely optional cascading drop clause.
		return "";
	}

	@Override
	public boolean dropConstraints() {
		// Do we need to drop constraints before dropping tables in this dialect?
		return true;
	}

	@Override
	public boolean supportsCascadeDelete() {
		return true;
	}

	@Override
	public boolean hasSelfReferentialForeignKeyBug() {
		return true;
	}

	@Override
	public MultiTableBulkIdStrategy getDefaultMultiTableBulkIdStrategy() {
		return new GlobalTemporaryTableBulkIdStrategy(
				new IdTableSupportStandardImpl() {
					@Override
					public String generateIdTableName(String baseName) {
						final String name = super.generateIdTableName( baseName );
						return name.length() > 25 ? name.substring( 1, 25 ) : name;
					}

					@Override
					public String getCreateIdTableCommand() {
						return "create global temporary table";
					}
				},
				AfterUseAction.CLEAN
		);
	}

/*	@Override
	public Class getNativeIdentifierGeneratorStrategy() {
		return  IdentityGenerator.class;
	}
*/
	// IDENTITY support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public IdentityColumnSupport getIdentityColumnSupport() {
		return new InterSystemsIRISIdentityColumnSupport();
	}

	// SEQUENCE support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public boolean supportsSequences() {
		return false;
	}

// It really does support sequences, but InterSystems elects to suggest usage of IDENTITY instead :/
// Anyway, below are the actual support overrides for users wanting to use this combo...
//
//	public String getSequenceNextValString(String sequenceName) {
//		return "select InterSystems.Sequences_GetNext('" + sequenceName + "') from InterSystems.Sequences where ucase(name)=ucase('" + sequenceName + "')";
//	}
//
//	public String getSelectSequenceNextValString(String sequenceName) {
//		return "(select InterSystems.Sequences_GetNext('" + sequenceName + "') from InterSystems.Sequences where ucase(name)=ucase('" + sequenceName + "'))";
//	}
//
//	public String getCreateSequenceString(String sequenceName) {
//		return "insert into InterSystems.Sequences(Name) values (ucase('" + sequenceName + "'))";
//	}
//
//	public String getDropSequenceString(String sequenceName) {
//		return "delete from InterSystems.Sequences where ucase(name)=ucase('" + sequenceName + "')";
//	}
//
//	public String getQuerySequencesString() {
//		return "select name from InterSystems.Sequences";
//	}

	// lock acquisition support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public boolean supportsOuterJoinForUpdate() {
		return false;
	}

	@Override
	public LockingStrategy getLockingStrategy( Lockable lockable, LockMode lockMode ) {

		// Just to make some tests happy, but InterSystems IRIS doesn't really support this.
		// need to use READ_COMMITTED as isolation level
		if (LockMode.UPGRADE == lockMode) {
			return new SelectLockingStrategy(lockable, lockMode);
		}

		// InterSystems InterSystemsIRIS does not current support "SELECT ... FOR UPDATE" syntax...
		// Set your transaction mode to READ_COMMITTED before using
		if ( lockMode==LockMode.PESSIMISTIC_FORCE_INCREMENT) {
			return new PessimisticForceIncrementLockingStrategy( lockable, lockMode);
		}
		else if ( lockMode==LockMode.PESSIMISTIC_WRITE) {
			return lockable.isVersioned()
					? new PessimisticWriteUpdateLockingStrategy( lockable, lockMode)
					: new PessimisticWriteSelectLockingStrategy( lockable, lockMode);
		}
		else if ( lockMode==LockMode.PESSIMISTIC_READ) {
			return lockable.isVersioned()
					? new PessimisticReadUpdateLockingStrategy( lockable, lockMode)
					: new PessimisticReadSelectLockingStrategy( lockable, lockMode);
		}
		else if ( lockMode==LockMode.OPTIMISTIC) {
			return new OptimisticLockingStrategy( lockable, lockMode);
		}
		else if ( lockMode==LockMode.OPTIMISTIC_FORCE_INCREMENT) {
			return new OptimisticForceIncrementLockingStrategy( lockable, lockMode);
		}
		else if ( lockMode.greaterThan( LockMode.READ ) ) {
			return new UpdateLockingStrategy( lockable, lockMode );
		}
		else {
			return new SelectLockingStrategy( lockable, lockMode );
		}
	}

	// LIMIT support (ala TOP) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public LimitHandler getLimitHandler() {
		//if ( isLegacyLimitHandlerBehaviorEnabled() ) {
		//	return super.getLimitHandler();
		//}
		return limitHandler;
	}

	@Override
	@SuppressWarnings("deprecation")
	public boolean supportsLimit() {
		return true;
	}

	@Override
	@SuppressWarnings("deprecation")
	public boolean supportsLimitOffset() {
		return false;
	}

	@Override
	@SuppressWarnings("deprecation")
	public boolean supportsVariableLimit() {
		return true;
	}

	@Override
	@SuppressWarnings("deprecation")
	public boolean bindLimitParametersFirst() {
		// Does the LIMIT clause come at the start of the SELECT statement, rather than at the end?
		return true;
	}

	@Override
	@SuppressWarnings("deprecation")
	public boolean useMaxForLimit() {
		// Does the LIMIT clause take a "maximum" row number instead of a total number of returned rows?
		return true;
	}

	// adapted from IRISLimitHandler.processSql()
	@Override
	@SuppressWarnings("deprecation")
	public String getLimitString(String sql, boolean hasOffset) {
		// This does not support the InterSystems IRIS SQL 'DISTINCT BY (comma-list)'
		// extensions, but this extension is not supported through Hibernate anyway.
		String lowersql = sql.toLowerCase( Locale.ROOT );
		final int selectIndex = lowersql.indexOf( "select" );

		if (hasOffset) {
			// insert clause after SELECT
			return new StringBuilder( sql.length() + 27 )
					.append( sql )
					.insert( selectIndex + 6, " %ROWOFFSET ? %ROWLIMIT ? " )
					.toString();
		}
		else {
			// insert clause after SELECT (and DISTINCT, if present)
			final int selectDistinctIndex = lowersql.indexOf( "select distinct" );
			final int insertionPoint = selectIndex + (selectDistinctIndex == selectIndex ? 15 : 6);

			return new StringBuilder( sql.length() + 8 )
					.append( sql )
					.insert( insertionPoint, " TOP ? " )
					.toString();
		}
	}

	// callable statement support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public int registerResultSetOutParameter(CallableStatement statement, int col) throws SQLException {
		return col;
	}

	@Override
	public ResultSet getResultSet(CallableStatement ps) throws SQLException {
		ps.execute();
		return (ResultSet) ps.getObject( 1 );
	}

	// miscellaneous support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public String renderOrderByElement(String expression, String collation, String order, NullPrecedence nulls) {
		final String orderBy = super.renderOrderByElement( expression, collation, order, NullPrecedence.NONE );

		if ( (nulls == null)
				|| (nulls == NullPrecedence.NONE)
				|| (order == null || ("ASC".equalsIgnoreCase(order)) && (nulls == NullPrecedence.FIRST))
				|| ("DESC".equalsIgnoreCase(order) && (nulls == NullPrecedence.LAST))
		) {
			return orderBy;
		}

		return String.format(
				"CASE WHEN %s IS NULL THEN %s END, %s",
				expression,
				(nulls == NullPrecedence.FIRST) ? "0 ELSE 1" : "1 ELSE 0",
				orderBy
		);
	}

	@Override
	public String getLowercaseFunction() {
		// The name of the SQL function that transforms a string to lowercase
		return "lower";
	}

	@Override
	public String getNullColumnString() {
		return "";
	}

	@Override
	public JoinFragment createOuterJoinFragment() {
		// Create an OuterJoinGenerator for this dialect.
		return new InterSystemsIRISJoinFragment();
	}

	@Override
	public String getNoColumnsInsertString() {
		// The keyword used to insert a row without specifying
		// any column values
		return " default values";
	}

	@Override
	public SQLExceptionConversionDelegate buildSQLExceptionConversionDelegate() {
		return new InterSystemsIRISSQLExceptionConversionDelegate( this );
	}

	@Override
	public ViolatedConstraintNameExtracter getViolatedConstraintNameExtracter() {
		return EXTRACTER;
	}

	/**
	 * The InterSystemsI RIS ViolatedConstraintNameExtracter.
	 */
	public static final ViolatedConstraintNameExtracter EXTRACTER = new TemplatedViolatedConstraintNameExtracter() {
		@Override
		protected String doExtractConstraintName(SQLException sqle) throws NumberFormatException {
			return extractUsingTemplate( "constraint (", ") violated", sqle.getMessage() );
		}
	};


	// Overridden informational metadata ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public boolean supportsEmptyInList() {
		return false;
	}

	@Override
	public boolean areStringComparisonsCaseInsensitive() {
		return true;
	}

	@Override
	public boolean supportsResultSetPositionQueryMethodsOnForwardOnlyCursor() {
		return false;
	}

	/**
	 * ddl like ""value" integer null check ("value">=2 AND "value"<=10)" isn't supported
	 */
	@Override
	public boolean supportsColumnCheck() {
		return false;
	}

	/**
	 * select count(distinct a,b,c) from hasi
	 * isn't supported ;)
	 */
	@Override
	public boolean supportsTupleDistinctCounts() {
		return false;
	}

	@Override
	public boolean supportsTuplesInSubqueries() {
		return false;
	}

	@Override
	public ScrollMode defaultScrollMode() {
		return super.defaultScrollMode();
	}

	@Override
	public boolean supportsExistsInSelect() {
		return false;
	}
}

package org.hibernate.community.dialect;

/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */


/**
 * A Hibernate dialect for InterSystems IRIS
 * <p>
 * intended for  Hibernate 5.2+  and jdk 1.8
 * <p>
 * Hibernate works with intersystems-jdbc-3.2.0.jar, located in the dev\java\lib\JDK18 sub-directory
 * of the InterSystems IRIS installation directory.
 * Hibernate properties example
 * hibernate.dialect org.hibernate.dialect.ISCDialect
 * hibernate.connection.driver_class com.intersystems.jdbc.IRISDriver
 * hibernate.connection.url jdbc:IRIS://127.0.0.1:1972/USER/*
 * hibernate.connection.username _SYSTEM*
 * hibernate.connection.password SYS*
 * Change items marked by '*' to correspond to your system.
 *
 * @author Jonathan Levinson, Ralph Vater, Dmitry Umansky
 */

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.hibernate.LockMode;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.function.CommonFunctionFactory;
import org.hibernate.dialect.function.TimestampaddFunction;
import org.hibernate.dialect.function.TimestampdiffFunction;
import org.hibernate.dialect.sequence.NoSequenceSupport;
import org.hibernate.dialect.sequence.SequenceSupport;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.DataException;
import org.hibernate.exception.spi.TemplatedViolatedConstraintNameExtractor;
import org.hibernate.exception.spi.ViolatedConstraintNameExtractor;
import org.hibernate.internal.util.JdbcExceptionHelper;
import org.hibernate.query.spi.Limit;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.ScrollMode;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.Dialect;
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
import org.hibernate.exception.spi.SQLExceptionConversionDelegate;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.query.sqm.function.SqmFunctionRegistry;
import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.spi.TypeConfiguration;

import static org.hibernate.exception.spi.TemplatedViolatedConstraintNameExtractor.extractUsingTemplate;
import static org.hibernate.query.sqm.produce.function.FunctionParameterType.NUMERIC;

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

			if ( hasOffset ) {
				// insert clause after SELECT
				return new StringBuilder( sql.length() + 27 )
						.append( sql )
						.insert( selectIndex + 6, " %ROWOFFSET ? %ROWLIMIT ? " )
						.toString();
			}
			else {
				// insert clause after SELECT (and DISTINCT, if present)
				final int selectDistinctIndex = lowersql.indexOf( "select distinct" );
				final int insertionPoint = selectIndex + ( selectDistinctIndex == selectIndex ? 15 : 6 );

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
		public boolean supportsLimitOffset() {
			return false;
		}

		@Override
		public boolean supportsVariableLimit() {
			return true;
		}

		@Override
		public boolean bindLimitParametersFirst() {
			return true;
		}

		@Override
		public boolean useMaxForLimit() {
			// Does the LIMIT clause take a "maximum" row number instead of a total number of returned rows?
			return true;
		}
	};

	private LimitHandler limitHandler;

	/**
	 * Creates new <code>InterSystemsIRISDialect</code> instance. Sets up the JDBC /
	 * Cach&eacute; type mappings.
	 */
	public InterSystemsIRISDialect(DatabaseVersion version) {
		super( version );
		commonRegistration();
		this.limitHandler = IRISLimitHandler;
	}

	public InterSystemsIRISDialect(DialectResolutionInfo info) {
		super( info );
		commonRegistration();
		this.limitHandler = IRISLimitHandler;
	}

	/**
	 * Register SQL functions supported by IRIS (see https://docs.intersystems.com/iris20212/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_FUNCTIONS)
	 *
	 * ABS 								- super.initializeFunctionRegistry() -> functionFactory.math()
	 * ACOS 							- super.initializeFunctionRegistry() -> functionFactory.math()
	 * ASCII							- functionFactory.ascii()
	 * ASIN								- super.initializeFunctionRegistry() -> functionFactory.math()
	 * ATAN								- super.initializeFunctionRegistry() -> functionFactory.math()
	 * ATAN2							- super.initializeFunctionRegistry() -> functionFactory.math()
	 * CAST								- super.initializeFunctionRegistry()
	 * CEILING						- super.initializeFunctionRegistry() -> functionFactory.math()
	 * CHAR								-
	 * CHARACTER_LENGTH 	- super.initializeFunctionRegistry() -> functionFactory.length_characterLength()
	 * CHAR_INDEX					-
	 * CHAR_LENGTH 				- alternate key for CHARACTER_LENGTH
	 * COALESCE						- super.initializeFunctionRegistry() -> functionFactory.coalesce()
	 * CONCAT 						- super.initializeFunctionRegistry() -> functionFactory.concat()
	 * CONVERT						-
	 * COS 								- super.initializeFunctionRegistry() -> functionFactory.math()
	 * COT								- super.initializeFunctionRegistry() -> functionFactory.trigonometry()
	 * CURDATE						- functionFactory.nowCurdateCurtime()
	 * CURRENT_DATE				- super.initializeFunctionRegistry()
	 * CURRENT_TIME				- super.initializeFunctionRegistry()
	 * CURRENT_TIMESTAMP	- super.initializeFunctionRegistry()
	 * CURTIME						- functionFactory.nowCurdateCurtime()
	 * DATABASE						-
	 * DATALENGTH					-
	 * DATE								- functionFactory.date()
	 * DATEADD						- custom
	 * DATEDIFF						- custom
	 * DATENAME						- functionFactory.datepartDatename()
	 * DATEPART						- functionFactory.datepartDatename()
	 * DAY								- functionFactory.yearMonthDay()
	 * DAYNAME						- functionFactory.daynameMonthname()
	 * DAYOFMONTH					- functionFactory.dayofweekmonthyear()
	 * DAYOFWEEK					- functionFactory.dayofweekmonthyear()
	 * DAYOFYEAR					- functionFactory.dayofweekmonthyear()
	 * DECODE							-
	 * DEGREES						- functionFactory.degrees()
	 * %EXACT							-
	 * EXP								- super.initializeFunctionRegistry() -> functionFactory.math()
	 * %EXTERNAL					-
	 * $EXTRACT						-
	 * $FIND							-
	 * FLOOR							- super.initializeFunctionRegistry() -> functionFactory.math()
	 * GETDATE						-
	 * GETUTCDATE					-
	 * GREATEST						- super.initializeFunctionRegistry() -> functionFactory.leastGreatest()
	 * HOUR								- functionFactory.hourMinuteSecond()
	 * IFNULL							- note super.initializeFunctionRegistry() is wrong
	 * INSTR							- functionFactory.instr()
	 * %INTERNAL					-
	 * ISNULL							-
	 * ISNUMERIC					-
	 * JSON_ARRAY					-
	 * JSON_OBJECT				-
	 * $JUSTIFY						-
	 * LAST_DAY						- functionFactory.lastDay()
	 * LAST_IDENTITY			-
	 * LCASE							- alternative to lower
	 * LEAST							- super.initializeFunctionRegistry() -> functionFactory.leastGreatest()
	 * LEFT								- super.initializeFunctionRegistry() -> functionFactory.leftRight()
	 * LEN								- functionFactory.characterLength_len()
	 * LENGTH							- functionFactory.characterLength_len()
	 * $LENGTH						-
	 * $LIST							-
	 * $LISTBUILD					-
	 * $LISTDATA					-
	 * $LISTFIND					-
	 * $LISTFROMSTRING		-
	 * $LISTGET						-
	 * $LISTLENGTH				-
	 * $LISTSAME					-
	 * $LISTTOSTRING			-
	 * LOG								- custom
	 * LOG10							- functionFactory.log10()
	 * LOWER							- super.initializeFunctionRegistry() -> functionFactory.lowerUpper()
	 * LPAD								- super.initializeFunctionRegistry() -> functionFactory.pad()
	 * LTRIM							- functionFactory.trim1()
	 * %MINUS							-
	 * MINUTE							- functionFactory.hourMinuteSecond()
	 * MOD								- super.initializeFunctionRegistry() -> functionFactory.math()
	 * MONTH							- functionFactory.yearMonthDay()
	 * MONTHNAME					- functionFactory.daynameMonthname()
	 * NOW								- functionFactory.nowCurdateCurtime()
	 * NULLIF							- super.initializeFunctionRegistry -> functionFactory.nullif()
	 * NVL								-
	 * %OBJECT						-
	 * %ODBCIN						-
	 * %ODBCOUT						-
	 * %OID								-
	 * PI									- super.initializeFunctionRegistry() -> functionFactory.math()
	 * $PIECE							-
	 * %PLUS							-
	 * POSITION						- functionFactory.position()
	 * POWER							- super.initializeFunctionRegistry() -> functionFactory.math()
	 * PREDICT						-
	 * PROBABILITY				-
	 * QUARTER						- functionFactory.weekQuarter()
	 * RADIANS						- functionFactory.radians()
	 * REPEAT							- functionFactory.repeat_replicate()
	 * REPLACE						- functionFactory.replace()
	 * REPLICATE					- functionFactory.repeat_replicate()
	 * REVERSE						- functionFactory.reverse()
	 * RIGHT							- super.initializeFunctionRegistry() -> functionFactory.leftRight()
	 * ROUND							- super.initializeFunctionRegistry() -> functionFactory.math()
	 * RPAD								- super.initializeFunctionRegistry() -> functionFactory.pad()
	 * RTRIM							- functionFactory.trim1()
	 * SEARCH_INDEX				-
	 * SECOND							- functionFactory.hourMinuteSecond()
	 * SIGN								- super.initializeFunctionRegistry() -> functionFactory.math()
	 * SIN								- super.initializeFunctionRegistry() -> functionFactory.trigonometry
	 * SPACE							- functionFactory.space()
	 * %SQLSTRING					-
	 * %SQLUPPER					-
	 * SQRT								- super.initializeFunctionRegistry() -> functionFactory.math()
	 * SQUARE							- functionFactory.square()
	 * STR								-
	 * STRING							-
	 * STUFF							-
	 * SUBSTR							- functionFactory.substr()
	 * SUBSTRING					- super.initializeFunctionRegistry() -> functionFactory.substring()
	 * SYSDATE						- functionFactory.sysdate()
	 * TAN								- super.initializeFunctionRegistry() -> functionFactory.trigonometry
	 * TIMESTAMPADD				-
	 * TIMESTAMPDIFF			-
	 * TO_CHAR						- functionFactory.toCharNumberDateTimestamp()
	 * TO_DATE						- functionFactory.toCharNumberDateTimestamp()
	 * TO_NUMBER					- functionFactory.toCharNumberDateTimestamp()
	 * TO_POSIXTIME				-
	 * TO_TIMESTAMP				- functionFactory.toCharNumberDateTimestamp()
	 * $TRANSLATE					-
	 * TRIM								- super.initializeFunctionRegistry()
	 * TRUNCATE						- functionFactory.truncate()
	 * %TRUNCATE					-
	 * $TSQL_NEWID				-
	 * UCASE							- alternative to upper
	 * UNIX_TIMESTAMP			-
	 * UPPER							- super.initializeFunctionRegistry() -> functionFactory.lowerUpper()
	 * USER								-
	 * WEEK								- functionFactory.yearMonthDay()
	 * XMLCONCAT					-
	 * XMLELEMENT					-
	 * XMLFOREST					-
	 * YEAR								- functionFactory.yearMonthDay()
	 */
	@Override
	public void initializeFunctionRegistry(QueryEngine queryEngine) {
		super.initializeFunctionRegistry( queryEngine );

		final BasicTypeRegistry basicTypeRegistry = queryEngine.getTypeConfiguration().getBasicTypeRegistry();
		final TypeConfiguration typeConfiguration = queryEngine.getTypeConfiguration();
		final CommonFunctionFactory functionFactory = new CommonFunctionFactory( queryEngine );
		final SqmFunctionRegistry functionRegistry = queryEngine.getSqmFunctionRegistry();

		// register common functions not registered by super.initializeFunctionRegistry()
		functionFactory.ascii();
		functionFactory.nowCurdateCurtime();
		functionFactory.date();
		functionFactory.datepartDatename();
		functionFactory.yearMonthDay();
		functionFactory.daynameMonthname();
		functionFactory.dayofweekmonthyear();
		functionFactory.degrees();
		functionFactory.hourMinuteSecond();
		functionFactory.instr();
		functionFactory.lastDay();
		functionFactory.characterLength_len();
		functionFactory.log10();
		functionFactory.trim1();
		functionFactory.weekQuarter();
		functionFactory.radians();
		functionFactory.repeat_replicate();
		functionFactory.reverse();
		functionFactory.space();
		functionFactory.square();
		functionFactory.substr();
		functionFactory.sysdate();
		functionFactory.toCharNumberDateTimestamp();
		functionFactory.truncate();

		// register native support for emulated functions
		functionFactory.position();

		// register alternate keys of common functions
		functionRegistry.registerAlternateKey( "char_length", "character_length" );
		functionRegistry.registerAlternateKey( "lcase", "lower" );
		functionRegistry.registerAlternateKey( "ucase", "upper" );

		// timestampadd/diff take keywords as first arg, dateadd/diff take temporal_unit as first arg
		queryEngine.getSqmFunctionRegistry().register(
				"dateadd",
				new TimestampaddFunction( this, typeConfiguration )
		);
		queryEngine.getSqmFunctionRegistry().register(
				"datediff",
				new TimestampdiffFunction( this, typeConfiguration )
		);

		// log is natural log in IRIS, so only take 1 arg
		functionRegistry.namedDescriptorBuilder( "log" )
				.setInvariantType( basicTypeRegistry.resolve( StandardBasicTypes.DOUBLE ) )
				.setExactArgumentCount( 1 )
				.setParameterTypes( NUMERIC )
				.register();
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

		getDefaultProperties().setProperty( Environment.USE_SQL_COMMENTS, "false" );
	}

	@Override
	public boolean useInputStreamToInsertBlob() {
		return false;
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

	// IDENTITY support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public IdentityColumnSupport getIdentityColumnSupport() {
		return new InterSystemsIRISIdentityColumnSupport();
	}

	// SEQUENCE support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public SequenceSupport getSequenceSupport() {
		return NoSequenceSupport.INSTANCE;
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
	public LockingStrategy getLockingStrategy(Lockable lockable, LockMode lockMode) {

		// Just to make some tests happy, but InterSystems IRIS doesn't really support this.
		// need to use READ_COMMITTED as isolation level
		if ( LockMode.UPGRADE == lockMode ) {
			return new SelectLockingStrategy( lockable, lockMode );
		}

		// InterSystems InterSystemsIRIS does not current support "SELECT ... FOR UPDATE" syntax...
		// Set your transaction mode to READ_COMMITTED before using
		if ( lockMode == LockMode.PESSIMISTIC_FORCE_INCREMENT ) {
			return new PessimisticForceIncrementLockingStrategy( lockable, lockMode );
		}
		else if ( lockMode == LockMode.PESSIMISTIC_WRITE ) {
			return lockable.isVersioned()
					? new PessimisticWriteUpdateLockingStrategy( lockable, lockMode )
					: new PessimisticWriteSelectLockingStrategy( lockable, lockMode );
		}
		else if ( lockMode == LockMode.PESSIMISTIC_READ ) {
			return lockable.isVersioned()
					? new PessimisticReadUpdateLockingStrategy( lockable, lockMode )
					: new PessimisticReadSelectLockingStrategy( lockable, lockMode );
		}
		else if ( lockMode == LockMode.OPTIMISTIC ) {
			return new OptimisticLockingStrategy( lockable, lockMode );
		}
		else if ( lockMode == LockMode.OPTIMISTIC_FORCE_INCREMENT ) {
			return new OptimisticForceIncrementLockingStrategy( lockable, lockMode );
		}
		else if ( lockMode.greaterThan( LockMode.READ ) ) {
			return new UpdateLockingStrategy( lockable, lockMode );
		}
		else {
			return new SelectLockingStrategy( lockable, lockMode );
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
	public String getLowercaseFunction() {
		// The name of the SQL function that transforms a string to lowercase
		return "lower";
	}

	@Override
	public String getNullColumnString() {
		return "";
	}

	@Override
	public String getNoColumnsInsertString() {
		// The keyword used to insert a row without specifying
		// any column values
		return " default values";
	}

	private static final Set<String> DATA_CATEGORIES = new HashSet<>();
	private static final Set<Integer> INTEGRITY_VIOLATION_CATEGORIES = new HashSet<>();

	static {
		DATA_CATEGORIES.add( "22" );
		DATA_CATEGORIES.add( "21" );
		DATA_CATEGORIES.add( "02" );

		INTEGRITY_VIOLATION_CATEGORIES.add( 119 );
		INTEGRITY_VIOLATION_CATEGORIES.add( 120 );
		INTEGRITY_VIOLATION_CATEGORIES.add( 121 );
		INTEGRITY_VIOLATION_CATEGORIES.add( 122 );
		INTEGRITY_VIOLATION_CATEGORIES.add( 123 );
		INTEGRITY_VIOLATION_CATEGORIES.add( 124 );
		INTEGRITY_VIOLATION_CATEGORIES.add( 125 );
		INTEGRITY_VIOLATION_CATEGORIES.add( 127 );
	}

	@Override
	public SQLExceptionConversionDelegate buildSQLExceptionConversionDelegate() {
		return (sqlException, message, sql) -> {
			String sqlStateClassCode = JdbcExceptionHelper.extractSqlStateClassCode( sqlException );
			if ( sqlStateClassCode != null ) {
				Integer errorCode = JdbcExceptionHelper.extractErrorCode( sqlException );
				if ( INTEGRITY_VIOLATION_CATEGORIES.contains( errorCode ) ) {
					String constraintName = getViolatedConstraintNameExtractor()
							.extractConstraintName( sqlException );
					return new ConstraintViolationException( message, sqlException, sql, constraintName );
				}
				else if ( DATA_CATEGORIES.contains( sqlStateClassCode ) ) {
					return new DataException( message, sqlException, sql );
				}
			}
			return null; // allow other delegates the chance to look
		};
	}

	@Override
	public ViolatedConstraintNameExtractor getViolatedConstraintNameExtractor() {
		return EXTRACTOR;
	}

	private static final ViolatedConstraintNameExtractor EXTRACTOR =
			new TemplatedViolatedConstraintNameExtractor( sqle -> extractUsingTemplate(
					"constraint (",
					") violated",
					sqle.getMessage()
			) );


	// Overridden informational metadata ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
	public ScrollMode defaultScrollMode() {
		return super.defaultScrollMode();
	}

	@Override
	public boolean supportsExistsInSelect() {
		return false;
	}
}

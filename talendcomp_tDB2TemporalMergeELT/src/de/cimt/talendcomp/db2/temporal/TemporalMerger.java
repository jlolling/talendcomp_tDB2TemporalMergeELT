package de.cimt.talendcomp.db2.temporal;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

public class TemporalMerger {

	private List<Column> listSourceFields = new ArrayList<Column>();
	private String businessTimePeriodStartColumn;
	private String businessTimePeriodEndColumn;
	private boolean businessTimeIsDay;
	private String systemTimePeriodStartColumn;
	private String systemTimePeriodEndColumn;
	private String sourceTable;
	private String sourceSchema;
	private String sourceWhereCondition;
	private String targetTableSpace;
	private String targetTable;
	private String targetSchema;
	private String businessTimeStartValue;
	private String businessTimeEndValue;
	private String scdEndDateStr = "'9999-12-30'";
	private static final String INDENT = "    ";
	private List<String> preparedParamList = new ArrayList<String>();
	private Map<String, Object> paramValues = new HashMap<String, Object>();
	private Connection connection;
	private String createTablespaceStatement = null;
	private String createTableStatement = null;
	private String createHistTableStatement = null;
	private String alterTableStatementAddVersioning = null;
	private String alterTableDropVersionStatement = null;
	private String createIndexPkStatememt = null;
	private String createIndexBUSStatement = null;
	private String createIndexSYSStatement = null;
	private String mergeStatement = null;
	private int countChangedDatasets = 0;
	private boolean debug = false;
	private boolean useBusinessTime = true;
	private boolean useInternalConnection = true;
	private String transStartColumn = "TRANS_START";
	private String histTableName = null;
	private boolean doNotExecute = false;
	private boolean buildTargetTableWithPk = false;
	private boolean enableLogStatements = false;
	private String statementsLogFilePath = null;
	private File statementLogFile = null;
	private String logSectionComent = null;
	private String deleteCondition = null;
	
	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public void setConnection(Connection connection) {
		if (connection != null) {
			useInternalConnection = false;
			this.connection = connection;
		}
	}
	
	public void commit() throws Exception {
		connection.commit();
	}

	public void connect(
			String host, 
			String port, 
			String database, 
			String user,
			String password, 
			String propertiesStr) throws Exception {
		if (isEmpty(host)) {
			throw new IllegalArgumentException("host cannot be null or empty");
		}
		if (isEmpty(port)) {
			throw new IllegalArgumentException("port cannot be null or empty");
		}
		if (isEmpty(database)) {
			throw new IllegalArgumentException("database cannot be null or empty");
		}
		if (isEmpty(user)) {
			throw new IllegalArgumentException("user cannot be null or empty");
		}
		if (isEmpty(password)) {
			throw new IllegalArgumentException("password cannot be null or empty");
		}
		StringBuilder url = new StringBuilder();
		url.append("jdbc:db2://");
		url.append(host);
		url.append(":");
		url.append(port);
		url.append("/");
		url.append(database);
		Properties properties = new Properties();
		if (isEmpty(propertiesStr) == false) {
			StringTokenizer st = new StringTokenizer(propertiesStr, ";");
			String token = null;
			String key = null;
			String value = null;
			int pos = 0;
			while (st.hasMoreTokens()) {
				token = st.nextToken();
				pos = token.indexOf("=");
				if (pos != -1) {
					key = token.substring(0, pos).trim();
					value = token.substring(pos + 1).trim();
					if (isEmpty(key) == false) {
						properties.put(key, value);
					}
				}
			}
		}
		properties.put("user", user);
		properties.put("password", password);
		Class.forName("com.ibm.db2.jcc.DB2Driver"); // the static initializer of the Driver.class loads the driver
		connection = DriverManager.getConnection(url.toString(), properties);
		checkConnection();
		useInternalConnection = true;
	}

	private void checkConnection() throws Exception {
		if (connection == null) {
			throw new Exception("Connection is not created");
		}
		if (connection.isClosed()) {
			throw new Exception("Connection is already closed");
		}
		if (connection.isReadOnly()) {
			throw new Exception("Connection is read only");
		}
	}

	private void executeStatement(String statement) throws Exception {
		checkConnection();
		Statement stat = connection.createStatement();
		try {
			logStatement(statement, null);
			if (doNotExecute == false) {
				stat.execute(statement);
				if (useInternalConnection) {
					connection.commit();
				}
			}
		} catch (Exception e) {
			if (useInternalConnection) {
				connection.rollback();
			}
			throw e;
		} finally {
			if (stat != null) {
				try {
					stat.close();
				} catch (Exception d) {
				}
			}
		}
	}
	
	public void executeCreateTablespace() throws Exception {
		if (isEmpty(targetTableSpace)) {
			throw new IllegalStateException("targetTableSpace cannot be null or empty");
		}
		checkConnection();
		createTablespaceStatement = "create tablespace " + targetTableSpace;
		Statement stat = connection.createStatement();
		try {
			logStatement(createTablespaceStatement, null);
			if (doNotExecute == false) {
				stat.execute(createTablespaceStatement);
				if (useInternalConnection) {
					connection.commit();
				}
			}
		} catch (SQLException e) {
			if (e.getErrorCode() == -601 || e.getMessage().contains("-601") ||
					(e.getMessage().contains("indentical") && e.getMessage().contains("existing") && e.getMessage().contains(targetTableSpace))) {
				// means, the table space already exists
				// we can ignore this error
				System.out.println("Attempt to create already existing tablespace ignored");
			} else {
				throw e;
			}
		} finally {
			if (stat != null) {
				try {
					stat.close();
				} catch (Exception d) {
				}
			}
		}
	}

	public void executeCreateTable() throws Exception {
		checkConnection();
		createTableStatement = createTableStatement();
		executeStatement(createTableStatement);
	}

	public void executeSetupTableAsTemporal() throws Exception {
		checkConnection();
		executeStatement(createAlterTableAlterTransStart1Statement());
		executeStatement(createAlterTableAlterTransStart2Statement());
		executeStatement(createAlterTableAlterSystemBeginStatement());
		executeStatement(createAlterTableAlterSystemEndStatement());
		executeStatement(createAlterTableAddPeriodSystemTimeStatement());
		if (useBusinessTime) {
			executeStatement(createAlterTableAddPeriodBusinessTimeStatement());
		}
		if (buildTargetTableWithPk) {
			executeStatement(createAlterTableAddPkStatement());
		}
	}

	public void executeCreateIndexForSrcKey() throws Exception {
		checkConnection();
		createIndexPkStatememt = createIndexPkStatement();
		executeStatement(createIndexPkStatememt);
	}

	public void executeCreateIndexforBUSTP() throws Exception {
		checkConnection();
		createIndexBUSStatement = createIndexBusTimeperiodStatement();
		executeStatement(createIndexBUSStatement);
	}

	public void executeCreateIndexforSYSTP() throws Exception {
		checkConnection();
		createIndexSYSStatement = createIndexSysTimeperiodStatement();
		executeStatement(createIndexSYSStatement);
	}

	public void executeCreateHistTable() throws Exception {
		checkConnection();
		createHistTableStatement = createHistTableStatement();
		executeStatement(createHistTableStatement);
	}

	public void executeAlterHistTableAppendOn() throws Exception {
		checkConnection();
		executeStatement(createAlterTableHistAppendOnStatement());
	}

	public void executeAlterTableAddVersioning() throws Exception {
		checkConnection();
		alterTableStatementAddVersioning = createAlterTableHistStatement();
		Statement stat = connection.createStatement();
		try {
			logStatement(alterTableStatementAddVersioning, null);
			if (doNotExecute == false) {
				stat.execute(alterTableStatementAddVersioning);
				if (useInternalConnection) {
					connection.commit();
				}
			}
		} catch (SQLException e) {
			if (e.getErrorCode() == -20490 || e.getMessage().contains("-20490")) {
				// means, the table is already in historization mode
				// we can ignore this error
				System.out.println("Attempt to add versioning to an already versioning table: ignored");
			} else {
				if (useInternalConnection) {
					connection.rollback();
				}
				throw e;
			}
		} finally {
			if (stat != null) {
				try {
					stat.close();
				} catch (Exception d) {
				}
			}
		}
	}

	public void executeAlterTableDropVersioning() throws Exception {
		checkConnection();
		alterTableDropVersionStatement = createAlterTableDropVersioningStatement();
		executeStatement(alterTableDropVersionStatement);
	}

	public int executeMerge() throws Exception {
		checkConnection();
		mergeStatement = createMergeStatement();
		// after that all prepared statement parameters are retrieved
		if (preparedParamList.isEmpty()) {
			Statement stat = connection.createStatement();
			logStatement(mergeStatement, null);
			try {
				if (doNotExecute == false) {
					countChangedDatasets = stat.executeUpdate(mergeStatement);
					if (useInternalConnection) {
						connection.commit();
					}
				}
			} catch (Exception e) {
				if (useInternalConnection) {
					connection.rollback();
				}
				throw e;
			} finally {
				if (stat != null) {
					try {
						stat.close();
					} catch (Exception d) {
					}
				}
			}
		} else {
			PreparedStatement ps = connection.prepareStatement(mergeStatement);
			StringBuilder comment = new StringBuilder();
			for (int i = 0; i < preparedParamList.size(); i++) {
				Object value = paramValues.get(preparedParamList.get(i));
				String s = "Prepare param: #"
						+ (i+1)
						+ " "
						+ preparedParamList.get(i) + " = " + value;
				comment.append(s);
				comment.append("\n");
				if (debug) {
					System.out.println(s);
				}
				setParamValue(ps, value, i + 1);
			}
			logStatement(mergeStatement, comment.toString());
			try {
				if (doNotExecute == false) {
					countChangedDatasets = ps.executeUpdate();
					if (useInternalConnection) {
						connection.commit();
					}
				}
			} catch (Exception e) {
				if (useInternalConnection) {
					connection.rollback();
				}
				throw e;
			} finally {
				if (ps != null) {
					try {
						ps.close();
					} catch (Exception d) {
					}
				}
			}
		}
		return countChangedDatasets;
	}

	private void setParamValue(
			PreparedStatement ps,
			Object value, 
			int paramIndex) throws Exception {
		if (value instanceof Integer) {
			ps.setInt(paramIndex, (Integer) value);
		} else if (value instanceof Long) {
			ps.setLong(paramIndex, (Long) value);
		} else if (value instanceof Short) {
			ps.setShort(paramIndex, (Short) value);
		} else if (value instanceof Double) {
			ps.setDouble(paramIndex, (Double) value);
		} else if (value instanceof BigDecimal) {
			ps.setBigDecimal(paramIndex, (BigDecimal) value);
		} else if (value instanceof String) {
			ps.setString(paramIndex, (String) value);
		} else if (value instanceof Timestamp) {
			ps.setTimestamp(paramIndex, (Timestamp) value);
		} else if (value instanceof Date) {
			ps.setDate(paramIndex, new java.sql.Date(((Date) value).getTime()));
		} else if (value instanceof Boolean) {
			ps.setBoolean(paramIndex, (Boolean) value);
		} else if (value instanceof Byte) {
			ps.setByte(paramIndex, (Byte) value);
		} else {
			ps.setNull(paramIndex, Types.OTHER);
		}
	}

	public void setParamValue(String columnName, Object value) {
		paramValues.put(columnName, value);
	}

	public int getCountChangedDatasets() {
		return countChangedDatasets;
	}

	public String getCreateTableStatement() {
		return createTableStatement;
	}

	public String getCreateHistTableStatement() {
		return createHistTableStatement;
	}

	public String getAlterTableStatement() {
		return alterTableStatementAddVersioning;
	}

	public String getMergeStatement() {
		return mergeStatement;
	}

	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}

	public String getSourceSchema() {
		return sourceSchema;
	}

	public void setSourceSchema(String sourceSchema) {
		this.sourceSchema = sourceSchema;
	}

	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public String getTargetSchema() {
		return targetSchema;
	}

	public void setTargetSchema(String targetSchema) {
		this.targetSchema = targetSchema;
	}

	public void setBusinessTimePeriodStartColumn(String name) {
		businessTimePeriodStartColumn = name;
	}

	public void setBusinessTimePeriodEndColumn(String name) {
		businessTimePeriodEndColumn = name;
	}

	public void setSystemTimePeriodStartColumn(String name) {
		systemTimePeriodStartColumn = name;
	}

	public void setSystemTimePeriodEndColumn(String name) {
		systemTimePeriodEndColumn = name;
	}

	public String getTargetTableSpace() {
		return targetTableSpace;
	}

	public void setTargetTableSpace(String targetTableSpace) {
		this.targetTableSpace = targetTableSpace;
	}

	public boolean isBusinessTimeIsDay() {
		return businessTimeIsDay;
	}

	public void setBusinessTimeIsDay(boolean businessTimeIsDay) {
		this.businessTimeIsDay = businessTimeIsDay;
	}

	public String getSourceWhereCondition() {
		return sourceWhereCondition;
	}

	public void setSourceWhereCondition(String sourceWhereCondition) {
		this.sourceWhereCondition = sourceWhereCondition;
	}

	public static boolean isEmpty(String s) {
		return s == null || s.trim().isEmpty();
	}

	public String getScdEndDateStr() {
		return scdEndDateStr;
	}

	public void setScdEndDate(String scdEndDateStr) {
		if (scdEndDateStr != null) {
			this.scdEndDateStr = scdEndDateStr;
		}
	}

	public void setScdEndDate(Date scdEndDate) {
		if (scdEndDate != null) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			this.scdEndDateStr = "'" + sdf.format(scdEndDate) + "'";
		}
	}

	public String getBusinessTimeStartValue() {
		return businessTimeStartValue;
	}

	public void setBusinessTimeStartValue(String businessTimeStartValueStr) {
		this.businessTimeStartValue = businessTimeStartValueStr;
	}

	public String getBusinessTimeEndValue() {
		return businessTimeEndValue;
	}

	public void setBusinessTimeEndValue(String businessTimeEndValueStr) {
		this.businessTimeEndValue = businessTimeEndValueStr;
	}
	
	private boolean isNotInAdditionalColumns(String name) {
		name = name.toLowerCase();
		if (useBusinessTime) {
			if (name.equals(businessTimePeriodStartColumn.toLowerCase())) {
				return false;
			} else if (name.equals(businessTimePeriodEndColumn.toLowerCase())) {
				return false;
			}
		}
		if (name.equals(systemTimePeriodStartColumn.toLowerCase())) {
			return false;
		} else if (name.equals(systemTimePeriodEndColumn.toLowerCase())) {
			return false;
		} else if (name.equals(getTransStartColumn().toLowerCase())) {
			return false;
		} else {
			return true;
		}
	}

	public void addSourceColumn(String name, String type, Integer length, Integer precision, boolean nullable, boolean isKey, boolean trackChanges) {
		if (isNotInAdditionalColumns(name)) {
			Column col = new Column();
			if (isEmpty(name)) {
				throw new IllegalArgumentException("name cannot be null or empty");
			}
			if (isEmpty(type)) {
				throw new IllegalArgumentException("type cannot be null or empty");
			}
			col.setName(name);
			col.setDbType(type);
			col.setLength(length);
			col.setPrecision(precision);
			col.setNotNullable(nullable == false);
			col.setPartOfSourceKey(isKey);
			col.setTrackChanges(trackChanges);
			int pos = listSourceFields.indexOf(col);
			if (pos != -1) {
				listSourceFields.remove(pos);
			}
			listSourceFields.add(col);
		}
	}

	public Column addAdditionalColumn(String name, String type, Integer length, Integer precision, boolean nullable, boolean forInsert, boolean forUpdate) {
		if (isEmpty(name)) {
			throw new IllegalArgumentException("name cannot be null or empty");
		}
		if (isEmpty(type)) {
			throw new IllegalArgumentException("type cannot be null or empty");
		}
		Column col = new Column();
		col.setNotInSource(true);
		col.setPreparedParam(true);
		col.setName(name);
		col.setDbType(type);
		col.setLength(length);
		col.setPrecision(precision);
		col.setForInsert(forInsert);
		col.setForUpdate(forUpdate);
		int pos = listSourceFields.indexOf(col);
		if (pos != -1) {
			listSourceFields.remove(pos);
		}
		listSourceFields.add(col);
		return col;
	}
	
	public String getCreateTablespaceStatement() {
		return createTablespaceStatement;
	}

	private String createTableStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("create table ");
		sb.append(getTargetSchemaTable());
		sb.append(" (\n");
		// create transaction start time stamp column
		sb.append(INDENT); // 4
		sb.append(getTransStartColumn());
		sb.append(" TIMESTAMP(12)");
		sb.append(",\n");
		if (useBusinessTime) {
			// create business period columns
			sb.append(INDENT); // 4
			sb.append(businessTimePeriodStartColumn);
			if (businessTimeIsDay) {
				sb.append(" DATE NOT NULL");
			} else {
				sb.append(" TIMESTAMP NOT NULL");
			}
			sb.append(",\n");
			sb.append(INDENT); // 4
			sb.append(businessTimePeriodEndColumn);
			if (businessTimeIsDay) {
				sb.append(" DATE NOT NULL");
			} else {
				sb.append(" TIMESTAMP NOT NULL");
			}
			sb.append(",\n");
		}
		// create system period columns
		sb.append(INDENT); // 4
		sb.append(systemTimePeriodStartColumn);
		sb.append(" TIMESTAMP(12) NOT NULL");
		sb.append(",\n");
		sb.append(INDENT); // 4
		sb.append(systemTimePeriodEndColumn);
		sb.append(" TIMESTAMP(12) NOT NULL");
		sb.append(",\n");
		// none source columns (management columns etc)
		boolean firstLoop = true;
		for (Column col : listSourceFields) {
			if (col.isNotInSource()) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					sb.append(",\n");
				}
				sb.append(INDENT); // 4
				sb.append(createColumnCode(col));
			}
		}
		// source columns
		for (Column col : listSourceFields) {
			if (col.isNotInSource() == false) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					sb.append(",\n");
				}
				sb.append(INDENT); // 4
				sb.append(createColumnCode(col));
			}
		}
		// close the field and constraint list
		sb.append("\n)");
		sb.append(getInTableSpaceClause());
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- create base table");
			System.out.println(sql + ";");
		}
		return sql;
	}
	
	private String getInTableSpaceClause() {
		if (targetTableSpace != null && targetTableSpace.isEmpty() == false) {
			StringBuilder sb = new StringBuilder();
			sb.append("\nin ");
			sb.append(targetTableSpace);
			return sb.toString();
		} else {
			return "";
		}
	}

	private String createColumnCode(Column column) {
		StringBuilder sb = new StringBuilder();
		sb.append(column.getName());
		sb.append(" ");
		sb.append(column.getDbType());
		String type = column.getDbType().toLowerCase();
		if (type.contains("int") || type.contains("date") || type.contains("timestamp")) {
			// add no length and precision
		    if (type.contains("timestamp") && column.getLength() == 12) {
		    	// except for the high precision timestamp(12)
		    	sb.append("(");
		    	sb.append(column.getLength());
		    	sb.append(")");
		    }
			if (column.isAutoIncrement()) {
				sb.append(" NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1)");
			} else if (column.isNotNullable() || column.isPartOfSourceKey()) {
				sb.append(" NOT NULL");
			}
		} else {
			if (type.contains("char")) {
				// add only length
				sb.append("(");
				sb.append(column.getLength());
				sb.append(")");
			} else {
				sb.append("(");
				sb.append(column.getLength());
				sb.append(",");
				sb.append(column.getPrecision());
				sb.append(")");
			}
			if (column.isNotNullable()) {
				sb.append(" NOT NULL");
			}
		}
		return sb.toString();
	}
	
	private String createIndexPkStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("create index ");
		sb.append(getTargetSchemaTable());
		sb.append("_src_pk on ");
		sb.append(getTargetSchemaTable());
		sb.append("(");
		sb.append(getSourceKeyFieldList());
		sb.append(")");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- create index for source key");
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String getSourceKeyFieldList() {
		StringBuilder sb = new StringBuilder();
		boolean firstLoop = true;
		for (Column col : listSourceFields) {
			if (col.isPartOfSourceKey()) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					sb.append(",");
				}
				sb.append(col.getName());
			}
		}
		return sb.toString();
	}
	
	private String createAlterTableAlterTransStart1Statement() {
		StringBuilder sb = new StringBuilder();
		sb.append("alter table ");
		sb.append(getTargetSchemaTable());
		sb.append(" alter column ");
		sb.append(getTransStartColumn());
		sb.append(" set GENERATED ALWAYS AS TRANSACTION START ID");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- setup transaction start column");
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String createAlterTableAlterTransStart2Statement() {
		StringBuilder sb = new StringBuilder();
		sb.append("alter table ");
		sb.append(getTargetSchemaTable());
		sb.append(" alter column ");
		sb.append(getTransStartColumn());
		sb.append(" set IMPLICITLY HIDDEN");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- set transaction start column as hidden");
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String createAlterTableAddPeriodBusinessTimeStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("alter table ");
		sb.append(getTargetSchemaTable());
		sb.append(" add PERIOD BUSINESS_TIME (");
		sb.append(businessTimePeriodStartColumn);
		sb.append(",");
		sb.append(businessTimePeriodEndColumn);
		sb.append(")");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- add period business_time");
			System.out.println(sql + ";");
		}
		return sql;
	}
	
	private String createAlterTableAlterSystemBeginStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("alter table ");
		sb.append(getTargetSchemaTable());
		sb.append(" alter column ");
		sb.append(systemTimePeriodStartColumn);
		sb.append(" set GENERATED ALWAYS AS ROW BEGIN");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- setup system_time start column");
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String createAlterTableAlterSystemEndStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("alter table ");
		sb.append(getTargetSchemaTable());
		sb.append(" alter column ");
		sb.append(systemTimePeriodEndColumn);
		sb.append(" set GENERATED ALWAYS AS ROW END");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- setup system_time end column");
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String createAlterTableAddPeriodSystemTimeStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("alter table ");
		sb.append(getTargetSchemaTable());
		sb.append(" add PERIOD SYSTEM_TIME (");
		sb.append(systemTimePeriodStartColumn);
		sb.append(",");
		sb.append(systemTimePeriodEndColumn);
		sb.append(")");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- add period system_time");
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String createAlterTableAddPkStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("alter table ");
		sb.append(getTargetSchemaTable());
		sb.append(" add constraint ");
		sb.append(getTargetTable());
		sb.append("_pk primary key (");
		// add source key fields
		sb.append(getSourceKeyFieldList());
		if (useBusinessTime) {
			// add business time period
			sb.append(", BUSINESS_TIME WITHOUT OVERLAPS)");
		} else {
			sb.append(")");
		}
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- add primary key");
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String createIndexBusTimeperiodStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("create index ");
		sb.append(getTargetSchemaTable());
		sb.append("_bustp on ");
		sb.append(getTargetSchemaTable());
		sb.append("(");
		sb.append(businessTimePeriodStartColumn);
		sb.append(",");
		sb.append(businessTimePeriodEndColumn);
		sb.append(")");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- create index for business time period");
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String createIndexSysTimeperiodStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("create index ");
		sb.append(getTargetSchemaTable());
		sb.append("_systp on ");
		sb.append(getTargetSchemaTable());
		sb.append("(");
		sb.append(systemTimePeriodStartColumn);
		sb.append(",");
		sb.append(systemTimePeriodEndColumn);
		sb.append(")");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- create index for system time period");
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String createHistTableStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("create table ");
		sb.append(getTargetSchemaTableHist());
		sb.append(" like ");
		sb.append(getTargetSchemaTable());
		if (targetTableSpace != null && targetTableSpace.isEmpty() == false) {
			sb.append(" in ");
			sb.append(targetTableSpace);
		}
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- create history table");
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String getTargetSchemaTable() {
		StringBuilder sb = new StringBuilder();
		if (isEmpty(targetSchema) == false) {
			sb.append(targetSchema);
			sb.append(".");
		}
		sb.append(targetTable);
		return sb.toString();
	}

	private String getTargetSchemaTableHist() {
		StringBuilder sb = new StringBuilder();
		if (isEmpty(targetSchema) == false) {
			sb.append(targetSchema);
			sb.append(".");
		}
		sb.append(getHistTableName());
		return sb.toString();
	}

	private String getSourceSchemaTable() {
		StringBuilder sb = new StringBuilder();
		if (isEmpty(sourceSchema) == false) {
			sb.append(sourceSchema);
			sb.append(".");
		}
		sb.append(sourceTable);
		return sb.toString();
	}

	private String createAlterTableHistStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("alter table ");
		sb.append(getTargetSchemaTable());
		sb.append(" ADD VERSIONING USE HISTORY TABLE ");
		sb.append(getTargetSchemaTableHist());
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- switch on versioning");
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String createAlterTableHistAppendOnStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("alter table ");
		sb.append(getTargetSchemaTableHist());
		sb.append(" append on");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- set history table append on");
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String createAlterTableDropVersioningStatement() {
		StringBuilder sb = new StringBuilder();
		sb.append("alter table ");
		sb.append(getTargetSchemaTable());
		sb.append(" DROP VERSIONING");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- switch off versioning");
			System.out.println(sql + ";");
		}
		return sql;
	}
	
	private static class ParamIndex {
		private int index = 0;
		
		public int next() {
			return ++index;
		}
	}
	
	private void buildMergeStatementHeader(StringBuilder sb, ParamIndex paramIndex) {
		sb.append("merge into ");
		sb.append(getTargetSchemaTable());
		sb.append(" t \nusing (\n");
		sb.append(INDENT);
		sb.append("select * from ");
		sb.append(getSourceSchemaTable());
		if (isEmpty(sourceWhereCondition) == false) {
			sb.append(" where ");
			sb.append(sourceWhereCondition);
		}
		sb.append("\n) s\n");
		sb.append("on (");
		boolean firstLoop = true;
		for (Column c : listSourceFields) {
			if (c.isPartOfSourceKey()) {
				if (firstLoop) {
					firstLoop = false;
					sb.append("\n");
					sb.append(INDENT);
				} else {
					sb.append(" and\n");
					sb.append(INDENT);
				}
				sb.append("t.");
				sb.append(c.getName());
				sb.append(" = s.");
				sb.append(c.getName());
			}
		}
		if (useBusinessTime) {
			sb.append(" and\n");
			sb.append(INDENT);
			sb.append("t.");
			sb.append(businessTimePeriodEndColumn);
			sb.append(" > ");
			if (isEmpty(businessTimeStartValue)) {
				sb.append("?/*#");
				preparedParamList.add("businessTimeStartValue");
				sb.append(paramIndex.next());
				sb.append(" business_time_start */");
			} else {
				sb.append(businessTimeStartValue);
			}
		}
		if (firstLoop) {
			throw new IllegalStateException("No source key columns defined!");
		}
		sb.append("\n)\n");
	}
	
	private void buildMergeStatementInsert(StringBuilder sb, ParamIndex paramIndex) {
		sb.append("when not matched ");
		if (deleteCondition != null) {
			sb.append(" and not (");
			sb.append(deleteCondition);
			sb.append(") ");
		}
		sb.append("then\n");
		sb.append("insert (\n");
		if (useBusinessTime) {
			sb.append("  ");
			sb.append(businessTimePeriodStartColumn);
			sb.append(",\n");
			sb.append("  ");
			sb.append(businessTimePeriodEndColumn);
			sb.append(",\n");
		}
		boolean firstLoop = true;
		for (Column col : listSourceFields) {
			if (col.isAutoIncrement() == false && col.isForInsert()) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					sb.append(",\n");
				}
				sb.append("  ");
				sb.append(col.getName());
			}
		}
		sb.append(")\nvalues (\n");
		if (useBusinessTime) {
			if (isEmpty(businessTimeStartValue)) {
				sb.append("  ?/*#");
				preparedParamList.add("businessTimeStartValue");
				sb.append(paramIndex.next());
				sb.append(" business_time_start */");
			} else {
				sb.append(businessTimeStartValue);
			}
			sb.append(",\n");
			sb.append("  ");
			sb.append(scdEndDateStr);
			sb.append(",\n");
		}
		firstLoop = true;
		for (Column col : listSourceFields) {
			if (col.isAutoIncrement() == false && col.isForInsert()) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					sb.append(",\n");
				}
				if (col.isPreparedParam()) {
					sb.append("  ?/*#");
					preparedParamList.add(col.getName());
					sb.append(paramIndex.next());
					sb.append(" ");
					sb.append(col.getName());
					sb.append("*/");
				} else {
					sb.append("  s.");
					sb.append(col.getName());
				}
			}
		}
		sb.append("\n)\n");
	}
	
	private void buildMergeStatementUpdate(StringBuilder sb, ParamIndex paramIndex) {
		sb.append("when matched");
		boolean firstLoop = true;
		for (Column col : listSourceFields) {
			if (col.isTrackChanges() && col.isPartOfSourceKey() == false) {
				if (firstLoop) {
					firstLoop = false;
					sb.append(" and (\n    ");
				} else {
					sb.append("\n    or ");
				}
				sb.append("(t.");
				sb.append(col.getName());
				sb.append(" <> s.");
				sb.append(col.getName());
				sb.append(")");
			}
		}
		if (firstLoop == false) {
			sb.append("\n)");
		}
		if (deleteCondition != null) {
			sb.append(" and not (");
			sb.append(deleteCondition);
			sb.append(") ");
		}
		sb.append(" then\n");
		sb.append("update");
		if (useBusinessTime) {
			sb.append("\n  for portion of business_time\n");
			sb.append("      from ");
			if (isEmpty(businessTimeStartValue)) {
				sb.append("?/*#");
				preparedParamList.add("businessTimeStartValue");
				sb.append(paramIndex.next());
				sb.append(" business_time_start */");
			} else {
				sb.append(businessTimeStartValue);
			}
			sb.append("\n      to ");
			if (isEmpty(businessTimeEndValue)) {
				sb.append("?/*#");
				preparedParamList.add("businessTimeEndValue");
				sb.append(paramIndex.next());
				sb.append(" business_time_end */");
			} else {
				sb.append(businessTimeEndValue);
			}
		}
		sb.append("\nset ");
		firstLoop = true;
		for (Column col : listSourceFields) {
			if (col.isPartOfSourceKey() == false
					&& col.isAutoIncrement() == false && col.isForUpdate()) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					sb.append(",\n");
					sb.append(INDENT);
				}
				if (col.isPreparedParam()) {
					sb.append("t.");
					sb.append(col.getName());
					sb.append(" = ?/*#");
					preparedParamList.add(col.getName());
					sb.append(paramIndex.next());
					sb.append(" ");
					sb.append(col.getName());
					sb.append("*/");
				} else {
					sb.append("t.");
					sb.append(col.getName());
					sb.append(" = s.");
					sb.append(col.getName());
				}
			}
		}
		sb.append("\n");
	}
	
	private void buildMergeStatementDelete(StringBuilder sb, ParamIndex paramIndex) {
		sb.append("when matched ");
		sb.append("and (");
		sb.append(deleteCondition);
		sb.append(") ");
		sb.append("then\n");
		sb.append("delete ");
		if (useBusinessTime) {
			sb.append("\n  for portion of business_time\n");
			sb.append("      from ");
			if (isEmpty(businessTimeStartValue)) {
				sb.append("?/*#");
				preparedParamList.add("businessTimeStartValue");
				sb.append(paramIndex.next());
				sb.append(" business_time_start */");
			} else {
				sb.append(businessTimeStartValue);
			}
			sb.append("\n      to ");
			if (isEmpty(businessTimeEndValue)) {
				sb.append("?/*#");
				preparedParamList.add("businessTimeEndValue");
				sb.append(paramIndex.next());
				sb.append(" business_time_end */");
			} else {
				sb.append(businessTimeEndValue);
			}
		}
	}

	private String createMergeStatement() {
		StringBuilder sb = new StringBuilder();
		ParamIndex paramIndex = new ParamIndex();
		buildMergeStatementHeader(sb, paramIndex);
		// insert statement
		buildMergeStatementInsert(sb, paramIndex);
		// update statement
		buildMergeStatementUpdate(sb, paramIndex);
		// delete statement (only if we have an delete condition)
		if (deleteCondition != null) {
			buildMergeStatementDelete(sb, paramIndex);
		}
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- merge");
			System.out.println(sql + ";");
		}
		return sql;
	}

	public void setBusinessTimeStartValue(Date start) {
		paramValues.put("businessTimeStartValue", start);
	}

	public void setBusinessTimeEndValue(Date end) {
		paramValues.put("businessTimeEndValue", end);
	}

	public boolean existsTargetTable() throws Exception {
		checkConnection();
		DatabaseMetaData dbmd = connection.getMetaData();
		ResultSet rs = dbmd.getTables(null, targetSchema.toUpperCase(), targetTable.toUpperCase(), null);
		boolean exists = false;
		if (rs != null && rs.next()) {
			exists = true;
		}
		rs.close();
		return exists;
	}

	public boolean isUseBusinessTime() {
		return useBusinessTime;
	}

	public void setUseBusinessTime(boolean useBusinessTime) {
		this.useBusinessTime = useBusinessTime;
	}
	
	private String getTransStartColumn() {
		return transStartColumn;
	}

	public void setTransStartColumn(String transStartColumn) {
		if (isEmpty(transStartColumn) == false) {
			this.transStartColumn = transStartColumn.trim();
		}
	}

	public String getHistTableName() {
		if (isEmpty(histTableName) == false) {
			return histTableName;
		} else {
			return targetTable + "_HIST";
		}
	}

	public void setHistTableName(String histTableName) {
		if (isEmpty(histTableName) == false) {
			this.histTableName = histTableName;
		}
	}

	public boolean isBuildTargetTableWithPk() {
		return buildTargetTableWithPk;
	}

	public void setBuildTargetTableWithPk(boolean buildTargetTableWithPk) {
		this.buildTargetTableWithPk = buildTargetTableWithPk;
	}

	public boolean isDoNotExecuteMode() {
		return doNotExecute;
	}

	public void doNotExecuteMode(boolean readOnly) {
		this.doNotExecute = readOnly;
	}
	
	private void logStatement(String statement, String comment) {
		if (enableLogStatements) {
			if (statementsLogFilePath != null) {
				boolean newLogSection = false;
				if (statementLogFile == null) {
					statementLogFile = new File(statementsLogFilePath);
					newLogSection = true;
				}
				File dir = statementLogFile.getParentFile();
				if (dir != null && dir.exists() == false) {
					dir.mkdirs();
				}
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				try {
					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(statementLogFile, true), "UTF-8"));
					if (newLogSection) {
						if (logSectionComent != null) {
							bw.write("-- ########### " + logSectionComent + " #########\n");
							bw.write("-- ########### run at " + sdf.format(new Date()) + " ######### \n\n");
						} else {
							bw.write("-- ########### run at " + sdf.format(new Date()) + " ######### \n\n");
						}
					}
					bw.write(statement);
					bw.write(";\n\n");
					if (comment != null && comment.isEmpty() == false) {
						bw.write("/*\n");
						bw.write(comment);
						bw.write("\n*/\n");
					}
					bw.flush();
					bw.close();
				} catch (Exception e) {
					System.err.println(e.getMessage());
				}
			} else {
				System.out.println(statement + ";\n\n");
			}
		}
	}

	public boolean isEnableLogStatements() {
		return enableLogStatements;
	}

	public void setEnableLogStatements(boolean enableLogStatements) {
		this.enableLogStatements = enableLogStatements;
	}

	public String getStatementsLogFile() {
		return statementsLogFilePath;
	}

	public void setStatementsLogFile(String statementsLogFile, String comment) {
		if (isEmpty(statementsLogFile) == false) {
			this.statementsLogFilePath = statementsLogFile;
		}
		if (isEmpty(comment) == false) {
			this.logSectionComent = comment.replace("\n", " ");
		}
	}

	public String getDeleteCondition() {
		return deleteCondition;
	}

	public void setDeleteCondition(String deleteCondition) {
		if (deleteCondition != null && deleteCondition.trim().isEmpty() == false) {
			this.deleteCondition = deleteCondition;
		} else {
			this.deleteCondition = null;
		}
	}

}
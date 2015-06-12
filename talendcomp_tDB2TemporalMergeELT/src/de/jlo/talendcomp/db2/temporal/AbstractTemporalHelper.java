package de.jlo.talendcomp.db2.temporal;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

public abstract class AbstractTemporalHelper {

	protected Connection connection;
	protected boolean useInternalConnection = true;
	protected boolean debug = false;
	protected String alterTableStatementAddVersioning = null;
	protected String alterTableDropVersionStatement = null;
	protected String sourceTable;
	protected String sourceSchema;
	protected String sourceWhereCondition;
	protected boolean applySourceWhereCondToTarget = false;
	protected String targetTableSpace;
	protected String targetTable;
	protected String targetSchema;
	protected String histTableName = null;
	private boolean enableLogStatements = false;
	private String statementsLogFilePath = null;
	private File statementLogFile = null;
	private String logSectionComent = null;
	protected boolean doNotExecute = false;
	protected String businessTimeStartValue;
	protected String businessTimeEndValue;
	protected boolean businessTimeIsDay;
	protected String scdEndDateStr = "'9999-12-30'";
	protected static final String INDENT = "    ";
	protected List<String> preparedParamList = new ArrayList<String>();
	protected Map<String, Object> paramValues = new HashMap<String, Object>();
	protected List<Column> listSourceFields = new ArrayList<Column>();
	protected SimpleDateFormat sdfDate = null;
	protected SimpleDateFormat sdfTimestamp = null;
	protected NumberFormat nf = null;

	protected void initFormatter() {
		nf = NumberFormat.getInstance(Locale.ENGLISH);
		nf.setGroupingUsed(false);
		sdfDate = new SimpleDateFormat("yyyy-MM-dd");
		sdfTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
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

	public static boolean isEmpty(String s) {
		return s == null || s.trim().isEmpty();
	}

	protected void checkConnection() throws Exception {
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

	public void setConnection(Connection connection) {
		if (connection != null) {
			useInternalConnection = false;
			this.connection = connection;
		}
	}
	
	public void commit() throws Exception {
		connection.commit();
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
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
				// means, the table is already in history mode
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

	protected String createAlterTableDropVersioningStatement() {
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
	
	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTable) {
		if (isEmpty(sourceTable)) {
			throw new IllegalArgumentException("soirce table name cannot be empty or null");
		}
		this.sourceTable = sourceTable;
	}

	public String getSourceSchema() {
		return sourceSchema;
	}

	public String getSourceWhereCondition() {
		return sourceWhereCondition;
	}

	public void setSourceWhereCondition(String sourceWhereCondition) {
		if (isEmpty(sourceWhereCondition) == false) {
			sourceWhereCondition = sourceWhereCondition.trim();
			if (sourceWhereCondition.toLowerCase().startsWith("where")) {
				sourceWhereCondition = sourceWhereCondition.substring("where".length());
			}
			this.sourceWhereCondition = sourceWhereCondition;
		}
	}

	public void setSourceSchema(String sourceSchema) {
		if (isEmpty(sourceSchema) == false) {
			this.sourceSchema = sourceSchema;
		}
	}

	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		if (isEmpty(targetTable)) {
			throw new IllegalArgumentException("target table name cannot be empty or null");
		}
		this.targetTable = targetTable;
	}

	public String getTargetSchema() {
		return targetSchema;
	}

	public void setTargetSchema(String targetSchema) {
		if (isEmpty(targetSchema) == false) {
			this.targetSchema = targetSchema;
		}
	}

	protected String getTargetSchemaTable() {
		StringBuilder sb = new StringBuilder();
		if (isEmpty(targetSchema) == false) {
			sb.append(targetSchema);
			sb.append(".");
		}
		sb.append(targetTable);
		return sb.toString();
	}

	protected String getTargetSchemaTableHist() {
		StringBuilder sb = new StringBuilder();
		if (isEmpty(targetSchema) == false) {
			sb.append(targetSchema);
			sb.append(".");
		}
		sb.append(getHistTableName());
		return sb.toString();
	}

	protected String getSourceSchemaTable() {
		StringBuilder sb = new StringBuilder();
		if (isEmpty(sourceSchema) == false) {
			sb.append(sourceSchema);
			sb.append(".");
		}
		sb.append(sourceTable);
		return sb.toString();
	}

	protected String getHistTableName() {
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

	protected void executeStatement(String statement) throws Exception {
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
	
	protected void logStatement(String statement, String comment) {
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

	public boolean isDoNotExecuteMode() {
		return doNotExecute;
	}

	public void doNotExecuteMode(boolean readOnly) {
		this.doNotExecute = readOnly;
	}
	
	public String getBusinessTimeStartValue() {
		return businessTimeStartValue;
	}

	public void setBusinessTimeStartValue(String businessTimeStartValueStr) {
		if (isEmpty(businessTimeStartValueStr)) {
			throw new IllegalArgumentException("business time period start value string cannot be null or empty");
		}
		this.businessTimeStartValue = businessTimeStartValueStr;
	}

	public String getBusinessTimeEndValue() {
		return businessTimeEndValue;
	}

	public void setBusinessTimeEndValue(String businessTimeEndValueStr) {
		if (isEmpty(businessTimeEndValueStr)) {
			throw new IllegalArgumentException("business time period end value string cannot be null or empty");
		}
		this.businessTimeEndValue = businessTimeEndValueStr;
	}

	public void setBusinessTimeStartValue(Date start) {
		if (businessTimeIsDay) {
			paramValues.put("businessTimeStartValue", start);
		} else {
			paramValues.put("businessTimeStartValue", new Timestamp(start.getTime()));
		}
	}

	public void setBusinessTimeEndValue(Date end) {
		if (businessTimeIsDay) {
			paramValues.put("businessTimeEndValue", end);
		} else {
			paramValues.put("businessTimeEndValue", new Timestamp(end.getTime()));
		}
	}

	protected boolean isNotInAdditionalColumns(String name) {
		return true;
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

	protected static class ParamIndex {
		private int index = 0;
		
		public int next() {
			return ++index;
		}
	}

	protected void setParamValue(
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

	public void setBusinessTimeIsDay(boolean businessTimeIsDay) {
		this.businessTimeIsDay = businessTimeIsDay;
	}

	protected String toSQLString(Object o) {
		if (o instanceof Timestamp) {
			return "'" + sdfTimestamp.format((Timestamp) o) + "'";
		} else if (o instanceof Date) {
			return "'" + sdfDate.format((Date) o) + "'";
		} else if (o instanceof Number) {
			return nf.format((Number) o);
		} else if (o instanceof String) {
			return "'" + ((String) o) + "'";
		} else if (o instanceof Boolean) {
			return ((Boolean) o) ? "true" : "false";
		} else if (o != null) {
			return o.toString();
		} else {
			return "null";
		}
	}

	public void setApplySourceWhereCondToTarget(boolean applySourceWhereCondToTarget) {
		this.applySourceWhereCondToTarget = applySourceWhereCondToTarget;
	}
	
}

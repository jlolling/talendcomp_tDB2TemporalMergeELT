package de.cimt.talendcomp.db2.temporal;

import java.sql.PreparedStatement;
import java.sql.Statement;

public class TemporalDelete extends AbstractTemporalHelper {
	
	private int countDeletedDatasets = 0;
	private String deleteStatement;

	public TemporalDelete() {
		initFormatter();
	}

	public int executeDelete() throws Exception {
		checkConnection();
		deleteStatement = createDeleteStatement();
		// after that all prepared statement parameters are retrieved
		if (preparedParamList.isEmpty()) {
			Statement stat = connection.createStatement();
			logStatement(deleteStatement, null);
			try {
				if (doNotExecute == false) {
					countDeletedDatasets = stat.executeUpdate(deleteStatement);
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
			PreparedStatement ps = connection.prepareStatement(deleteStatement);
			StringBuilder comment = new StringBuilder();
			for (int i = 0; i < preparedParamList.size(); i++) {
				Object value = paramValues.get(preparedParamList.get(i));
				String s = "Prepare param: #"
						+ (i+1)
						+ " "
						+ preparedParamList.get(i) + " = " + toSQLString(value) + " class:" + (value != null ? value.getClass().getName() : "");
				comment.append(s);
				comment.append("\n");
				if (debug) {
					System.out.println(s);
				}
				setParamValue(ps, value, i + 1);
			}
			logStatement(deleteStatement, comment.toString());
			try {
				if (doNotExecute == false) {
					countDeletedDatasets = ps.executeUpdate();
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
		return countDeletedDatasets;
	}

	private String createDeleteStatement() throws Exception {
		ParamIndex paramIndex = new ParamIndex();
		StringBuilder sb = new StringBuilder();
		sb.append("delete from ");
		sb.append(getTargetSchemaTable());
		sb.append("\n");
		sb.append(INDENT);
		sb.append("for portion of business_time\n");
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
		sb.append(" t \nwhere not exists (\n");
		sb.append(INDENT);
		sb.append("select * from ");
		sb.append(getSourceSchemaTable());
		sb.append(" s\n");
		sb.append(INDENT);
		sb.append("where ");
		if (isEmpty(sourceWhereCondition) == false) {
			sb.append("not (");
			sb.append(sourceWhereCondition);
			sb.append(")");
		}
		boolean firstLoop = true;
		if (listSourceFields.isEmpty()) {
			throw new Exception("No key column defined!");
		}
		for (Column c : listSourceFields) {
			if (c.isPartOfSourceKey()) {
				if (firstLoop) {
					firstLoop = false;
					if (isEmpty(sourceWhereCondition) == false) {
						sb.append(" and ");
					}
					sb.append("\n");
					sb.append(INDENT+INDENT);
				} else {
					sb.append(" and\n");
					sb.append(INDENT+INDENT);
				}
				sb.append("t.");
				sb.append(c.getName());
				sb.append(" = s.");
				sb.append(c.getName());
			}
		}
		sb.append("\n)");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- execute delete");
			System.out.println(sql + ";");
		}
		return sql;
	}
	
}

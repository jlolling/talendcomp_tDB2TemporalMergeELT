import java.util.Date;

import de.jlo.talendcomp.db2.temporal.TemporalDelete;
import de.jlo.talendcomp.db2.temporal.TemporalMerger;


public class TestCodeGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		testMerger();
	}
	
	public static void testDelete() {
		TemporalDelete g = new TemporalDelete();
		g.setDebug(true);
		//g.doNotExecuteMode(true);
        g.setSourceSchema("DB2INST2");
        g.setSourceTable("EMP");
        g.setTargetTable("EMPLOYEE");
        g.setTargetSchema("DWH_ODS");
        g.addSourceColumn("EMPNO", "CHAR", 6, 0, false, true, false);
        g.addSourceColumn("LASTNAME", "CHAR", 6, 0, false, true, false);
        g.setBusinessTimeIsDay(true);
        g.setBusinessTimeStartValue(new Date());
        g.setBusinessTimeEndValue("'9999-12-30'");
        g.setSourceWhereCondition("s.JOB is null");
        try {
			System.out.println("connecting...");
			g.connect("on-0337-jll.local", "50001", "SAMPLE", "db2inst2",
					"db2inst2", "retrieveMessagesFromServerOnGetMessage=true");
			System.out.println("connected");
			System.out.println("Deleted records:" + g.executeDelete());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void testMerger() {
		TemporalMerger g = new TemporalMerger();
		g.setBusinessTimeIsDay(true);
		g.setBusinessTimePeriodStartColumn("DWH_BUS_VALID_FROM");
		g.setBusinessTimePeriodEndColumn("DWH_BUS_VALID_TO");
		g.setSystemTimePeriodStartColumn("DWH_SYS_VALID_FROM");
		g.setSystemTimePeriodEndColumn("DWH_SYS_VALID_TO");
		g.setSourceSchema("DB2INST2");
		g.setSourceTable("EMP");
		g.setTargetTable("EMPPLOYEE");
		g.setTargetSchema("DWH_ODS");
		g.setBusinessTimeStartValue("'2014-01-01'");
		try {
			g.connect("on-0337-jll.local", "50001", "SAMPLE", "db2inst2",
					"db2inst2", "retrieveMessagesFromServerOnGetMessage=true");
			System.out.println("connected");
			if (g.existsTargetTable() == false) {
				g.executeCreateTable();
				System.out.println("table created");
				g.executeCreateHistTable();
				System.out.println("hist table created");
				g.executeAlterTableAddVersioning();
				System.out.println("hist table bound");
			} else {
				System.out.println("Table already exists");
			}
			System.out.println("Anzahl Saetze geschrieben:" + g.executeMerge());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

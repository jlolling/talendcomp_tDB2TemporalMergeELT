import java.util.Date;

import de.cimt.talendcomp.db2.temporal.TemporalMerger;


public class TestCodeGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TemporalMerger g = new TemporalMerger();
        g.setBusinessTimeIsDay(true);
        g.setBusinessTimePeriodStartColumn("DWH_BUS_VALID_FROM");
        g.setBusinessTimePeriodEndColumn("DWH_BUS_VALID_TO");
        g.setSystemTimePeriodStartColumn("DWH_SYS_VALID_FROM");
        g.setSystemTimePeriodEndColumn("DWH_SYS_VALID_TO");
        g.setSourceSchema("DB2INST2");
        g.setSourceTable("EMP");
        g.setTargetTable("EMP");
        g.setTargetSchema("DWH_ODS");
        g.setBusinessTimeStartValue(new Date());
        try {
              g.connect("on-0337-jll.local", "50001", "SAMPLE", "db2inst2", "db2inst2", "retrieveMessagesFromServerOnGetMessage=true");
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
        System.out.println(g.getCreateTableStatement());
        System.out.println(g.getCreateHistTableStatement());
        System.out.println(g.getAlterTableStatement());
        System.out.println(g.getMergeStatement());
	}

}

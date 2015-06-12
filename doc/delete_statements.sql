-- ########### test_db2delete:tDB2TemporalDeleteELT_1 #########
-- ########### run at 2015-01-15 19:45:30 ######### 

alter table DWH_ODS.EMPLOYEE ADD VERSIONING USE HISTORY TABLE DWH_ODS.EMPLOYEE_HIST;

delete from DWH_ODS.EMPLOYEE
    for portion of business_time
      from '2014-09-01'
      to '9999-12-30' t 
where not exists (
    select * from DB2INST2.EMPLOYEE s
    where  (SEX='M') and not (s.JOB is null) and
        t.EMPNO = s.EMPNO
) and (SEX='M');

alter table DWH_ODS.EMPLOYEE DROP VERSIONING;


-- ########### test_db2delete:tDB2TemporalMergeELT_1 #########
-- ########### run at 2015-01-15 19:45:30 ######### 

alter table DWH_ODS.EMPLOYEE ADD VERSIONING USE HISTORY TABLE DWH_ODS.EMPLOYEE_HIST;

merge into DWH_ODS.EMPLOYEE t 
using (
    select * from DB2INST2.EMPLOYEE where SEX='M'
) s
on (
    t.EMPNO = s.EMPNO and
    t.BUS_VALID_TO > '2014-09-01'
)
when not matched  and not (s.JOB is null) then
insert (
  BUS_VALID_FROM,
  BUS_VALID_TO,
  EMPNO,
  FIRSTNME,
  MIDINIT,
  LASTNAME,
  WORKDEPT,
  PHONENO,
  HIREDATE,
  JOB,
  EDLEVEL,
  SEX,
  BIRTHDATE,
  SALARY,
  BONUS,
  COMM)
values (
'2014-09-01',
  '9999-12-30',
  s.EMPNO,
  s.FIRSTNME,
  s.MIDINIT,
  s.LASTNAME,
  s.WORKDEPT,
  s.PHONENO,
  s.HIREDATE,
  s.JOB,
  s.EDLEVEL,
  s.SEX,
  s.BIRTHDATE,
  s.SALARY,
  s.BONUS,
  s.COMM
)
when matched and (
    (t.FIRSTNME <> s.FIRSTNME)
    or (t.MIDINIT <> s.MIDINIT)
    or (t.LASTNAME <> s.LASTNAME)
    or (t.WORKDEPT <> s.WORKDEPT)
    or (t.PHONENO <> s.PHONENO)
    or (t.HIREDATE <> s.HIREDATE)
    or (t.JOB <> s.JOB)
    or (t.EDLEVEL <> s.EDLEVEL)
    or (t.SEX <> s.SEX)
    or (t.BIRTHDATE <> s.BIRTHDATE)
    or (t.SALARY <> s.SALARY)
    or (t.BONUS <> s.BONUS)
    or (t.COMM <> s.COMM)
) and not (s.JOB is null)  then
update
  for portion of business_time
      from '2014-09-01'
      to '2014-09-01'
set t.FIRSTNME = s.FIRSTNME,
    t.MIDINIT = s.MIDINIT,
    t.LASTNAME = s.LASTNAME,
    t.WORKDEPT = s.WORKDEPT,
    t.PHONENO = s.PHONENO,
    t.HIREDATE = s.HIREDATE,
    t.JOB = s.JOB,
    t.EDLEVEL = s.EDLEVEL,
    t.SEX = s.SEX,
    t.BIRTHDATE = s.BIRTHDATE,
    t.SALARY = s.SALARY,
    t.BONUS = s.BONUS,
    t.COMM = s.COMM
when matched and (s.JOB is null) then
delete 
  for portion of business_time
      from '2014-09-01'
      to '2014-09-01';


create or replace procedure analyze_schema_proc is
 vtablename varchar2(30);
 v_username varchar2(255);

begin
  --进行表分析
  select USERNAME into v_username from user_users;
  DBMS_STATS.GATHER_SCHEMA_STATS(v_username,DBMS_STATS.AUTO_SAMPLE_SIZE);
end analyze_schema_proc;
/


create or replace procedure delete_schema_stat
is
 v_username varchar2(255);
begin
 --删除表分析
 select USERNAME into v_username
 from user_users;
 DBMS_STATS.DELETE_SCHEMA_STATS(v_username);

end  delete_schema_stat;
/
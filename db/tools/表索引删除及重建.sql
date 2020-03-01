create or replace procedure RECOVER_IND_TABLE_PROC(tablename varchar2) is
  v_tablename   varchar2(100);
  v_tmpstr   varchar2(100);
  v_uniqueness varchar2(100);
  v_indexname varchar2(100);
  v_columnname varchar2(100);
  i sys_refcursor;
  j sys_refcursor;
begin
  select upper(tablename) into v_tablename from dual;
  --重建索引
  open i for 'select distinct bk.index_name,bk.uniqueness from TMP_INDEX_BK bk where table_name='''||v_tablename||'''';
  while true
  Loop
   fetch i into v_indexname,v_uniqueness;
   Exit When i%Notfound;
   --处理多列索引的情况
    v_tmpstr:='';
    open j for 'select t.column_name from TMP_INDEX_BK t where table_name='''||v_tablename||''' and t.index_name='''||v_indexname||'''';
    while true
    loop
        fetch j into v_columnname;
        Exit When j%Notfound;
        v_tmpstr := v_tmpstr||v_columnname||',';
    end loop;
    close j;
    --去除最后一个逗号
    if v_tmpstr='' then
      return;
    else
      v_tmpstr := substr(v_tmpstr,0,length(v_tmpstr)-1);
    end if;
    
    if (v_uniqueness != 'UNIQUE') then
      v_uniqueness := '';
    end if;
    execute immediate 'create ' || v_uniqueness || ' index ' ||
                      v_indexname || ' on ' || v_tablename || '(' ||
                      v_tmpstr || ')';
      --清除索引备份信息
     execute immediate 'delete from TMP_INDEX_BK where table_name='''||v_tablename||''' and index_name='''||v_indexname||'''';
     commit;                
  end loop;
  close i;
  --重启约束
  for i in (select u.constraint_name
              from user_constraints u
             where TABLE_name = v_tablename) loop
    execute immediate 'alter table ' || v_tablename ||
                      ' enable constraint ' || i.constraint_name;
  end loop;  

end RECOVER_IND_TABLE_PROC;
/

create or replace procedure DROP_IND_TABLE_PROC(tablename varchar2) is
  v_tablename varchar2(100);
  v_count     number(5);
begin
  select upper(tablename) into v_tablename from dual;
  select count(1)
    into v_count
    from user_tables ut
   where ut.table_name = 'TMP_INDEX_BK';
  if (v_count = 0) then
    --建立备份表
    execute immediate 'create table TMP_INDEX_BK(index_name varchar2(100),column_name varchar2(100),uniqueness varchar2(20),table_name varchar2(100))';
    execute immediate ' create index idx_tmp_ind_tablename on tmp_index_bk(table_name)';
    execute immediate 'create index idx_tmp_ind_indexname on tmp_index_bk(index_name)';
  end if;
  --为了安全，先恢复备份表里已记录的索引信息 
  recover_ind_table_proc(v_tablename);
  --创建索引信息备份表
  execute immediate 'insert into TMP_INDEX_BK(index_name,table_name,column_name,uniqueness)' ||
                    ' select i.index_name,i.table_name,c.column_name,i.uniqueness from user_indexes i,user_ind_columns c where i.index_name=c.index_name and i.table_name=''' ||
                    v_tablename || ''''; 
  commit;
  --禁用约束
  for i in (select u.constraint_name
              from user_constraints u
             where TABLE_name = v_tablename) loop
    execute immediate 'alter table ' || v_tablename ||
                      ' disable constraint ' || i.constraint_name;
  end loop;
  --drop索引
  for ind in (select distinct i.index_name
                from user_indexes i, user_ind_columns c
               where i.index_name = c.index_name
                 and i.table_name = v_tablename) loop
    execute immediate 'drop index ' || ind.index_name;
  end loop;
end DROP_IND_TABLE_PROC;
/

create or replace procedure DROP_IND_DQC_NM_TABLES_PROC is
begin
  --删除所有表名为DQC_NM开头的索引
  for t in (select table_name from user_tables where table_name like 'DQC_NM%')
  loop
      drop_ind_table_proc(t.table_name);
  end loop;
end DROP_IND_DQC_NM_TABLES_PROC;
/

create or replace procedure RECOVER_ALL_IND_TABLES_PROC is
begin
  --恢复所有临时表中的索引
  for t in (select distinct table_name from tmp_index_bk)
  loop
      recover_ind_table_proc(t.table_name);
  end loop;
end RECOVER_ALL_IND_TABLES_PROC;
/
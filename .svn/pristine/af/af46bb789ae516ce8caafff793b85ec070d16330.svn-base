--1.创建目录utl_dir用于存放导出的创建脚本
create or replace directory utl_dir as '/home/lianzhi/lianzhi/';
--2.给所有用户赋予读写utl_dir的权限
grant write,read on directory utl_dir to public;
--3.创建存储过程，用来导出单个对象的创建脚本
create or replace procedure exportddl(username varchar2,p_type varchar2,p_name varchar2,p_filename varchar2) is
begin
  declare
    l_file         utl_file.file_type;
    l_buffer       varchar2(4000);
    l_amount    binary_integer := 1000;
    l_pos          integer := 1;
    l_clob         clob;
    l_clob_len     integer;
  begin
    --去掉定义中的用户名和 结尾的换行
    if upper(p_type) = 'PROCEDURE' then
       select replace(RTRIM( RTRIM(dbms_metadata.get_ddl(upper(p_type),p_name)) ,CHR(10)),'"'||username||'".','') 
          into l_clob from dual;
    else
       select replace(RTRIM( RTRIM(dbms_metadata.get_ddl(upper(p_type),p_name)) ,CHR(10)),'"'||username||'".','')||';' 
         into l_clob from dual;
    end if;

    l_clob_len := dbms_lob.getlength(l_clob);
    l_file := utl_file.fopen('UTL_DIR', p_filename||'.sql', 'a', 1000);

    while l_pos < l_clob_len loop
      dbms_lob.read(l_clob, l_amount, l_pos, l_buffer);
      utl_file.put(l_file, l_buffer);
      l_pos := l_pos + l_amount;
    end loop;
    --加入 /

    if upper(p_type) = 'PROCEDURE' then
       utl_file.put_line(l_file, CHR(10)||'/');
    end if;
    utl_file.fclose(l_file);
  end;
end exportddl;
/

--4.创建存储过程，用来导出所有对象（表、索引、视图、同义词）的创建脚本
create or replace procedure exportddl_all(username varchar2,p_filename varchar2) is
begin

  --for x in (select table_name from user_tables) loop
  --     exportddl('TABLE',x.table_name,p_filename);
  --end loop;

 -- for x in (select index_name from user_indexes) loop
  --     exportddl('INDEX',x.index_name,p_filename);
 -- end loop;
  --导出视图
  DBMS_OUTPUT.PUT_LINE('开始view');
  for x in (select view_name from user_views) loop
       exportddl(upper(username),'VIEW',x.view_name,p_filename);
  end loop;
  --导出存储过程
  for x in (select object_name from USER_PROCEDURES where object_name != 'GETLOCALREGIONID') loop
       exportddl(upper(username),'PROCEDURE',x.object_name,p_filename);
  end loop;

 -- for x in (select synonym_name from user_synonyms) loop
 --      exportddl('SYNONYM',x.synonym_name,p_filename);
 -- end loop;

end exportddl_all;
/

--5.使用过程导出scott用户所有对象的创建脚本
conn scott/tiger

exec exportddl_all('DQC','DQC_view_procedure');
exec exportddl_all('BAM','BAM_view_procedure');

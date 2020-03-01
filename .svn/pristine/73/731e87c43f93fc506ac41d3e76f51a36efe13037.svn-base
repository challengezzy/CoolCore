--1.同库不同用户之间建立同义词.使用同义词创建函数实现.
-- Create the user
create user DQCTEST identified by DQCTEST
  default tablespace SDH_DATA
  temporary tablespace TEMP
  profile DEFAULT;
--赋予相应权限
grant create session to DQCTEST;
grant create synonym to DQCTEST;
--赋予表查询权限
grant select any table to dqctest;
grant select any  view to dqctest
--创建同义词创建存储过程
create or replace procedure CreateSynonymForUser
 (SourceUser in varchar2,TarUser in varchar2) is
  iCount integer(8);
  sTableName varchar2(100);
  sSql varchar2(300);
  cursor c1(p1 varchar2) is select TABLE_NAME from dba_tables where OWNER= upper(SourceUser);
begin
  --表同义词
  open c1(SourceUser);
  loop
      sSql:= 'create or replace synonym ';
      fetch c1 into sTableName;
      exit when c1%notfound;
      sSql:= sSql || TarUser || '.' || sTableName || ' for ' || SourceUser || '.' || sTableName;
      DBMS_OUTPUT.PUT_LINE(sSql);
      EXECUTE IMMEDIATE sSql;
      iCount:=iCount + 1;
      DBMS_OUTPUT.PUT_LINE(iCount || ' synonyms created !');
  end loop;
end CreateSynonymForUser;


2.不同库用户间创建同义词.

(1)先从源库中导出需要创建同义词的表信息.使用SQL语句:

select TABLE_NAME from dba_tables where OWNER=[表所属用户名] and TABLESPACE_NAME='USERS';

(2)在即将创建同义词的库中创建临时表,如TempTable.只需要一列用来存放(1)步中导出的表信息,如:TableName.

(3)将(1)步中导出的单列数据放入(2)步中创建的临时表中.

(4)检查数据库链路,确保源库与目标库的正常.

(5)在目标库中创建如下同义词创建函数,然后填入适当参数运行即可.

create or replace function CreateSynonymForDbLink
(SourceUser in varchar2,--同义词指向的表所在用户
TarUser in varchar2,--指定同义词创建到的用户
dblink in varchar2,--同义词使用的数据库链路名称
sourceTabname in varchar2,--需创建同义词信息所在临时表
colName in varchar2--表信息所在列
) return integer is
  Result integer;
  V_SQL_SELECT VARCHAR2(400);
  iCount integer(8);
  sTableName varchar2(100);
  sSql varchar2(300);
  TYPE V_CURSOR IS REF CURSOR;
   c1 V_CURSOR;
  --cursor c1(p1 varchar2,p2 varchar2)
   --is select p2 from p1;
begin
  --open c1(sourceTabname,colName);
  V_SQL_SELECT:='select ' || colName || ' from ' || sourceTabname;
  open c1 for V_SQL_SELECT;
  loop
      sSql:= 'create or replace synonym ';
      fetch c1 into sTableName;
      exit when c1%notfound;
      sSql:= sSql || TarUser || '.' || sTableName || ' for ' || SourceUser || '.' || sTableName || '@' || dblink;
      EXECUTE IMMEDIATE sSql;
      iCount:=iCount + 1;
      Result:=iCount;
  end loop;
  return(Result);
end CreateSynonymForDbLink;

--ps:介于以下操作会涉及数据导入导出操作,强烈建议使用PL/SQL软件来实现.

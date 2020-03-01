create or replace procedure delBigTab(p_TableName in varchar2,
                                      p_Condition in varchar2,
                                      p_Count     in varchar) is
  pragma autonomous_transaction;
  n_delete number := 0;
begin
  --大批量数据删除，指定表名、条件和单次删除数据量
  while 1 = 1 loop
    EXECUTE IMMEDIATE 'delete from ' || p_TableName || ' where ' ||
                      p_Condition || ' and rownum <= :rn'
      USING p_Count;
    if SQL%NOTFOUND then
      exit;
    else
      n_delete := n_delete + SQL%ROWCOUNT;
    end if;
    commit;
  end loop;
  commit;
  DBMS_OUTPUT.PUT_LINE('Finished!');
  DBMS_OUTPUT.PUT_LINE('Totally ' || to_char(n_delete) ||
                       ' records deleted!');
end delBigTab;
/

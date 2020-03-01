CREATE OR REPLACE Function f_bizc_Ip2char(p_Id Varchar2) Return Varchar2 Is
	v_Ip Varchar2(20);
Begin
	v_Ip := Lpad(Substr(p_Id, 1, Instr(p_Id, '.', 1, 1) - 1), 3, '0') ||
					Lpad(Substr(p_Id, Instr(p_Id, '.', 1, 1) + 1, Instr(p_Id, '.', 1, 2) - Instr(p_Id, '.', 1, 1) - 1), 3, '0') ||
					Lpad(Substr(p_Id, Instr(p_Id, '.', 1, 2) + 1, Instr(p_Id, '.', 1, 3) - Instr(p_Id, '.', 1, 2) - 1), 3, '0') ||
					Lpad(Substr(p_Id, Instr(p_Id, '.', 1, 3) + 1, Length(p_Id) - Instr(p_Id, '.', 1, 3)), 3, '0');
	Return v_Ip;
End;
/
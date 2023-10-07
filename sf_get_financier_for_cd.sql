create or replace function muldms.sf_get_financier_for_cd(p_mul_dealer_cd in varchar2,
                                        p_for_cd        in varchar2,
                                        p_outlet_cd     in varchar2)
  return varchar2 is
  lv_for_cd varchar2(20);
  lv_parent_group varchar2(10);
begin
  begin
    select parent_group
      into lv_parent_group
      from am_dealer_loc am
     where am.mul_dealer_cd=p_mul_dealer_cd
       and am.for_cd=p_for_cd
       and am.outlet_cd=p_outlet_cd
       and rownum=1;
  exception
    when others then
      lv_parent_group:=null;
  end;
  if lv_for_cd is null then
    begin
      select am.for_cd
        into lv_for_cd
        from am_dealer_loc am
       where am.mul_dealer_cd=p_mul_dealer_cd
         and am.for_cd=p_for_cd
         and am.outlet_cd=p_outlet_cd
         and am.dealer_category='MAS'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
  if lv_for_cd is null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew
       where Dealer_cd = p_mul_dealer_cd
         and for_Cd = p_for_cd
         and outlet_Cd = p_outlet_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'VCF';
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
  if lv_for_cd is null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew
       where Dealer_cd = p_mul_dealer_cd
         and for_Cd = p_for_cd
         and outlet_Cd = p_outlet_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'DDL';
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
  if lv_for_cd is null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew
       where Dealer_cd = p_mul_dealer_cd
         and for_Cd = p_for_cd
         and outlet_Cd = p_outlet_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'DMM';
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
  if lv_for_cd is null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew
       where Dealer_cd = p_mul_dealer_cd
         and for_Cd = p_for_cd
         and outlet_Cd = p_outlet_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'MUL';
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
-------
  if lv_for_cd is null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew
       where Dealer_cd = p_mul_dealer_cd
         and for_Cd = p_for_cd
         --and outlet_Cd = p_outlet_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'VCF'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
  if lv_for_cd is null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew
       where Dealer_cd = p_mul_dealer_cd
         and for_Cd = p_for_cd
         --and outlet_Cd = p_outlet_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'DDL'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
  if lv_for_cd is null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew
       where Dealer_cd = p_mul_dealer_cd
         and for_Cd = p_for_cd
         --and outlet_Cd = p_outlet_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'DMM'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
  if lv_for_cd is null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew
       where Dealer_cd = p_mul_dealer_cd
         and for_Cd = p_for_cd
         --and outlet_Cd = p_outlet_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'MUL'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
-------
  if lv_for_cd is null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew
       where Dealer_cd = p_mul_dealer_cd
         --and for_Cd = p_for_cd
         --and outlet_Cd = p_outlet_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'VCF'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
  if lv_for_cd is null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew
       where Dealer_cd = p_mul_dealer_cd
         --and for_Cd = p_for_cd
         --and outlet_Cd = p_outlet_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'DDL'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
  if lv_for_cd is null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew
       where Dealer_cd = p_mul_dealer_cd
         --and for_Cd = p_for_cd
         --and outlet_Cd = p_outlet_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'DMM'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
  if lv_for_cd is null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew
       where Dealer_cd = p_mul_dealer_cd
         --and for_Cd = p_for_cd
         --and outlet_Cd = p_outlet_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'MUL'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;



  if lv_for_cd is null and lv_parent_group is not null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew, am_dealer_loc am
       where ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         and am.parent_group=lv_parent_group
         and am.for_Cd=p_for_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'VCF'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;

  if lv_for_cd is null and lv_parent_group is not null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew, am_dealer_loc am
       where ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         and am.parent_group=lv_parent_group
         and am.for_Cd=p_for_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'DDL'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;

  if lv_for_cd is null and lv_parent_group is not null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew, am_dealer_loc am
       where ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         and am.parent_group=lv_parent_group
         and am.for_Cd=p_for_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'DMM'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
  if lv_for_cd is null and lv_parent_group is not null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew, am_dealer_loc am
       where ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         and am.parent_group=lv_parent_group
         and am.for_Cd=p_for_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'MUL'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;


  if lv_for_cd is null and lv_parent_group is not null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew, am_dealer_loc am
       where ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         and am.parent_group=lv_parent_group
         --and am.for_Cd=p_for_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'VCF'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;

  if lv_for_cd is null and lv_parent_group is not null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew, am_dealer_loc am
       where ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         and am.parent_group=lv_parent_group
         --and am.for_Cd=p_for_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'DDL'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;

  if lv_for_cd is null and lv_parent_group is not null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew, am_dealer_loc am
       where ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         and am.parent_group=lv_parent_group
         --and am.for_Cd=p_for_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'DMM'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
  if lv_for_cd is null and lv_parent_group is not null then
    begin
      select ew.financier_for_cd
        into lv_for_cd
        from dmsd_ew_fin ew, am_dealer_loc am
       where ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         and am.parent_group=lv_parent_group
         --and am.for_Cd=p_for_cd
         and ason_date = trunc(sysdate - 1)
         and financier_delr_catg = 'MUL'
         and rownum=1;
    exception
      when no_data_found then
        lv_for_cd := null;
    end;
  end if;
-------
 return lv_for_cd;
exception
  when others then return null;
end sf_get_financier_for_cd;

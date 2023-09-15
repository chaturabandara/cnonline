
 CREATE FUNCTION [dbo].[pmz_ssrs_RollingSalesAnalysis_Accounting](@Period int,@BLDGID varchar(max),@stypid varchar(max),@sqftType varchar(10))
 RETURNS @Table1 Table
 (BLDGID varchar(10),Tenant varchar(250),UnitId varchar(50),StypId varchar(10),descrptn varchar(250),boma float,amount float,lastboma float,lastamount float,camount float,pamount float,
 cyamount float,pyamount float,iscomparable int,amountPSF float,lastamountpsf float,camountpsf float,pamountpsf float,
 ProAmount1 float,ProAmount2 float,ProAmount3 float,ProAmount4 float,ProAmount5 float,ProAmount6 float,ProAmount7 float,ProAmount8 float,ProAmount9 float,ProAmount10 float,ProAmount11 float,ProAmount12 float,
 ProAmount13 float,ProAmount14 float,ProAmount15 float,ProAmount16 float,ProAmount17 float,ProAmount18 float,ProAmount19 float,ProAmount20 float,ProAmount21 float,ProAmount22 float,ProAmount23 float,ProAmount24 float,
 ProSqft1 float,ProSqft2 float,ProSqft3 float,ProSqft4 float,ProSqft5 float,ProSqft6 float,ProSqft7 float,ProSqft8 float,ProSqft9 float,ProSqft10 float,ProSqft11 float,ProSqft12 float,
 ProSqft13 float,ProSqft14 float,ProSqft15 float,ProSqft16 float,ProSqft17 float,ProSqft18 float,ProSqft19 float,ProSqft20 float,ProSqft21 float,ProSqft22 float,ProSqft23 float,ProSqft24 float,SumPsf1 float,SumPsf2 float,ReportPeriod int,
 ComAmount1 float,ComAmount2 float,ComAmount3 float,ComAmount4 float,ComAmount5 float,ComAmount6 float,ComAmount7 float,ComAmount8 float,ComAmount9 float,ComAmount10 float,ComAmount11 float,ComAmount12 float,
 ComAmount13 float,ComAmount14 float,ComAmount15 float,ComAmount16 float,ComAmount17 float,ComAmount18 float,ComAmount19 float,ComAmount20 float,ComAmount21 float,ComAmount22 float,ComAmount23 float,ComAmount24 float
 )
 AS
 begin
 declare @Date date,@Date1 date
 set @Date =dateadd(month,1,left(@Period,4)+'-'+right(@Period,2)+'-01')
 set @Date1 =left(@Period,4)+'-'+right(@Period,2)+'-01'
 declare @Table Table
 (ChainName varchar(max),SuitId varchar(50),YearEnd int,BLDGID varchar(10),LeasId varchar(10),RentStrt date,expir date,Vacate date,SUITSQFT float,StypId varchar(10),
 DESCRPTN varchar(max),fdate date,amount money,sqft float,PPSF float,TenantId int,PropertyId int,unitId int,stypidCode int,IsComp int)
 --Added for multiple properties
 declare @MyBldgId int
 DECLARE Bldg_Cursor CURSOR FOR  
 (select * from dbo.StringSplit(@BldgId,',')) 
 OPEN Bldg_Cursor
 FETCH NEXT FROM Bldg_Cursor into @MyBldgId
 WHILE @@FETCH_STATUS = 0  
    BEGIN  
 declare @sqftType1 int
 set @sqftType1=case when @sqftType='ANSI' then case when (select dSqft0  from CommPropAreaLabel where hproperty=@MyBldgId)='Billable' then 1 else 0 end 
                     when @sqftType ='Billable' then case when (select dSqft1  from CommPropAreaLabel where hproperty=@MyBldgId)='Billable' then 1 else 0 end 
 					else case when (select dSqft2  from CommPropAreaLabel where hproperty=@MyBldgId)='Recoverable' then 2 else 4 end  end
 if @stypid ='All'
 insert Into @Table 
 select distinct isnull((select sname from Customer where hMyPerson =t.hcustomer and hCustomerType =4),t.SLASTNAME) as ChainName,u.SCODE as SuitId,null as YearEnd,p.SCODE as BLDGID, t.SCODE as LeasId,t.DTMOVEIN as RentStrt, t.DTLEASETO as expir, 
        t.DTMOVEOUT as Vacate,
 	   (select top 1 case when @sqftType1 =0 then dsqft0 when @sqftType1 =1 then DSQFT1 when @sqftType1 =2 then DSQFT2 else DSQFT4 end  from SQFT where HPOINTER=u.HMY and DTDATE<=t.DTMOVEIN and itype =4 order by DTDATE desc) as SUITSQFT,
 	   left(st1.sCode,3) as StypId,
        (select sDesc from commsalescategory where scode=left(st1.scode,3)) as DESCRPTN,s.dtFrom as fdate,
    	   case when isnull(s.dAudited,0)<>0 then s.dAudited when isnull(s.dActual,0)<>0 then s.dactual when isnull(s.dEstimate,0)<>0 then s.dEstimate else s.dforecasted end as amount, 
        (select top 1 case when @sqftType1 =0 then dsqft0 when @sqftType1 =1 then DSQFT1 when @sqftType1 =2 then DSQFT2 else DSQFT4 end from SQFT where HPOINTER=u.HMY and DTDATE<=s.dtto  and (a.dtmoveout is null or dtdate<a.dtmoveout) and itype =4 order by dtdate  desc) as sqft,

 	   dbo.pmz_ssrs_ProratedPSF_sales(cast(year(s.dtFrom) as char(4))+right('0'+cast(month(s.dtFrom) as varchar(2)),2),u.hmy,a.hmy) as PPSF,t.HMYPERSON as TenantId,
 	   p.hmy as PropertyId,u.hmy as UnitId,(select hmy from commsalescategory where scode=left(st1.scode,3)) as stypidcode,

    	   case when (select iscomp from AMENDBUT1 where hcode= a.hmy)='Yes' then 1 else 0 end as IsComp
 from property p
 inner join tenant t on  t.HPROPERTY =@MyBldgId and p.HMY=t.HPROPERTY
 inner join CommAmendments a on a.hTenant =t.HMYPERSON 
 inner join CommTenant ct on ct.hTenant =t.HMYPERSON 
 left join CommSalesData s on t.HMYPERSON =s.hTenant 
 left join CommSalesType st1 on st1.hMy=s.hSalesType 

 left join unitxref x on x.hAmendment =a.hmy
 left join unit u on x.hunit=u.hmy
 where t.HPROPERTY  =@MyBldgId and a.dtStart  <@Date and (isnull(a.dtMoveout,a.dtend) is null or isnull(a.dtMoveout,a.dtend)>=DATEADD(month,-25,@Date))--t.DTMOVEIN <=@Date and isnull(t.DTMOVEOUT ,t.DTLEASETO) >=DATEADD(month,-24,@Date)

 and (isnull(a.dtmoveout,a.dtend) is null or a.dtStart <isnull(a.dtmoveout,a.dtend) )
 and s.dtFrom<@Date and s.dtFrom>DATEADD(month,-25,@Date) and 
 (isnull(a.dtMoveout,a.dtend) is null or s.dtFrom between a.dtStart  and isnull(a.dtMoveout,a.dtend) or 
 s.dtTo  between a.dtStart  and isnull(a.dtMoveout,a.dtend))

 and not left(st1.sCode,3) in ('NON','SPL','LRG') 
 and not (st1.sCode='vnm' and isnull((select sum(1) from camrule cc inner join CHARGTYP chh on chh.hmy=cc.hSalesChargecode where cc.hAmendment =a.hmy and chh.scode ='pr_slvp'),0)>0 ) 
 and not x.hTenant is null and left(st1.sCode,4) <>'com_'
 and s.hunit = case when (select top 1 hunit from commsalesdata where htenant=t.hmyperson order by hunit desc)=0 then s.hunit else x.hunit end
 and s.hSalesType in (select hSalesType from camrule where hAmendment =a.hmy and HCHARGECODE is null)
 else
 insert Into @Table 
 select distinct isnull((select sname from Customer where hMyPerson =t.hcustomer and hCustomerType =4),t.SLASTNAME) as ChainName,u.SCODE as SuitId,null as YearEnd,p.SCODE as BLDGID, t.SCODE as LeasId,t.DTMOVEIN as RentStrt, t.DTLEASETO as expir, 
        t.DTMOVEOUT as Vacate,
 	   (select top 1 case when @sqftType1 =0 then dsqft0 when @sqftType1 =1 then DSQFT1 when @sqftType1 =2 then DSQFT2 else DSQFT4 end  from SQFT where HPOINTER=u.HMY and DTDATE<=t.DTMOVEIN and itype =4 order by DTDATE desc) as SUITSQFT,left(st1.sCode,3) as StypId,
        (select sDesc from commsalescategory where scode=left(st1.scode,3)) as DESCRPTN,s.dtFrom as fdate, 
   	   case when isnull(s.dAudited,0)<>0 then s.dAudited when isnull(s.dActual,0)<>0 then s.dactual when isnull(s.dEstimate,0)<>0 then s.dEstimate else s.dforecasted end as amount, 
        (select top 1 case when @sqftType1 =0 then dsqft0 when @sqftType1 =1 then DSQFT1 when @sqftType1 =2 then DSQFT2 else DSQFT4 end from SQFT where HPOINTER=u.HMY and DTDATE<=s.dtto  and (a.dtmoveout is null or dtdate<a.dtmoveout) and itype =4 order by dtdate  desc) as sqft,

 	   dbo.pmz_ssrs_ProratedPSF_sales(cast(year(s.dtFrom) as char(4))+right('0'+cast(month(s.dtFrom) as varchar(2)),2),u.hmy,a.hmy) as PPSF,t.HMYPERSON as TenantId,
 	   p.hmy as PropertyId,u.hmy as UnitId,(select hmy from commsalescategory where scode=left(st1.scode,3)) as stypidcode,
 	   
    	   case when (select iscomp from AMENDBUT1 where hcode= a.hmy)='Yes' then 1 else 0 end as IsComp
 from Property p
 inner join tenant t on t.HPROPERTY =@MyBldgId  and p.HMY=t.HPROPERTY 
 inner join CommTenant ct on ct.hTenant =t.HMYPERSON 
 inner join CommAmendments a on a.hTenant =t.HMYPERSON 
 left join CommSalesData s on t.HMYPERSON =s.hTenant 
 left join CommSalesType st1 on st1.hMy=s.hSalesType 

 left join unitxref x on x.hAmendment =a.HMY 
 left join unit u on x.hunit=u.hmy
 where t.HPROPERTY  =@MyBldgId and a.dtStart <@Date and (isnull(a.dtMoveout,a.dtend) is null or isnull(a.dtMoveout,a.dtend)>=DATEADD(month,-25,@Date))--t.DTMOVEIN <=@Date and isnull(t.DTMOVEOUT ,t.DTLEASETO) >=DATEADD(month,-24,@Date)
 and (isnull(a.dtmoveout,a.dtend) is null or a.dtStart<isnull(a.dtmoveout,a.dtend) )
 and s.dtFrom<@Date and s.dtFrom>DATEADD(month,-25,@Date) and (isnull(a.dtMoveout,a.dtend) is null or s.dtFrom between a.dtStart  and isnull(a.dtMoveout,a.dtend) or s.dtTo  between a.dtStart  and isnull(a.dtMoveout,a.dtend))

 and left(st1.sCode,3) in (select * from dbo.StringSplit(@StypId,',')) and not left(st1.sCode,3) in ('NON','SPL','LRG') and st1.scode<>'vnm_slvp'
 and not x.hTenant is null and left(st1.sCode,4) <>'com_'
 and s.hunit = case when (select top 1 hunit from commsalesdata where htenant=t.hmyperson order by hunit desc)=0 then s.hunit else x.hunit end
 and s.hSalesType in (select hSalesType from camrule where hAmendment =a.hmy and HCHARGECODE is null)
 insert into @Table1 
 select *,
 @Period as ReportPeriod,
 case when iscomparable =0 then 0 else ProAmount1 end  as ComAmount1,
 case when iscomparable =0 then 0 else ProAmount2 end  as ComAmount2,
 case when iscomparable =0 then 0 else ProAmount3 end  as ComAmount3,
 case when iscomparable =0 then 0 else ProAmount4 end  as ComAmount4,
 case when iscomparable =0 then 0 else ProAmount5 end  as ComAmount5,
 case when iscomparable =0 then 0 else ProAmount6 end  as ComAmount6,
 case when iscomparable =0 then 0 else ProAmount7 end  as ComAmount7,
 case when iscomparable =0 then 0 else ProAmount8 end  as ComAmount8,
 case when iscomparable =0 then 0 else ProAmount9 end  as ComAmount9,
 case when iscomparable =0 then 0 else ProAmount10 end  asComAmount10,
 case when iscomparable =0 then 0 else ProAmount11 end  as ComAmount11,
 case when iscomparable =0 then 0 else ProAmount12 end  as ComAmount12,
 case when iscomparable =0 then 0 else ProAmount13 end  as ComAmount13,
 case when iscomparable =0 then 0 else ProAmount14 end  as ComAmount14,
 case when iscomparable =0 then 0 else ProAmount15 end  as ComAmount15,
 case when iscomparable =0 then 0 else ProAmount16 end  as ComAmount16,
 case when iscomparable =0 then 0 else ProAmount17 end  as ComAmount17,
 case when iscomparable =0 then 0 else ProAmount18 end  as ComAmount18,
 case when iscomparable =0 then 0 else ProAmount19 end  as ComAmount19,
 case when iscomparable =0 then 0 else ProAmount20 end  as ComAmount20,
 case when iscomparable =0 then 0 else ProAmount21 end  as ComAmount21,
 case when iscomparable =0 then 0 else ProAmount22 end  as ComAmount22,
 case when iscomparable =0 then 0 else ProAmount23 end  as ComAmount23,
 case when iscomparable =0 then 0 else ProAmount24 end  as ComAmount24
  from (
 select distinct bldgid,ChainName as Tenant,SuitId as UnitId,stypid,descrptn,
 isnull((select top 1 sqft from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId   and fdate=@Date1 order by fdate desc),0) as Boma,
 isnull((select sum(amount) from (select distinct rentstrt,expir,vacate,fdate,amount,tenantid,propertyid,unitid,stypidcode from @Table) t5 where TenantId=t.tenantid and unitid =t.unitid and stypidcode=t.stypidcode  and fdate=@Date1),0) as Amount,
 isnull((select top 1 sqft from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId and fdate=dateadd(year,-1,@Date1) order by fdate desc),0) as LastBoma,
 isnull((select SUM(amount) from (select distinct rentstrt,expir,vacate,fdate,amount,tenantid,propertyid,unitid,stypidcode from @Table) t5 where TenantId=t.tenantid and unitid =t.unitid and stypidcode=t.stypidcode  and fdate=dateadd(year,-1,@Date1)),0) as LastAmount,
 isnull((select SUM(amount) from (select distinct rentstrt,expir,vacate,fdate,amount,tenantid,propertyid,unitid,stypidcode from @Table) t5 where TenantId=t.tenantid and unitid =t.unitid and stypidcode=t.stypidcode and fdate between dateadd(month,1,DATEADD(year,-1,@Date1)) and  @Date1),0) as CAmount,
 isnull((select SUM(amount) from (select distinct rentstrt,expir,vacate,fdate,amount,tenantid,propertyid,unitid,stypidcode from @Table) t5 where TenantId=t.tenantid and unitid =t.unitid and stypidcode=t.stypidcode and fdate between dateadd(month,1,DATEADD(year,-2,@Date1)) and dateadd(year,-1,@Date1)),0) as PAmount,
 isnull((select SUM(amount) from (select distinct rentstrt,expir,vacate,fdate,amount,tenantid,propertyid,unitid,stypidcode from @Table) t5 where TenantId=t.tenantid and unitid =t.unitid and stypidcode=t.stypidcode and fdate between DATEADD(month,(-1 * month(@Date1))+1,@Date1) and  @Date1),0) as CYAmount,
 isnull((select SUM(amount) from (select distinct rentstrt,expir,vacate,fdate,amount,tenantid,propertyid,unitid,stypidcode from @Table) t5 where TenantId=t.tenantid and unitid =t.unitid and stypidcode=t.stypidcode and fdate between DATEADD(month,(-1 * month(@Date1))+1,dateadd(year,-1,@Date1)) and dateadd(year,-1,@Date1)),0) as PYAmount

 ,case when (select top 1 IsComp from @Table where TenantId=t.tenantid and isnull(suitid,0)=isnull(t.suitid,0)  and fdate=@Date1 order by fdate desc)=1 then 1 else
 dbo.[pmz_ssrs_IsComparableSales_Sales] (propertyid ,unitId , @sqftType1 , @DATE1, stypidcode) end as isComparable,
 isnull((select sum(case when sqft=0 or ppsf=0 then 0 else amount/(sqft * ppsf) end) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId and fdate=@Date1),0) as AmountPSF,
 isnull((select SUM(case when sqft=0 or ppsf=0 then 0 else amount/(sqft * ppsf) end) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId and fdate=dateadd(year,-1,@Date1)),0) as LastAmountPSF,
 isnull((select SUM(case when sqft=0 or ppsf=0 then 0 else amount/(sqft * ppsf) end) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId and fdate between dateadd(month,1,DATEADD(year,-1,@Date1)) and  @Date1),0) as CAmountPSF,
 isnull((select SUM(case when sqft=0 or ppsf=0 then 0 else amount/(sqft * ppsf) end) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId and fdate between dateadd(month,1,DATEADD(year,-2,@Date1)) and dateadd(year,-1,@Date1)),0) as PAmountPSF,

 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =@Date1 and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount1,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-1,@Date1) and stypid=t.stypid and unitid=t.unitid ),0) as ProAmount2,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-2,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount3,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-3,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount4,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-4,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount5,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-5,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount6,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-6,@Date1) and stypid=t.stypid and unitid=t.unitid ),0) as ProAmount7,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-7,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount8,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-8,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount9,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-9,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount10,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-10,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount11,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-11,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount12,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-12,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount13,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-13,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount14,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-14,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount15,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-15,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount16,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-16,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount17,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-17,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount18,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-18,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount19,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-19,@Date1) and stypid=t.stypid and unitid=t.unitid ),0) as ProAmount20,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-20,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount21,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-21,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount22,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-22,@Date1) and stypid=t.stypid and unitid=t.unitid  ),0) as ProAmount23,
 isnull((select sum(amount) from (select distinct fdate,amount,tenantid,propertyid,unitid,stypid,RentStrt from @Table) t5 where TenantId =t.TenantId and fdate =dateadd(month,-23,@Date1) and stypid=t.stypid and unitid=t.unitid ),0) as ProAmount24,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=@Date1 ),0) as ProSqft1,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-1,@Date1)),0) as ProSqft2,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-2,@Date1) ),0) as ProSqft3,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-3,@Date1) ),0) as ProSqft4,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-4,@Date1) ),0) as ProSqft5,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-5,@Date1) ),0) as ProSqft6,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-6,@Date1) ),0) as ProSqft7,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-7,@Date1) ),0) as ProSqft8,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-8,@Date1) ),0) as ProSqft9,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-9,@Date1) ),0) as ProSqft10,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-10,@Date1) ),0) as ProSqft11,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-11,@Date1) ),0) as ProSqft12,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-12,@Date1) ),0) as ProSqft13,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-13,@Date1) ),0) as ProSqft14,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-14,@Date1) ),0) as ProSqft15,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-15,@Date1) ),0) as ProSqft16,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-16,@Date1) ),0) as ProSqft17,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-17,@Date1) ),0) as ProSqft18,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-18,@Date1) ),0) as ProSqft19,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-19,@Date1) ),0) as ProSqft20,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-20,@Date1) ),0) as ProSqft21,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-21,@Date1) ),0) as ProSqft22,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-22,@Date1) ),0) as ProSqft23,
 isnull((select sum(sqft*ppsf) from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-23,@Date1) ),0) as ProSqft24,
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=@Date1 order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-1,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-2,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-3,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-4,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-5,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-6,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-7,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-8,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-9,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-10,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-11,@Date1) order by fdate desc),0) as SumPsf1,
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-12,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-13,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-14,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-15,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-16,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-17,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-18,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-19,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-20,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-21,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-22,@Date1) order by fdate desc),0) +
 isnull((select top 1 1 as ppsf from @Table where TenantId=t.tenantid and SuitId =t.SuitId and stypid=t.StypId  and fdate=dateadd(month,-23,@Date1) order by fdate desc),0) as SumPsf2
 from @Table t ) k
 order by bldgid,descrptn,unitid

          FETCH NEXT FROM Bldg_Cursor into @MyBldgId  
    END
 CLOSE Bldg_Cursor;  
 DEALLOCATE Bldg_Cursor; 
 RETURN
 END
GO



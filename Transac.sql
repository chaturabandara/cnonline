USE [PRR_DW]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE     proc [etl].[proc_upd_table_yardi_tenant_transactional] as 
set nocount on
begin
	begin try
	 
/***********************************************************************************/
 --------------------------------------------------------------------------------------------------	
		declare @period int,@LastPeriod int,@StartDate date,@bldgid1 int,@bldgid int,@stypid varchar(5),@sqftType varchar(5),@CurrentDate date,@count int
		declare @OPENQUERY nvarchar(max), @LinkedServer nvarchar(4000),@sql varchar(max),@sql1 varchar(max)
		declare @WSDate date, @WEDate date
		declare @sqftType1 int
		Create Table  #table3   (hmy int,scode varchar(10),saddr1 varchar(100),sstate varchar(10), tdate date,NonCRUGLA float,CRUGLA float,isActive int,leaseableGLA float,NOI float,BudgetedNOI float)
		Create Table  #table1   (bldgid int, tenantid int,unitid varchar(10),tdate date,sqft float,SalesGLA float,sales float,baserent float,additional float,IsComparable int,MtM int,R12GLA float,R12SalesGLA float,R12Sales float,R12BaseRent float, R12Additional float,stypid varchar(10),psf float)
		Create Table  #table2   (bldgid int, tenantid int,unitid varchar(10),tenant varchar(100),descrptn varchar(100),stypid varchar(20),NTenant varchar(100),ChainName varchar(100), isLargeSL int,RentStrt date,RentEnd date,unittype varchar(50))
		Create Table #yardi_TenantDimension_test  (
			[TenantId] [int] ,
			[PropertyId] [int] ,
			[NationalTenant] [varchar](250) ,
			[Tenant] [varchar](250) ,
			[Category] [varchar](250) ,
			[CategoryGrouped] [varchar](250) ,
			[UnitType] [varchar](100) ,
			[LeaseType] [varchar](100) ,
			[LeaseStartDate] [date] ,
			[LeaseEndDate] [date] ,
			[Stypid] [varchar](10) 
		) 

		Create table #Yardi_TenantTransactional_test (
			[TenantId] [int] ,
			[PropertyId] [int] ,
			[Date] [date] ,
			[SameStore] [int] ,
			[MonthToMonth] [int],
			[WeightedGLA] [float] ,
			[Sales] [money] ,
			[BaseRent] [money] ,
			[AdditionalRent] [money] ,
			[R12WeightedGLA] [float] ,
			[R12Sales] [money] ,
			[R12BaseRent] [money] ,
			[R12AdditionalRent] [money] ,
			[stypid] [varchar](10) ,
			[SalesGLA] [money] ,
			[R12SalesGLA] [money] ,
			[psf] [float] 
		) 
		CREATE TABLE  #table(
	[rPeriod] [int] NULL,
	[bldgid] [int] NULL,
	[bldgname] [varchar](max) NULL,
	[descrptn] [varchar](100) NULL,
	[stypid] [varchar](20) NULL,
	[NTenant] [varchar](100) NULL,
	[ChainName] [varchar](100) NULL,
	[tenant] [varchar](100) NULL,
	[UnitID] [varchar](100) NULL,
	[RentStrt] [date] NULL,
	[RentEnd] [date] NULL,
	[leasid] [varchar](10) NULL,
	[tenantid] [int] NULL,
	[amount1] [money] NULL,
	[amount2] [money] NULL,
	[amount3] [money] NULL,
	[amount4] [money] NULL,
	[amount5] [money] NULL,
	[amount6] [money] NULL,
	[amount7] [money] NULL,
	[amount8] [money] NULL,
	[amount9] [money] NULL,
	[amount10] [money] NULL,
	[amount11] [money] NULL,
	[amount12] [money] NULL,
	[amount13] [money] NULL,
	[amount14] [money] NULL,
	[amount15] [money] NULL,
	[amount16] [money] NULL,
	[amount17] [money] NULL,
	[amount18] [money] NULL,
	[amount19] [money] NULL,
	[amount20] [money] NULL,
	[amount21] [money] NULL,
	[amount22] [money] NULL,
	[amount23] [money] NULL,
	[amount24] [money] NULL,
	[amount25] [money] NULL,
	[amount26] [money] NULL,
	[amount27] [money] NULL,
	[amount28] [money] NULL,
	[amount29] [money] NULL,
	[amount30] [money] NULL,
	[amount31] [money] NULL,
	[amount32] [money] NULL,
	[amount33] [money] NULL,
	[amount34] [money] NULL,
	[amount35] [money] NULL,
	[amount36] [money] NULL,
	[amount37] [money] NULL,
	[amount38] [money] NULL,
	[amount39] [money] NULL,
	[amount40] [money] NULL,
	[amount41] [money] NULL,
	[amount42] [money] NULL,
	[amount43] [money] NULL,
	[amount44] [money] NULL,
	[amount45] [money] NULL,
	[amount46] [money] NULL,
	[amount47] [money] NULL,
	[amount48] [money] NULL,
	[sqft1] [float] NULL,
	[sqft2] [float] NULL,
	[sqft3] [float] NULL,
	[sqft4] [float] NULL,
	[sqft5] [float] NULL,
	[sqft6] [float] NULL,
	[sqft7] [float] NULL,
	[sqft8] [float] NULL,
	[sqft9] [float] NULL,
	[sqft10] [float] NULL,
	[sqft11] [float] NULL,
	[sqft12] [float] NULL,
	[sqft13] [float] NULL,
	[sqft14] [float] NULL,
	[sqft15] [float] NULL,
	[sqft16] [float] NULL,
	[sqft17] [float] NULL,
	[sqft18] [float] NULL,
	[sqft19] [float] NULL,
	[sqft20] [float] NULL,
	[sqft21] [float] NULL,
	[sqft22] [float] NULL,
	[sqft23] [float] NULL,
	[sqft24] [float] NULL,
	[sqft25] [float] NULL,
	[sqft26] [float] NULL,
	[sqft27] [float] NULL,
	[sqft28] [float] NULL,
	[sqft29] [float] NULL,
	[sqft30] [float] NULL,
	[sqft31] [float] NULL,
	[sqft32] [float] NULL,
	[sqft33] [float] NULL,
	[sqft34] [float] NULL,
	[sqft35] [float] NULL,
	[sqft36] [float] NULL,
	[sqft37] [float] NULL,
	[sqft38] [float] NULL,
	[sqft39] [float] NULL,
	[sqft40] [float] NULL,
	[sqft41] [float] NULL,
	[sqft42] [float] NULL,
	[sqft43] [float] NULL,
	[sqft44] [float] NULL,
	[sqft45] [float] NULL,
	[sqft46] [float] NULL,
	[sqft47] [float] NULL,
	[sqft48] [float] NULL,
	[baserent1] [float] NULL,
	[baserent2] [float] NULL,
	[baserent3] [float] NULL,
	[baserent4] [float] NULL,
	[baserent5] [float] NULL,
	[baserent6] [float] NULL,
	[baserent7] [float] NULL,
	[baserent8] [float] NULL,
	[baserent9] [float] NULL,
	[baserent10] [float] NULL,
	[baserent11] [float] NULL,
	[baserent12] [float] NULL,
	[baserent13] [float] NULL,
	[baserent14] [float] NULL,
	[baserent15] [float] NULL,
	[baserent16] [float] NULL,
	[baserent17] [float] NULL,
	[baserent18] [float] NULL,
	[baserent19] [float] NULL,
	[baserent20] [float] NULL,
	[baserent21] [float] NULL,
	[baserent22] [float] NULL,
	[baserent23] [float] NULL,
	[baserent24] [float] NULL,
	[baserent25] [float] NULL,
	[baserent26] [float] NULL,
	[baserent27] [float] NULL,
	[baserent28] [float] NULL,
	[baserent29] [float] NULL,
	[baserent30] [float] NULL,
	[baserent31] [float] NULL,
	[baserent32] [float] NULL,
	[baserent33] [float] NULL,
	[baserent34] [float] NULL,
	[baserent35] [float] NULL,
	[baserent36] [float] NULL,
	[baserent37] [float] NULL,
	[baserent38] [float] NULL,
	[baserent39] [float] NULL,
	[baserent40] [float] NULL,
	[baserent41] [float] NULL,
	[baserent42] [float] NULL,
	[baserent43] [float] NULL,
	[baserent44] [float] NULL,
	[baserent45] [float] NULL,
	[baserent46] [float] NULL,
	[baserent47] [float] NULL,
	[baserent48] [float] NULL,
	[additional1] [float] NULL,
	[additional2] [float] NULL,
	[additional3] [float] NULL,
	[additional4] [float] NULL,
	[additional5] [float] NULL,
	[additional6] [float] NULL,
	[additional7] [float] NULL,
	[additional8] [float] NULL,
	[additional9] [float] NULL,
	[additional10] [float] NULL,
	[additional11] [float] NULL,
	[additional12] [float] NULL,
	[additional13] [float] NULL,
	[additional14] [float] NULL,
	[additional15] [float] NULL,
	[additional16] [float] NULL,
	[additional17] [float] NULL,
	[additional18] [float] NULL,
	[additional19] [float] NULL,
	[additional20] [float] NULL,
	[additional21] [float] NULL,
	[additional22] [float] NULL,
	[additional23] [float] NULL,
	[additional24] [float] NULL,
	[additional25] [float] NULL,
	[additional26] [float] NULL,
	[additional27] [float] NULL,
	[additional28] [float] NULL,
	[additional29] [float] NULL,
	[additional30] [float] NULL,
	[additional31] [float] NULL,
	[additional32] [float] NULL,
	[additional33] [float] NULL,
	[additional34] [float] NULL,
	[additional35] [float] NULL,
	[additional36] [float] NULL,
	[additional37] [float] NULL,
	[additional38] [float] NULL,
	[additional39] [float] NULL,
	[additional40] [float] NULL,
	[additional41] [float] NULL,
	[additional42] [float] NULL,
	[additional43] [float] NULL,
	[additional44] [float] NULL,
	[additional45] [float] NULL,
	[additional46] [float] NULL,
	[additional47] [float] NULL,
	[additional48] [float] NULL,
	[isLargeSL] [int] NULL,
	[ppsf1] [int] NULL,
	[ppsf2] [int] NULL,
	[ppsf3] [int] NULL,
	[ppsf4] [int] NULL,
	[isComparable1] [int] NULL,
	[isComparable2] [int] NULL,
	[isComparable3] [int] NULL,
	[isComparable4] [int] NULL,
	[isComparable5] [int] NULL,
	[isComparable6] [int] NULL,
	[isComparable7] [int] NULL,
	[isComparable8] [int] NULL,
	[isComparable9] [int] NULL,
	[isComparable10] [int] NULL,
	[isComparable11] [int] NULL,
	[isComparable12] [int] NULL,
	[isComparable13] [int] NULL,
	[isComparable14] [int] NULL,
	[isComparable15] [int] NULL,
	[isComparable16] [int] NULL,
	[isComparable17] [int] NULL,
	[isComparable18] [int] NULL,
	[isComparable19] [int] NULL,
	[isComparable20] [int] NULL,
	[isComparable21] [int] NULL,
	[isComparable22] [int] NULL,
	[isComparable23] [int] NULL,
	[isComparable24] [int] NULL,
	[isComparable25] [int] NULL,
	[isComparable26] [int] NULL,
	[isComparable27] [int] NULL,
	[isComparable28] [int] NULL,
	[isComparable29] [int] NULL,
	[isComparable30] [int] NULL,
	[isComparable31] [int] NULL,
	[isComparable32] [int] NULL,
	[isComparable33] [int] NULL,
	[isComparable34] [int] NULL,
	[isComparable35] [int] NULL,
	[isComparable36] [int] NULL,
	[unittype] [varchar](50) NULL,
	[asqft1] [float] NULL,
	[asqft2] [float] NULL,
	[asqft3] [float] NULL,
	[asqft4] [float] NULL,
	[asqft5] [float] NULL,
	[asqft6] [float] NULL,
	[asqft7] [float] NULL,
	[asqft8] [float] NULL,
	[asqft9] [float] NULL,
	[asqft10] [float] NULL,
	[asqft11] [float] NULL,
	[asqft12] [float] NULL,
	[asqft13] [float] NULL,
	[asqft14] [float] NULL,
	[asqft15] [float] NULL,
	[asqft16] [float] NULL,
	[asqft17] [float] NULL,
	[asqft18] [float] NULL,
	[asqft19] [float] NULL,
	[asqft20] [float] NULL,
	[asqft21] [float] NULL,
	[asqft22] [float] NULL,
	[asqft23] [float] NULL,
	[asqft24] [float] NULL,
	[asqft25] [float] NULL,
	[asqft26] [float] NULL,
	[asqft27] [float] NULL,
	[asqft28] [float] NULL,
	[asqft29] [float] NULL,
	[asqft30] [float] NULL,
	[asqft31] [float] NULL,
	[asqft32] [float] NULL,
	[asqft33] [float] NULL,
	[asqft34] [float] NULL,
	[asqft35] [float] NULL,
	[asqft36] [float] NULL,
	[asqft37] [float] NULL,
	[asqft38] [float] NULL,
	[asqft39] [float] NULL,
	[asqft40] [float] NULL,
	[asqft41] [float] NULL,
	[asqft42] [float] NULL,
	[asqft43] [float] NULL,
	[asqft44] [float] NULL,
	[asqft45] [float] NULL,
	[asqft46] [float] NULL,
	[asqft47] [float] NULL,
	[asqft48] [float] NULL,
	[psf1] [float] NULL,
	[psf2] [float] NULL,
	[psf3] [float] NULL,
	[psf4] [float] NULL,
	[psf5] [float] NULL,
	[psf6] [float] NULL,
	[psf7] [float] NULL,
	[psf8] [float] NULL,
	[psf9] [float] NULL,
	[psf10] [float] NULL,
	[psf11] [float] NULL,
	[psf12] [float] NULL,
	[psf13] [float] NULL,
	[psf14] [float] NULL,
	[psf15] [float] NULL,
	[psf16] [float] NULL,
	[psf17] [float] NULL,
	[psf18] [float] NULL,
	[psf19] [float] NULL,
	[psf20] [float] NULL,
	[psf21] [float] NULL,
	[psf22] [float] NULL,
	[psf23] [float] NULL,
	[psf24] [float] NULL,
	[psf25] [float] NULL,
	[psf26] [float] NULL,
	[psf27] [float] NULL,
	[psf28] [float] NULL,
	[psf29] [float] NULL,
	[psf30] [float] NULL,
	[psf31] [float] NULL,
	[psf32] [float] NULL,
	[psf33] [float] NULL,
	[psf34] [float] NULL,
	[psf35] [float] NULL,
	[psf36] [float] NULL,
	[psf37] [float] NULL,
	[psf38] [float] NULL,
	[psf39] [float] NULL,
	[psf40] [float] NULL,
	[psf41] [float] NULL,
	[psf42] [float] NULL,
	[psf43] [float] NULL,
	[psf44] [float] NULL,
	[psf45] [float] NULL,
	[psf46] [float] NULL,
	[psf47] [float] NULL,
	[psf48] [float] NULL
) 


--------------------------------------------------------------------------------------------------
			set @CurrentDate =dateadd(month,-1,getdate())
			set @period=cast(year(@CurrentDate ) as char(4)) + right('0'+cast(month(@CurrentDate ) as varchar(2)),2)
			set @Lastperiod=cast(year(@CurrentDate )-2 as char(4)) + right('0'+cast(month(@CurrentDate ) as varchar(2)),2)
			set @StartDate =cast(year(@CurrentDate ) as char(4)) + '-' + right('0'+cast(month(@CurrentDate ) as varchar(2)),2) +'-01'
			set @bldgid1=1514--(SELECT hmy from yardi_property where scode='DUF001') 
			set @stypid ='all'
			set @sqftType = 'Billable'
			set @count=0
			set @WEDate =dateadd(day, -1,dateadd(month, 1, @StartDate))
			set @WSDate =dateadd(day,  1,dateadd(month,-12,@wedate)  )
--------------------------------------------------------------------------------------------------

		while @count<36
			begin
				insert into #table3(hmy,scode,saddr1 ,sstate ,tdate,NonCruGLA ,CRUGLA ,isActive,leaseableGLA ,noi,BudgetedNOI ) 
				select d.hmy,d.scode,d.SADDR1,d.sstate,dateadd(month,-1*@count,@startdate) as tdate,d.gl1 ,d.gl,iif(isnull(d.gl,0)<=0 or not sdispose is null,0,1) as isactive,
				isnull(d.gl,0)+isnull(d.gl1,0) as LeaseableGLA,
				isnull((select sum (smtd) from yardi_total t inner join yardi_acct a on a.hmy = hacct  where not smtd is null and hppty = d.hmy and umonth = dateadd(month,-1*@count,@startdate) and ibook = 1 and a.scode in (
				 select scode  from yardi_accttreedetail d where d.htree = 268 and hmy in (
				 select ax.hdetail from yardi_AcctTreeDetailXref ax
						  inner join yardi_AcctTreeDetail ad on ad.hmy=hHeader where ax.hTree =268 and ad.sdesc ='NOI Adj''d for Non-Cash Items'
				 ))),0) * -1 as NOI,

				isnull((select sum(iif(isnull(a.INORMALBALANCE,0)=1,-1*isnull(sbudget,0),isnull(sbudget,0))) from yardi_total t inner join yardi_acct a on a.hmy = hacct  where hppty = d.hmy and umonth = dateadd(month,-1*@count,@startdate) and ibook = 1 and a.scode in (
				 select scode  from yardi_accttreedetail d  where d.htree = 268 and hmy in (
				 select hdetail from yardi_AcctTreeDetailXref ax
						  inner join yardi_AcctTreeDetail ad on ad.hmy=hHeader where ax.hTree =268 and ad.sdesc ='NOI Adj''d for Non-Cash Items'
				 ))),0) * -1 as BudgetedNOI

				from (
				select lp.hproperty as hmy, f.scode,f.SADDR1,C.dSqft0,C.dSqft1,C.dSqft2,f.szipcode,g.gl,g1.gl as gl1,f.sdispose, f.sstate
				From yardi_property p 
				inner join yardi_listprop2 lp on lp.hproplist = p.hmy 
				inner join yardi_property f on f.hmy = lp.hproperty 
				left join yardi_CommPropAreaLabel c on c.hproperty=lp.hproperty
				left join (select k.hproperty,sum(sqft) as GL from (
					select t.hproperty,u.scode, t.hunit, 
					isnull(case when t.ddate<=dateadd(month,(@count*-1),@startdate) then
					case when c.dsqft0='Billable' then s1.dsqft0 when c.dSqft1 ='Billable' then s1.dSqft1 else s1.DSQFT4 end 
					else
					((case when c.dsqft0='Billable' then s1.dsqft0 when c.dSqft1 ='Billable' then s1.dSqft1 else s1.DSQFT4 end * datediff(day,t.ddate,dateadd(month,(@count*-1)+1,@startdate))-1)+
					(
					isnull((select top 1 case when c.dsqft0='Billable' then dsqft0 when c.dSqft1 ='Billable' then dSqft1 else DSQFT4 end from yardi_sqft where HPOINTER =t.hunit and ddate<t.ddate and itype=4 
					order by ddate desc),0) * day(ddate )))/(day(dateadd(day,-1,dateadd(month,(@count*-1)+1,@startdate)))) 
					end,0)
					as sqft
					from 
					(
					select s.HPOINTER as hunit,max(s.DTDATE) as ddate,u.HPROPERTY as hproperty  --,s.*
					from yardi_sqft s 
					inner join yardi_unit u on u.hmy=s.hpointer and s.ITYPE =4
					where dtdate<dateadd(day,-1,dateadd(month,(@count*-1)+1,@startdate))
					group by s.HPOINTER,u.HPROPERTY 
					) t
					inner join yardi_sqft s1 on s1.HPOINTER =t.hunit and s1.DTDATE =t.ddate and s1.ITYPE =4 
					inner join yardi_CommPropAreaLabel c on c.hProperty =t.hproperty 
					inner join yardi_PROPERTY p1 on p1.hmy=t.hproperty 
					inner join yardi_unit u on u.hmy=t.hunit
					inner join yardi_UNITTYPE ut on u.HUNITTYPE=ut.hmy
					where isnull(case when c.dsqft0='Billable' then s1.dsqft0 when c.dSqft1 ='Billable' then s1.dSqft1 else s1.DSQFT4 end,0)<
						  case when left(p1.scode,3)='PDR' then 9000
							   when left(p1.scode,3)='EMP' then 20000           
							   when left(p1.scode,3) in ('PPM','SPM') then 10000           
							   else 15000 end
	   					  and ut.scode in ('utbsmt','utfood','utkiosk','utext','utretail','utrepad') --added 2021-04-29
						) k 
					group by k.hproperty 
					) g on g.hproperty =lp.hproperty

				 left join (
					select k.hproperty,sum(sqft) as GL from (
					select t.hproperty,u.scode, t.hunit, 
					isnull(case when t.ddate<=dateadd(month,(@count*-1),@startdate) then
					case when c.dsqft0='Billable' then s1.dsqft0 when c.dSqft1 ='Billable' then s1.dSqft1 else s1.DSQFT4 end 
					else
					((case when c.dsqft0='Billable' then s1.dsqft0 when c.dSqft1 ='Billable' then s1.dSqft1 else s1.DSQFT4 end * datediff(day,t.ddate,dateadd(month,(@count*-1)+1,@startdate))-1)+
					(
					isnull((select top 1 case when c.dsqft0='Billable' then dsqft0 when c.dSqft1 ='Billable' then dSqft1 else DSQFT4 end from yardi_sqft where HPOINTER =t.hunit and ddate<t.ddate and itype=4 
					order by ddate desc),0) * day(ddate )))/(day(dateadd(day,-1,dateadd(month,(@count*-1)+1,@startdate)))) 
					end,0)
					as sqft
					from 
					(
					select s.HPOINTER as hunit,max(s.DTDATE) as ddate,u.HPROPERTY as hproperty  --,s.*
					from yardi_sqft s 
					inner join yardi_unit u on u.hmy=s.hpointer and s.ITYPE =4
					where dtdate<dateadd(day,-1,dateadd(month,(@count*-1)+1,@startdate))
					group by s.HPOINTER,u.HPROPERTY 
					) t
					inner join yardi_sqft s1 on s1.HPOINTER =t.hunit and s1.DTDATE =t.ddate and s1.ITYPE =4 
					inner join yardi_CommPropAreaLabel c on c.hProperty =t.hproperty 
					inner join yardi_PROPERTY p1 on p1.hmy=t.hproperty 
					inner join yardi_unit u on u.hmy=t.hunit
					inner join yardi_UNITTYPE ut on u.HUNITTYPE=ut.hmy
					where isnull(case when c.dsqft0='Billable' then s1.dsqft0 when c.dSqft1 ='Billable' then s1.dSqft1 else s1.DSQFT4 end,0)>=
						  case when left(p1.scode,3)='PDR' then 9000
							   when left(p1.scode,3)='EMP' then 20000           
							   when left(p1.scode,3) in ('PPM','SPM') then 10000           
							   else 15000 end
	   					  or not ut.scode in ('utbsmt','utfood','utkiosk','utext','utretail','utrepad') --added 2021-04-29
					) k  group by k.hproperty		 
					) g1 on g1.hproperty =lp.hproperty
				where --p.scode = '.pmzlist'    and 
				lp.itype = 3 and f.ITYPE <>443 /*added for removing sold properties*/and f.SDISPOSE is null 
				) d

				set @count =@count +1
		end

---------------------------------------------------------------------------------------------------------------------------------------------------
-- ****** Tenant
---------------------------------------------------------------------------------------------------------------------------------------------------

			set @sqftType1=case when @sqftType='ANSI' then case when (select dSqft0  from yardi_CommPropAreaLabel where hproperty=@BLDGID1)='Billable' then 1 else 0 end 
								when @sqftType ='Billable' then case when (select dSqft1  from yardi_CommPropAreaLabel where hproperty=@BLDGID1)='Billable' then 1 else 0 end 
								else case when (select dSqft2  from yardi_CommPropAreaLabel where hproperty=@BLDGID1)='Recoverable' then 2 else 4 end  end


set @count =0

DECLARE bldgid_Cursor1900 CURSOR FOR  
--select * from dbo.StringSplit(@BldgId1 ,',')
select distinct p.propertyid 
From yardi_propertyDimension p

OPEN bldgid_Cursor1900
FETCH NEXT FROM bldgid_Cursor1900 into @bldgid
WHILE @@FETCH_STATUS = 0  
   BEGIN  

	set @sql='''select rPeriod ,bldgid ,bldgname ,descrptn ,stypid ,NTenant,ChainName,tenant ,RentStrt,RentEnd ,leasid ,tenantid ,
				amount1,amount2,amount3,amount4,amount5,amount6,amount7,amount8,amount9,amount10,amount11,amount12,
				amount13,amount14,amount15,amount16,amount17,amount18,amount19,amount20,amount21,amount22,amount23,amount24,
				amount25,amount26,amount27,amount28,amount29,amount30,amount31,amount32,amount33,amount34,amount35,amount36,
				amount37,amount38,amount39,amount40,amount41,amount42,amount43,amount44,amount45,amount46,amount47,amount48,
				sqft1,sqft2,sqft3,sqft4,sqft5,sqft6,sqft7,sqft8,sqft9,sqft10,sqft11,sqft12,
				sqft13,sqft14,sqft15,sqft16,sqft17,sqft18,sqft19,sqft20,sqft21,sqft22,sqft23,sqft24,
				sqft25,sqft26,sqft27,sqft28,sqft29,sqft30,sqft31,sqft32,sqft33,sqft34,sqft35,sqft36,
				sqft37,sqft38,sqft39,sqft40,sqft41,sqft42,sqft43,sqft44,sqft45,sqft46,sqft47,sqft48,
				baserent1, baserent2, baserent3 , baserent4 , baserent5 , baserent6 , baserent7, baserent8, baserent9, baserent10, baserent11, baserent12,
				baserent13, baserent14, baserent15, baserent16, baserent17, baserent18, baserent19, baserent20, baserent21, baserent22, baserent23, baserent24,
				baserent25, baserent26, baserent27, baserent28, baserent29, baserent30, baserent31, baserent32, baserent33, baserent34, baserent35, baserent36,
				baserent37, baserent38, baserent39, baserent40, baserent41, baserent42, baserent43, baserent44, baserent45, baserent46, baserent47, baserent48, 
				additional1, additional2, additional3, additional4, additional5, additional6, additional7, additional8, additional9, additional10, additional11, additional12,
				additional13, additional14, additional15, additional16, additional17, additional18, additional19, additional20, additional21, additional22, additional23, additional24,
				additional25, additional26, additional27, additional28, additional29, additional30, additional31, additional32, additional33, additional34, additional35, additional36,
				additional37, additional38, additional39, additional40, additional41, additional42, additional43, additional44, additional45, additional46, additional47, additional48,
				isLargeSL,ppsf1,ppsf2,ppsf3,ppsf4,
				isComparable1, isComparable2, isComparable3, isComparable4, isComparable5, isComparable6, isComparable7, isComparable8, isComparable9, isComparable10, isComparable11, isComparable12, 
				isComparable13, isComparable14, isComparable15, isComparable16, isComparable17, isComparable18, isComparable19, isComparable20, isComparable21, isComparable22, isComparable23, isComparable24, 
				isComparable25, isComparable26, isComparable27, isComparable28, isComparable29, isComparable30, isComparable31, isComparable32, isComparable33, isComparable34, isComparable35, isComparable36,unittype,
				asqft1,asqft2,asqft3,asqft4,asqft5,asqft6,asqft7,asqft8,asqft9,asqft10,asqft11,asqft12,
				asqft13,asqft14,asqft15,asqft16,asqft17,asqft18,asqft19,asqft20,asqft21,asqft22,asqft23,asqft24,
				asqft25,asqft26,asqft27,asqft28,asqft29,asqft30,asqft31,asqft32,asqft33,asqft34,asqft35,asqft36,
				asqft37,asqft38,asqft39,asqft40,asqft41,asqft42,asqft43,asqft44,asqft45,asqft46,asqft47,asqft48
 
				from evqbwypwe_live.dbo.pmz_ssrs_TableauSales_Accounting(' + cast(@bldgid as varchar(10)) + ')'''

	SET @LinkedServer = '[Yardi_live]'

	
	SET @OPENQUERY = 'SELECT * FROM OPENQUERY('+ @LinkedServer + ',' + @sql + ')'

	
	insert into #table (rPeriod ,bldgid ,bldgname ,descrptn ,stypid ,NTenant ,ChainName,tenant ,RentStrt,RentEnd ,leasid ,tenantid ,
				amount1,amount2,amount3,amount4,amount5,amount6,amount7,amount8,amount9,amount10,amount11,amount12,
				amount13,amount14,amount15,amount16,amount17,amount18,amount19,amount20,amount21,amount22,amount23,amount24,
				amount25,amount26,amount27,amount28,amount29,amount30,amount31,amount32,amount33,amount34,amount35,amount36,
				amount37,amount38,amount39,amount40,amount41,amount42,amount43,amount44,amount45,amount46,amount47,amount48,
				sqft1,sqft2,sqft3,sqft4,sqft5,sqft6,sqft7,sqft8,sqft9,sqft10,sqft11,sqft12,
				sqft13,sqft14,sqft15,sqft16,sqft17,sqft18,sqft19,sqft20,sqft21,sqft22,sqft23,sqft24,
				sqft25,sqft26,sqft27,sqft28,sqft29,sqft30,sqft31,sqft32,sqft33,sqft34,sqft35,sqft36,
				sqft37,sqft38,sqft39,sqft40,sqft41,sqft42,sqft43,sqft44,sqft45,sqft46,sqft47,sqft48,
				baserent1, baserent2, baserent3 , baserent4 , baserent5 , baserent6 , baserent7, baserent8, baserent9, baserent10, baserent11, baserent12,
				baserent13, baserent14, baserent15, baserent16, baserent17, baserent18, baserent19, baserent20, baserent21, baserent22, baserent23, baserent24,
				baserent25, baserent26, baserent27, baserent28, baserent29, baserent30, baserent31, baserent32, baserent33, baserent34, baserent35, baserent36,
				baserent37, baserent38, baserent39, baserent40, baserent41, baserent42, baserent43, baserent44, baserent45, baserent46, baserent47, baserent48, 
				additional1, additional2, additional3, additional4, additional5, additional6, additional7, additional8, additional9, additional10, additional11, additional12,
				additional13, additional14, additional15, additional16, additional17, additional18, additional19, additional20, additional21, additional22, additional23, additional24,
				additional25, additional26, additional27, additional28, additional29, additional30, additional31, additional32, additional33, additional34, additional35, additional36,
				additional37, additional38, additional39, additional40, additional41, additional42, additional43, additional44, additional45, additional46, additional47, additional48,
				isLargeSL,ppsf1,ppsf2,ppsf3,ppsf4,
				isComparable1, isComparable2, isComparable3, isComparable4, isComparable5, isComparable6, isComparable7, isComparable8, isComparable9, isComparable10, isComparable11, isComparable12, 
				isComparable13, isComparable14, isComparable15, isComparable16, isComparable17, isComparable18, isComparable19, isComparable20, isComparable21, isComparable22, isComparable23, isComparable24, 
				isComparable25, isComparable26, isComparable27, isComparable28, isComparable29, isComparable30, isComparable31, isComparable32, isComparable33, isComparable34, isComparable35, isComparable36,unittype,
				asqft1,asqft2,asqft3,asqft4,asqft5,asqft6,asqft7,asqft8,asqft9,asqft10,asqft11,asqft12,
				asqft13,asqft14,asqft15,asqft16,asqft17,asqft18,asqft19,asqft20,asqft21,asqft22,asqft23,asqft24,
				asqft25,asqft26,asqft27,asqft28,asqft29,asqft30,asqft31,asqft32,asqft33,asqft34,asqft35,asqft36,
				asqft37,asqft38,asqft39,asqft40,asqft41,asqft42,asqft43,asqft44,asqft45,asqft46,asqft47,asqft48 )
	EXEC (@OPENQUERY) 

		        FETCH NEXT FROM bldgid_Cursor1900 into @bldgid  
   END
CLOSE bldgid_Cursor1900;  
DEALLOCATE bldgid_Cursor1900; 

			update #table 
			set psf1 =case when sqft1=0 then 0 when year(rentstrt)=year(@startdate) and month(rentstrt)=month(@startdate) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(@startdate) and month(RentEnd)=month(@startdate) then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf2 =case when sqft2=0 then 0 when year(rentstrt)=year(dateadd(month,-1,@startdate)) and month(rentstrt)=month(dateadd(month,-1,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-1,@startdate)) and month(RentEnd)=month(dateadd(month,-1,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf3 =case when sqft3=0 then 0 when year(rentstrt)=year(dateadd(month,-2,@startdate)) and month(rentstrt)=month(dateadd(month,-2,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-2,@startdate)) and month(RentEnd)=month(dateadd(month,-2,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf4 =case when sqft4=0 then 0 when year(rentstrt)=year(dateadd(month,-3,@startdate)) and month(rentstrt)=month(dateadd(month,-3,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-3,@startdate)) and month(RentEnd)=month(dateadd(month,-3,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf5 =case when sqft5=0 then 0 when year(rentstrt)=year(dateadd(month,-4,@startdate)) and month(rentstrt)=month(dateadd(month,-4,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-4,@startdate)) and month(RentEnd)=month(dateadd(month,-4,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf6 =case when sqft6=0 then 0 when year(rentstrt)=year(dateadd(month,-5,@startdate)) and month(rentstrt)=month(dateadd(month,-5,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-5,@startdate)) and month(RentEnd)=month(dateadd(month,-5,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf7 =case when sqft7=0 then 0 when year(rentstrt)=year(dateadd(month,-6,@startdate)) and month(rentstrt)=month(dateadd(month,-6,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-6,@startdate)) and month(RentEnd)=month(dateadd(month,-6,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf8 =case when sqft8=0 then 0 when year(rentstrt)=year(dateadd(month,-7,@startdate)) and month(rentstrt)=month(dateadd(month,-7,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-7,@startdate)) and month(RentEnd)=month(dateadd(month,-7,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf9 =case when sqft9=0 then 0 when year(rentstrt)=year(dateadd(month,-8,@startdate)) and month(rentstrt)=month(dateadd(month,-8,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-8,@startdate)) and month(RentEnd)=month(dateadd(month,-8,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf10 =case when sqft10=0 then 0 when year(rentstrt)=year(dateadd(month,-9,@startdate)) and month(rentstrt)=month(dateadd(month,-9,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-9,@startdate)) and month(RentEnd)=month(dateadd(month,-9,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf11 =case when sqft11=0 then 0 when year(rentstrt)=year(dateadd(month,-10,@startdate)) and month(rentstrt)=month(dateadd(month,-10,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-10,@startdate)) and month(RentEnd)=month(dateadd(month,-10,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf12 =case when sqft12=0 then 0 when year(rentstrt)=year(dateadd(month,-11,@startdate)) and month(rentstrt)=month(dateadd(month,-11,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-11,@startdate)) and month(RentEnd)=month(dateadd(month,-11,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf13 =case when sqft13=0 then 0 when year(rentstrt)=year(dateadd(month,-12,@startdate)) and month(rentstrt)=month(dateadd(month,-12,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-12,@startdate)) and month(RentEnd)=month(dateadd(month,-12,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf14 =case when sqft14=0 then 0 when year(rentstrt)=year(dateadd(month,-13,@startdate)) and month(rentstrt)=month(dateadd(month,-13,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-13,@startdate)) and month(RentEnd)=month(dateadd(month,-13,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf15 =case when sqft15=0 then 0 when year(rentstrt)=year(dateadd(month,-14,@startdate)) and month(rentstrt)=month(dateadd(month,-14,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-14,@startdate)) and month(RentEnd)=month(dateadd(month,-14,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf16 =case when sqft16=0 then 0 when year(rentstrt)=year(dateadd(month,-15,@startdate)) and month(rentstrt)=month(dateadd(month,-15,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-15,@startdate)) and month(RentEnd)=month(dateadd(month,-15,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf17 =case when sqft17=0 then 0 when year(rentstrt)=year(dateadd(month,-16,@startdate)) and month(rentstrt)=month(dateadd(month,-16,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-16,@startdate)) and month(RentEnd)=month(dateadd(month,-16,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf18 =case when sqft18=0 then 0 when year(rentstrt)=year(dateadd(month,-17,@startdate)) and month(rentstrt)=month(dateadd(month,-17,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-17,@startdate)) and month(RentEnd)=month(dateadd(month,-17,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf19 =case when sqft19=0 then 0 when year(rentstrt)=year(dateadd(month,-18,@startdate)) and month(rentstrt)=month(dateadd(month,-18,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-18,@startdate)) and month(RentEnd)=month(dateadd(month,-18,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf20 =case when sqft20=0 then 0 when year(rentstrt)=year(dateadd(month,-19,@startdate)) and month(rentstrt)=month(dateadd(month,-19,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-19,@startdate)) and month(RentEnd)=month(dateadd(month,-19,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf21 =case when sqft21=0 then 0 when year(rentstrt)=year(dateadd(month,-20,@startdate)) and month(rentstrt)=month(dateadd(month,-20,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-20,@startdate)) and month(RentEnd)=month(dateadd(month,-20,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf22 =case when sqft22=0 then 0 when year(rentstrt)=year(dateadd(month,-21,@startdate)) and month(rentstrt)=month(dateadd(month,-21,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-21,@startdate)) and month(RentEnd)=month(dateadd(month,-21,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf23 =case when sqft23=0 then 0 when year(rentstrt)=year(dateadd(month,-22,@startdate)) and month(rentstrt)=month(dateadd(month,-22,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-22,@startdate)) and month(RentEnd)=month(dateadd(month,-22,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf24 =case when sqft24=0 then 0 when year(rentstrt)=year(dateadd(month,-23,@startdate)) and month(rentstrt)=month(dateadd(month,-23,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-23,@startdate)) and month(RentEnd)=month(dateadd(month,-23,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf25 =case when sqft25=0 then 0 when year(rentstrt)=year(dateadd(month,-24,@startdate)) and month(rentstrt)=month(dateadd(month,-24,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-24,@startdate)) and month(RentEnd)=month(dateadd(month,-24,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf26 =case when sqft26=0 then 0 when year(rentstrt)=year(dateadd(month,-25,@startdate)) and month(rentstrt)=month(dateadd(month,-25,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-25,@startdate)) and month(RentEnd)=month(dateadd(month,-25,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf27 =case when sqft27=0 then 0 when year(rentstrt)=year(dateadd(month,-26,@startdate)) and month(rentstrt)=month(dateadd(month,-26,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-26,@startdate)) and month(RentEnd)=month(dateadd(month,-26,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf28 =case when sqft28=0 then 0 when year(rentstrt)=year(dateadd(month,-27,@startdate)) and month(rentstrt)=month(dateadd(month,-27,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-27,@startdate)) and month(RentEnd)=month(dateadd(month,-27,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf29 =case when sqft29=0 then 0 when year(rentstrt)=year(dateadd(month,-28,@startdate)) and month(rentstrt)=month(dateadd(month,-28,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-28,@startdate)) and month(RentEnd)=month(dateadd(month,-28,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf30 =case when sqft30=0 then 0 when year(rentstrt)=year(dateadd(month,-29,@startdate)) and month(rentstrt)=month(dateadd(month,-29,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-29,@startdate)) and month(RentEnd)=month(dateadd(month,-29,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf31 =case when sqft31=0 then 0 when year(rentstrt)=year(dateadd(month,-30,@startdate)) and month(rentstrt)=month(dateadd(month,-30,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-30,@startdate)) and month(RentEnd)=month(dateadd(month,-30,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf32 =case when sqft32=0 then 0 when year(rentstrt)=year(dateadd(month,-31,@startdate)) and month(rentstrt)=month(dateadd(month,-31,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-31,@startdate)) and month(RentEnd)=month(dateadd(month,-31,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf33 =case when sqft33=0 then 0 when year(rentstrt)=year(dateadd(month,-32,@startdate)) and month(rentstrt)=month(dateadd(month,-32,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-32,@startdate)) and month(RentEnd)=month(dateadd(month,-32,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf34 =case when sqft34=0 then 0 when year(rentstrt)=year(dateadd(month,-33,@startdate)) and month(rentstrt)=month(dateadd(month,-33,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-33,@startdate)) and month(RentEnd)=month(dateadd(month,-33,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf35 =case when sqft35=0 then 0 when year(rentstrt)=year(dateadd(month,-34,@startdate)) and month(rentstrt)=month(dateadd(month,-34,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-34,@startdate)) and month(RentEnd)=month(dateadd(month,-34,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf36 =case when sqft36=0 then 0 when year(rentstrt)=year(dateadd(month,-35,@startdate)) and month(rentstrt)=month(dateadd(month,-35,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-35,@startdate)) and month(RentEnd)=month(dateadd(month,-35,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf37 =case when sqft37=0 then 0 when year(rentstrt)=year(dateadd(month,-36,@startdate)) and month(rentstrt)=month(dateadd(month,-36,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-36,@startdate)) and month(RentEnd)=month(dateadd(month,-36,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf38 =case when sqft38=0 then 0 when year(rentstrt)=year(dateadd(month,-37,@startdate)) and month(rentstrt)=month(dateadd(month,-37,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-37,@startdate)) and month(RentEnd)=month(dateadd(month,-37,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf39 =case when sqft39=0 then 0 when year(rentstrt)=year(dateadd(month,-38,@startdate)) and month(rentstrt)=month(dateadd(month,-38,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-38,@startdate)) and month(RentEnd)=month(dateadd(month,-38,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf40 =case when sqft40=0 then 0 when year(rentstrt)=year(dateadd(month,-39,@startdate)) and month(rentstrt)=month(dateadd(month,-39,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-39,@startdate)) and month(RentEnd)=month(dateadd(month,-39,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf41 =case when sqft41=0 then 0 when year(rentstrt)=year(dateadd(month,-40,@startdate)) and month(rentstrt)=month(dateadd(month,-40,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-40,@startdate)) and month(RentEnd)=month(dateadd(month,-40,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf42 =case when sqft42=0 then 0 when year(rentstrt)=year(dateadd(month,-41,@startdate)) and month(rentstrt)=month(dateadd(month,-41,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-41,@startdate)) and month(RentEnd)=month(dateadd(month,-41,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf43 =case when sqft43=0 then 0 when year(rentstrt)=year(dateadd(month,-42,@startdate)) and month(rentstrt)=month(dateadd(month,-42,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-42,@startdate)) and month(RentEnd)=month(dateadd(month,-42,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf44 =case when sqft44=0 then 0 when year(rentstrt)=year(dateadd(month,-43,@startdate)) and month(rentstrt)=month(dateadd(month,-43,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-43,@startdate)) and month(RentEnd)=month(dateadd(month,-43,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf45 =case when sqft45=0 then 0 when year(rentstrt)=year(dateadd(month,-44,@startdate)) and month(rentstrt)=month(dateadd(month,-44,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-44,@startdate)) and month(RentEnd)=month(dateadd(month,-44,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf46 =case when sqft46=0 then 0 when year(rentstrt)=year(dateadd(month,-45,@startdate)) and month(rentstrt)=month(dateadd(month,-45,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-45,@startdate)) and month(RentEnd)=month(dateadd(month,-45,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf47 =case when sqft47=0 then 0 when year(rentstrt)=year(dateadd(month,-46,@startdate)) and month(rentstrt)=month(dateadd(month,-46,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-46,@startdate)) and month(RentEnd)=month(dateadd(month,-46,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			psf48 =case when sqft48=0 then 0 when year(rentstrt)=year(dateadd(month,-47,@startdate)) and month(rentstrt)=month(dateadd(month,-47,@startdate)) and day(rentstrt)>1 then (select dbo.Yardi_ProratedPSF_Tableau(rentstrt,1))
						   when year(RentEnd)=year(dateadd(month,-47,@startdate)) and month(RentEnd)=month(dateadd(month,-47,@startdate))  then (select dbo.Yardi_ProratedPSF_Tableau(RentEnd,0))   else 1 end,
			baserent1=case when isnull(amount1,0)=0 then 0 else baserent1 end,
			baserent2=case when isnull(amount2,0)=0 then 0 else baserent2 end,
			baserent3=case when isnull(amount3,0)=0 then 0 else baserent3 end,
			baserent4=case when isnull(amount4,0)=0 then 0 else baserent4 end,
			baserent5=case when isnull(amount5,0)=0 then 0 else baserent5 end,
			baserent6=case when isnull(amount6,0)=0 then 0 else baserent6 end,
			baserent7=case when isnull(amount7,0)=0 then 0 else baserent7 end,
			baserent8=case when isnull(amount8,0)=0 then 0 else baserent8 end,
			baserent9=case when isnull(amount9,0)=0 then 0 else baserent9 end,
			baserent10=case when isnull(amount10,0)=0 then 0 else baserent10 end,
			baserent11=case when isnull(amount11,0)=0 then 0 else baserent11 end,
			baserent12=case when isnull(amount12,0)=0 then 0 else baserent12 end,
			baserent13=case when isnull(amount13,0)=0 then 0 else baserent13 end,
			baserent14=case when isnull(amount14,0)=0 then 0 else baserent14 end,
			baserent15=case when isnull(amount15,0)=0 then 0 else baserent15 end,
			baserent16=case when isnull(amount16,0)=0 then 0 else baserent16 end,
			baserent17=case when isnull(amount17,0)=0 then 0 else baserent17 end,
			baserent18=case when isnull(amount18,0)=0 then 0 else baserent18 end,
			baserent19=case when isnull(amount19,0)=0 then 0 else baserent19 end,
			baserent20=case when isnull(amount20,0)=0 then 0 else baserent20 end,
			baserent21=case when isnull(amount21,0)=0 then 0 else baserent21 end,
			baserent22=case when isnull(amount22,0)=0 then 0 else baserent22 end,
			baserent23=case when isnull(amount23,0)=0 then 0 else baserent23 end,
			baserent24=case when isnull(amount24,0)=0 then 0 else baserent24 end,
			baserent25=case when isnull(amount25,0)=0 then 0 else baserent25 end,
			baserent26=case when isnull(amount26,0)=0 then 0 else baserent26 end,
			baserent27=case when isnull(amount27,0)=0 then 0 else baserent27 end,
			baserent28=case when isnull(amount28,0)=0 then 0 else baserent28 end,
			baserent29=case when isnull(amount29,0)=0 then 0 else baserent29 end,
			baserent30=case when isnull(amount30,0)=0 then 0 else baserent30 end,
			baserent31=case when isnull(amount31,0)=0 then 0 else baserent31 end,
			baserent32=case when isnull(amount32,0)=0 then 0 else baserent32 end,
			baserent33=case when isnull(amount33,0)=0 then 0 else baserent33 end,
			baserent34=case when isnull(amount34,0)=0 then 0 else baserent34 end,
			baserent35=case when isnull(amount35,0)=0 then 0 else baserent35 end,
			baserent36=case when isnull(amount36,0)=0 then 0 else baserent36 end,
			additional1=case when isnull(amount1,0)=0 then 0 else additional1 end,
			additional2=case when isnull(amount2,0)=0 then 0 else additional2 end,
			additional3=case when isnull(amount3,0)=0 then 0 else additional3 end,
			additional4=case when isnull(amount4,0)=0 then 0 else additional4 end,
			additional5=case when isnull(amount5,0)=0 then 0 else additional5 end,
			additional6=case when isnull(amount6,0)=0 then 0 else additional6 end,
			additional7=case when isnull(amount7,0)=0 then 0 else additional7 end,
			additional8=case when isnull(amount8,0)=0 then 0 else additional8 end,
			additional9=case when isnull(amount9,0)=0 then 0 else additional9 end,
			additional10=case when isnull(amount10,0)=0 then 0 else additional10 end,
			additional11=case when isnull(amount11,0)=0 then 0 else additional11 end,
			additional12=case when isnull(amount12,0)=0 then 0 else additional12 end,
			additional13=case when isnull(amount13,0)=0 then 0 else additional13 end,
			additional14=case when isnull(amount14,0)=0 then 0 else additional14 end,
			additional15=case when isnull(amount15,0)=0 then 0 else additional15 end,
			additional16=case when isnull(amount16,0)=0 then 0 else additional16 end,
			additional17=case when isnull(amount17,0)=0 then 0 else additional17 end,
			additional18=case when isnull(amount18,0)=0 then 0 else additional18 end,
			additional19=case when isnull(amount19,0)=0 then 0 else additional19 end,
			additional20=case when isnull(amount20,0)=0 then 0 else additional20 end,
			additional21=case when isnull(amount21,0)=0 then 0 else additional21 end,
			additional22=case when isnull(amount22,0)=0 then 0 else additional22 end,
			additional23=case when isnull(amount23,0)=0 then 0 else additional23 end,
			additional24=case when isnull(amount24,0)=0 then 0 else additional24 end,
			additional25=case when isnull(amount25,0)=0 then 0 else additional25 end,
			additional26=case when isnull(amount26,0)=0 then 0 else additional26 end,
			additional27=case when isnull(amount27,0)=0 then 0 else additional27 end,
			additional28=case when isnull(amount28,0)=0 then 0 else additional28 end,
			additional29=case when isnull(amount29,0)=0 then 0 else additional29 end,
			additional30=case when isnull(amount30,0)=0 then 0 else additional30 end,
			additional31=case when isnull(amount31,0)=0 then 0 else additional31 end,
			additional32=case when isnull(amount32,0)=0 then 0 else additional32 end,
			additional33=case when isnull(amount33,0)=0 then 0 else additional33 end,
			additional34=case when isnull(amount34,0)=0 then 0 else additional34 end,
			additional35=case when isnull(amount35,0)=0 then 0 else additional35 end,
			additional36=case when isnull(amount36,0)=0 then 0 else additional36 end


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid, tenantid,unitid, @StartDate ,isnull(sqft1,0),isnull(asqft1,0),isnull(Amount1,0) as sales,isnull(baserent1,0),isnull(additional1,0),isComparable1,iif(datediff(day,@startdate,rentend)<0,1,0) as MtM,
			iif(isnull(psf1,0)+isnull(psf2,0)+isnull(psf3,0)+isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)=0,0,
			(isnull(sqft1,0)+isnull(sqft2,0)+isnull(sqft3,0)+isnull(sqft4,0)+isnull(sqft5,0)+isnull(sqft6,0)+isnull(sqft7,0)+isnull(sqft8,0)+isnull(sqft9,0)+isnull(sqft10,0)+isnull(sqft11,0)+isnull(sqft12,0))/
			(isnull(psf1,0)+isnull(psf2,0)+isnull(psf3,0)+isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0))
			) as R12GLA,
			iif(isnull(psf1,0)+isnull(psf2,0)+isnull(psf3,0)+isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)=0,0,
			(isnull(asqft1,0)+isnull(asqft2,0)+isnull(asqft3,0)+isnull(asqft4,0)+isnull(asqft5,0)+isnull(asqft6,0)+isnull(asqft7,0)+isnull(asqft8,0)+isnull(asqft9,0)+isnull(asqft10,0)+isnull(asqft11,0)+isnull(asqft12,0))/
			(isnull(psf1,0)+isnull(psf2,0)+isnull(psf3,0)+isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0))
			) as R12SalesGLA,
			(isnull(Amount1,0)+isnull(Amount2,0)+isnull(Amount3,0)+isnull(Amount4,0)+isnull(Amount5,0)+isnull(Amount6,0)+isnull(Amount7,0)+isnull(Amount8,0)+isnull(Amount9,0)+isnull(Amount10,0)+isnull(Amount11,0)+amount12) as R12Sales,
			(isnull(baserent1,0)+isnull(baserent2,0)+isnull(baserent3,0)+isnull(baserent4,0)+isnull(baserent5,0)+isnull(baserent6,0)+isnull(baserent7,0)+isnull(baserent8,0)+isnull(baserent9,0)+isnull(baserent10,0)+isnull(baserent11,0)+isnull(baserent12,0)) as R12BaseRent,
			(isnull(additional1,0)+isnull(additional2,0)+isnull(additional3,0)+isnull(additional4,0)+isnull(additional5,0)+isnull(additional6,0)+isnull(additional7,0)+isnull(additional8,0)+isnull(additional9,0)+isnull(additional10,0)+isnull(additional11,0)+isnull(additional12,0)) as  R12Additional  
			,stypid,psf1 
			from #table where not amount1 is null and @StartDate between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-1,@StartDate) ,isnull(sqft2,0),isnull(asqft2,0),isnull(amount2,0) as sales,isnull(baserent2,0),isnull(additional2,0),isComparable2,iif(RentEnd<dateadd(month,-1,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft2,0)=0,0,1)+iif(isnull(sqft3,0)=0,0,1)+iif(isnull(sqft4,0)=0,0,1)+iif(isnull(sqft5,0)=0,0,1)+iif(isnull(sqft6,0)=0,0,1)+iif(isnull(sqft7,0)=0,0,1)+iif(isnull(sqft8,0)=0,0,1)+iif(isnull(sqft9,0)=0,0,1)+iif(isnull(sqft10,0)=0,0,1)+iif(isnull(sqft11,0)=0,0,1)+iif(isnull(sqft12,0)=0,0,1)+iif(isnull(sqft13,0)=0,0,1))=0,0,
			iif(isnull(psf2,0)+isnull(psf3,0)+isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)=0,0,
			(isnull(sqft2,0)+isnull(sqft3,0)+isnull(sqft4,0)+isnull(sqft5,0)+isnull(sqft6,0)+isnull(sqft7,0)+isnull(sqft8,0)+isnull(sqft9,0)+isnull(sqft10,0)+isnull(sqft11,0)+isnull(sqft12,0)+isnull(sqft13,0))/
			(isnull(psf2,0)+isnull(psf3,0)+isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0))
			) as R12GLA,
			iif(isnull(psf2,0)+isnull(psf3,0)+isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)=0,0,
			(isnull(asqft2,0)+isnull(asqft3,0)+isnull(asqft4,0)+isnull(asqft5,0)+isnull(asqft6,0)+isnull(asqft7,0)+isnull(asqft8,0)+isnull(asqft9,0)+isnull(asqft10,0)+isnull(asqft11,0)+isnull(asqft12,0)+isnull(asqft13,0))/
			(isnull(psf2,0)+isnull(psf3,0)+isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0))
			) as R12SalesGLA,
			(isnull(Amount2,0)+isnull(Amount3,0)+isnull(Amount4,0)+isnull(Amount5,0)+isnull(Amount6,0)+isnull(Amount7,0)+isnull(Amount8,0)+isnull(Amount9,0)+isnull(Amount10,0)+isnull(Amount11,0)+isnull(Amount12,0)+isnull(Amount13,0)) as R12Sales,
			(isnull(baserent2,0)+isnull(baserent3,0)+isnull(baserent4,0)+isnull(baserent5,0)+isnull(baserent6,0)+isnull(baserent7,0)+isnull(baserent8,0)+isnull(baserent9,0)+isnull(baserent10,0)+isnull(baserent11,0)+isnull(baserent12,0)+isnull(baserent13,0)) as R12BaseRent,
			(isnull(additional2,0)+isnull(additional3,0)+isnull(additional4,0)+isnull(additional5,0)+isnull(additional6,0)+isnull(additional7,0)+isnull(additional8,0)+isnull(additional9,0)+isnull(additional10,0)+isnull(additional11,0)+isnull(additional12,0)+isnull(additional13,0)) as  R12Additional   
			,stypid,psf2
			from #table where not amount2 is null and dateadd(month,-1,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-2,@StartDate) ,isnull(sqft3,0),isnull(asqft3,0),isnull(amount3,0) as sales,isnull(baserent3,0),isnull(additional3,0),isComparable3 ,iif(RentEnd<dateadd(month,-2,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft3,0)=0,0,1)+iif(isnull(sqft4,0)=0,0,1)+iif(isnull(sqft5,0)=0,0,1)+iif(isnull(sqft6,0)=0,0,1)+iif(isnull(sqft7,0)=0,0,1)+iif(isnull(sqft8,0)=0,0,1)+iif(isnull(sqft9,0)=0,0,1)+iif(isnull(sqft10,0)=0,0,1)+iif(isnull(sqft11,0)=0,0,1)+iif(isnull(sqft12,0)=0,0,1)+iif(isnull(sqft13,0)=0,0,1)+iif(isnull(sqft14,0)=0,0,1))=0,0,
			iif(isnull(psf3,0)+isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)=0,0,
			(isnull(sqft3,0)+isnull(sqft4,0)+isnull(sqft5,0)+isnull(sqft6,0)+isnull(sqft7,0)+isnull(sqft8,0)+isnull(sqft9,0)+isnull(sqft10,0)+isnull(sqft11,0)+isnull(sqft12,0)+isnull(sqft13,0)+isnull(sqft14,0))/
			(isnull(psf3,0)+isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0))
			) as R12GLA,
			iif(isnull(psf3,0)+isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)=0,0,
			(isnull(asqft3,0)+isnull(asqft4,0)+isnull(asqft5,0)+isnull(asqft6,0)+isnull(asqft7,0)+isnull(asqft8,0)+isnull(asqft9,0)+isnull(asqft10,0)+isnull(asqft11,0)+isnull(asqft12,0)+isnull(asqft13,0)+isnull(asqft14,0))/
			(isnull(psf3,0)+isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0))
			) as R12SalesGLA,
			(isnull(Amount3,0)+isnull(Amount4,0)+isnull(Amount5,0)+isnull(Amount6,0)+isnull(Amount7,0)+isnull(Amount8,0)+isnull(Amount9,0)+isnull(Amount10,0)+isnull(Amount11,0)+isnull(Amount12,0)+isnull(Amount13,0)+isnull(Amount14,0)) as R12Sales,
			(isnull(baserent3,0)+isnull(baserent4,0)+isnull(baserent5,0)+isnull(baserent6,0)+isnull(baserent7,0)+isnull(baserent8,0)+isnull(baserent9,0)+isnull(baserent10,0)+isnull(baserent11,0)+isnull(baserent12,0)+isnull(baserent13,0)+isnull(baserent14,0)) as R12BaseRent,
			(isnull(additional3,0)+isnull(additional4,0)+isnull(additional5,0)+isnull(additional6,0)+isnull(additional7,0)+isnull(additional8,0)+isnull(additional9,0)+isnull(additional10,0)+isnull(additional11,0)+isnull(additional12,0)+isnull(additional13,0)+isnull(additional14,0)) as  R12Additional   
			,stypid,psf3
			from #table where not amount3 is null and dateadd(month,-2,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-3,@StartDate) ,isnull(sqft4,0),isnull(asqft4,0),isnull(amount4,0) as sales,isnull(baserent4,0),isnull(additional4,0),isComparable4 ,iif(RentEnd<dateadd(month,-3,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft4,0)=0,0,1)+iif(isnull(sqft5,0)=0,0,1)+iif(isnull(sqft6,0)=0,0,1)+iif(isnull(sqft7,0)=0,0,1)+iif(isnull(sqft8,0)=0,0,1)+iif(isnull(sqft9,0)=0,0,1)+iif(isnull(sqft10,0)=0,0,1)+iif(isnull(sqft11,0)=0,0,1)+iif(isnull(sqft12,0)=0,0,1)+iif(isnull(sqft13,0)=0,0,1)+iif(isnull(sqft14,0)=0,0,1)+iif(isnull(sqft15,0)=0,0,1))=0,0,
			iif(isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)=0,0,
			(isnull(sqft4,0)+isnull(sqft5,0)+isnull(sqft6,0)+isnull(sqft7,0)+isnull(sqft8,0)+isnull(sqft9,0)+isnull(sqft10,0)+isnull(sqft11,0)+isnull(sqft12,0)+isnull(sqft13,0)+isnull(sqft14,0)+isnull(sqft15,0))/
			(isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0))
			) as R12GLA,
			iif(isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)=0,0,
			(isnull(asqft4,0)+isnull(asqft5,0)+isnull(asqft6,0)+isnull(asqft7,0)+isnull(asqft8,0)+isnull(asqft9,0)+isnull(asqft10,0)+isnull(asqft11,0)+isnull(asqft12,0)+isnull(asqft13,0)+isnull(asqft14,0)+isnull(asqft15,0))/
			(isnull(psf4,0)+isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0))
			) as R12SalesGLA,
			(isnull(Amount4,0)+isnull(Amount5,0)+isnull(Amount6,0)+isnull(Amount7,0)+isnull(Amount8,0)+isnull(Amount9,0)+isnull(Amount10,0)+isnull(Amount11,0)+isnull(Amount12,0)+isnull(Amount13,0)+isnull(Amount14,0)+isnull(Amount15,0)) as R12Sales,
			(isnull(baserent4,0)+isnull(baserent5,0)+isnull(baserent6,0)+isnull(baserent7,0)+isnull(baserent8,0)+isnull(baserent9,0)+isnull(baserent10,0)+isnull(baserent11,0)+isnull(baserent12,0)+isnull(baserent13,0)+isnull(baserent14,0)+isnull(baserent15,0)) as R12BaseRent,
			(isnull(additional4,0)+isnull(additional5,0)+isnull(additional6,0)+isnull(additional7,0)+isnull(additional8,0)+isnull(additional9,0)+isnull(additional10,0)+isnull(additional11,0)+isnull(additional12,0)+isnull(additional13,0)+isnull(additional14,0)+isnull(additional15,0)) as  R12Additional   
			,stypid,psf4
			from #table where not amount4 is null and dateadd(month,-3,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-4,@StartDate) ,isnull(sqft5,0),isnull(asqft5,0),isnull(amount5,0) as sales,isnull(baserent5,0),isnull(additional5,0),isComparable5,iif(RentEnd<dateadd(month,-4,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft5,0)=0,0,1)+iif(isnull(sqft6,0)=0,0,1)+iif(isnull(sqft7,0)=0,0,1)+iif(isnull(sqft8,0)=0,0,1)+iif(isnull(sqft9,0)=0,0,1)+iif(isnull(sqft10,0)=0,0,1)+iif(isnull(sqft11,0)=0,0,1)+iif(isnull(sqft12,0)=0,0,1)+iif(isnull(sqft13,0)=0,0,1)+iif(isnull(sqft14,0)=0,0,1)+iif(isnull(sqft15,0)=0,0,1)+iif(isnull(sqft16,0)=0,0,1))=0,0,
			iif(isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)=0,0,
			(isnull(sqft5,0)+isnull(sqft6,0)+isnull(sqft7,0)+isnull(sqft8,0)+isnull(sqft9,0)+isnull(sqft10,0)+isnull(sqft11,0)+isnull(sqft12,0)+isnull(sqft13,0)+isnull(sqft14,0)+isnull(sqft15,0)+isnull(sqft16,0))/
			(isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0))
			) as R12GLA,
			iif(isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)=0,0,
			(isnull(asqft5,0)+isnull(asqft6,0)+isnull(asqft7,0)+isnull(asqft8,0)+isnull(asqft9,0)+isnull(asqft10,0)+isnull(asqft11,0)+isnull(asqft12,0)+isnull(asqft13,0)+isnull(asqft14,0)+isnull(asqft15,0)+isnull(asqft16,0))/
			(isnull(psf5,0)+isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0))
			) as R12SalesGLA,
			(isnull(Amount5,0)+isnull(Amount6,0)+isnull(Amount7,0)+isnull(Amount8,0)+isnull(Amount9,0)+isnull(Amount10,0)+isnull(Amount11,0)+isnull(Amount12,0)+isnull(Amount13,0)+isnull(Amount14,0)+isnull(Amount15,0)+isnull(Amount16,0)) as R12Sales,
			(isnull(baserent5,0)+isnull(baserent6,0)+isnull(baserent7,0)+isnull(baserent8,0)+isnull(baserent9,0)+isnull(baserent10,0)+isnull(baserent11,0)+isnull(baserent12,0)+isnull(baserent13,0)+isnull(baserent14,0)+isnull(baserent15,0)+isnull(baserent16,0)) as R12BaseRent,
			(isnull(additional5,0)+isnull(additional6,0)+isnull(additional7,0)+isnull(additional8,0)+isnull(additional9,0)+isnull(additional10,0)+isnull(additional11,0)+isnull(additional12,0)+isnull(additional13,0)+isnull(additional14,0)+isnull(additional15,0)+isnull(additional16,0)) as  R12Additional    
			,stypid,psf5
			from #table where not amount5 is null and dateadd(month,-4,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-5,@StartDate) ,isnull(sqft6,0),isnull(asqft6,0),isnull(amount6,0) as sales,isnull(baserent6,0),isnull(additional6,0),isComparable6,iif(RentEnd<dateadd(month,-5,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft6,0)=0,0,1)+iif(isnull(sqft7,0)=0,0,1)+iif(isnull(sqft8,0)=0,0,1)+iif(isnull(sqft9,0)=0,0,1)+iif(isnull(sqft10,0)=0,0,1)+iif(isnull(sqft11,0)=0,0,1)+iif(isnull(sqft12,0)=0,0,1)+iif(isnull(sqft13,0)=0,0,1)+iif(isnull(sqft14,0)=0,0,1)+iif(isnull(sqft15,0)=0,0,1)+iif(isnull(sqft16,0)=0,0,1)+iif(isnull(sqft17,0)=0,0,1))=0,0,
			iif(isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)=0,0,
			(isnull(sqft6,0)+isnull(sqft7,0)+isnull(sqft8,0)+isnull(sqft9,0)+isnull(sqft10,0)+isnull(sqft11,0)+isnull(sqft12,0)+isnull(sqft13,0)+isnull(sqft14,0)+isnull(sqft15,0)+isnull(sqft16,0)+isnull(sqft17,0))/
			(isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0))
			) as R12GLA,
			iif(isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)=0,0,
			(isnull(asqft6,0)+isnull(asqft7,0)+isnull(asqft8,0)+isnull(asqft9,0)+isnull(asqft10,0)+isnull(asqft11,0)+isnull(asqft12,0)+isnull(asqft13,0)+isnull(asqft14,0)+isnull(asqft15,0)+isnull(asqft16,0)+isnull(asqft17,0))/
			(isnull(psf6,0)+isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0))
			) as R12SalesGLA,
			(isnull(Amount6,0)+isnull(Amount7,0)+isnull(Amount8,0)+isnull(Amount9,0)+isnull(Amount10,0)+isnull(Amount11,0)+isnull(Amount12,0)+isnull(Amount13,0)+isnull(Amount14,0)+isnull(Amount15,0)+isnull(Amount16,0)+isnull(Amount17,0)) as R12Sales,
			(isnull(baserent6,0)+isnull(baserent7,0)+isnull(baserent8,0)+isnull(baserent9,0)+isnull(baserent10,0)+isnull(baserent11,0)+isnull(baserent12,0)+isnull(baserent13,0)+isnull(baserent14,0)+isnull(baserent15,0)+isnull(baserent16,0)+isnull(baserent17,0)) as R12BaseRent,
			(isnull(additional6,0)+isnull(additional7,0)+isnull(additional8,0)+isnull(additional9,0)+isnull(additional10,0)+isnull(additional11,0)+isnull(additional12,0)+isnull(additional13,0)+isnull(additional14,0)+isnull(additional15,0)+isnull(additional16,0)+isnull(additional17,0)) as  R12Additional     
			,stypid,psf6
			from #table where not amount6 is null and dateadd(month,-5,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-6,@StartDate) ,isnull(sqft7,0),isnull(asqft7,0),isnull(amount7,0) as sales,isnull(baserent7,0),isnull(additional7,0),isComparable7,iif(RentEnd<dateadd(month,-6,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft7,0)=0,0,1)+iif(isnull(sqft8,0)=0,0,1)+iif(isnull(sqft9,0)=0,0,1)+iif(isnull(sqft10,0)=0,0,1)+iif(isnull(sqft11,0)=0,0,1)+iif(isnull(sqft12,0)=0,0,1)+iif(isnull(sqft13,0)=0,0,1)+iif(isnull(sqft14,0)=0,0,1)+iif(isnull(sqft15,0)=0,0,1)+iif(isnull(sqft16,0)=0,0,1)+iif(isnull(sqft17,0)=0,0,1)+iif(isnull(sqft18,0)=0,0,1))=0,0,
			iif(isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)=0,0,
			(isnull(sqft7,0)+isnull(sqft8,0)+isnull(sqft9,0)+isnull(sqft10,0)+isnull(sqft11,0)+isnull(sqft12,0)+isnull(sqft13,0)+isnull(sqft14,0)+isnull(sqft15,0)+isnull(sqft16,0)+isnull(sqft17,0)+isnull(sqft18,0))/
			(isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0))
			) as R12GLA,
			iif(isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)=0,0,
			(isnull(asqft7,0)+isnull(asqft8,0)+isnull(asqft9,0)+isnull(asqft10,0)+isnull(asqft11,0)+isnull(asqft12,0)+isnull(asqft13,0)+isnull(asqft14,0)+isnull(asqft15,0)+isnull(asqft16,0)+isnull(asqft17,0)+isnull(asqft18,0))/
			(isnull(psf7,0)+isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0))
			) as R12SalesGLA,
			(isnull(Amount7,0)+isnull(Amount8,0)+isnull(Amount9,0)+isnull(Amount10,0)+isnull(Amount11,0)+isnull(Amount12,0)+isnull(Amount13,0)+isnull(Amount14,0)+isnull(Amount15,0)+isnull(Amount16,0)+isnull(Amount17,0)+isnull(Amount18,0)) as R12Sales,
			(isnull(baserent7,0)+isnull(baserent8,0)+isnull(baserent9,0)+isnull(baserent10,0)+isnull(baserent11,0)+isnull(baserent12,0)+isnull(baserent13,0)+isnull(baserent14,0)+isnull(baserent15,0)+isnull(baserent16,0)+isnull(baserent17,0)+isnull(baserent18,0)) as R12BaseRent,
			(isnull(additional7,0)+isnull(additional8,0)+isnull(additional9,0)+isnull(additional10,0)+isnull(additional11,0)+isnull(additional12,0)+isnull(additional13,0)+isnull(additional14,0)+isnull(additional15,0)+isnull(additional16,0)+isnull(additional17,0)+isnull(additional18,0)) as  R12Additional      
			,stypid,psf7
			from #table where not amount7 is null and dateadd(month,-6,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-7,@StartDate) ,isnull(sqft8,0),isnull(asqft8,0),isnull(amount8,0) as sales,isnull(baserent8,0),isnull(additional8,0),isComparable8,iif(RentEnd<dateadd(month,-7,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft8,0)=0,0,1)+iif(isnull(sqft9,0)=0,0,1)+iif(isnull(sqft10,0)=0,0,1)+iif(isnull(sqft11,0)=0,0,1)+iif(isnull(sqft12,0)=0,0,1)+iif(isnull(sqft13,0)=0,0,1)+iif(isnull(sqft14,0)=0,0,1)+iif(isnull(sqft15,0)=0,0,1)+iif(isnull(sqft16,0)=0,0,1)+iif(isnull(sqft17,0)=0,0,1)+iif(isnull(sqft18,0)=0,0,1)+iif(isnull(sqft19,0)=0,0,1))=0,0,
			iif(isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)=0,0,
			(isnull(sqft8,0)+isnull(sqft9,0)+isnull(sqft10,0)+isnull(sqft11,0)+isnull(sqft12,0)+isnull(sqft13,0)+isnull(sqft14,0)+isnull(sqft15,0)+isnull(sqft16,0)+isnull(sqft17,0)+isnull(sqft18,0)+isnull(sqft19,0))/
			(isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0))
			) as R12GLA,
			iif(isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)=0,0,
			(isnull(asqft8,0)+isnull(asqft9,0)+isnull(asqft10,0)+isnull(asqft11,0)+isnull(asqft12,0)+isnull(asqft13,0)+isnull(asqft14,0)+isnull(asqft15,0)+isnull(asqft16,0)+isnull(asqft17,0)+isnull(asqft18,0)+isnull(asqft19,0))/
			(isnull(psf8,0)+isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0))
			) as R12SalesGLA,
			(isnull(Amount8,0)+isnull(Amount9,0)+isnull(Amount10,0)+isnull(Amount11,0)+isnull(Amount12,0)+isnull(Amount13,0)+isnull(Amount14,0)+isnull(Amount15,0)+isnull(Amount16,0)+isnull(Amount17,0)+isnull(Amount18,0)+isnull(Amount19,0)) as R12Sales,
			(isnull(baserent8,0)+isnull(baserent9,0)+isnull(baserent10,0)+isnull(baserent11,0)+isnull(baserent12,0)+isnull(baserent13,0)+isnull(baserent14,0)+isnull(baserent15,0)+isnull(baserent16,0)+isnull(baserent17,0)+isnull(baserent18,0)+isnull(baserent19,0)) as R12BaseRent,
			(isnull(additional8,0)+isnull(additional9,0)+isnull(additional10,0)+isnull(additional11,0)+isnull(additional12,0)+isnull(additional13,0)+isnull(additional14,0)+isnull(additional15,0)+isnull(additional16,0)+isnull(additional17,0)+isnull(additional18,0)+isnull(additional19,0)) as  R12Additional       
			,stypid,psf8
			from #table where not amount8 is null and dateadd(month,-7,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-8,@StartDate) ,isnull(sqft9,0),isnull(asqft9,0),isnull(amount9,0) as sales,isnull(baserent9,0),isnull(additional9,0),isComparable9,iif(RentEnd<dateadd(month,-8,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft9,0)=0,0,1)+iif(isnull(sqft10,0)=0,0,1)+iif(isnull(sqft11,0)=0,0,1)+iif(isnull(sqft12,0)=0,0,1)+iif(isnull(sqft13,0)=0,0,1)+iif(isnull(sqft14,0)=0,0,1)+iif(isnull(sqft15,0)=0,0,1)+iif(isnull(sqft16,0)=0,0,1)+iif(isnull(sqft17,0)=0,0,1)+iif(isnull(sqft18,0)=0,0,1)+iif(isnull(sqft19,0)=0,0,1)+iif(isnull(sqft20,0)=0,0,1))=0,0,
			iif(isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)=0,0,
			(isnull(sqft9,0)+isnull(sqft10,0)+isnull(sqft11,0)+isnull(sqft12,0)+isnull(sqft13,0)+isnull(sqft14,0)+isnull(sqft15,0)+isnull(sqft16,0)+isnull(sqft17,0)+isnull(sqft18,0)+isnull(sqft19,0)+isnull(sqft20,0))/
			(isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0))
			) as R12GLA,
			iif(isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)=0,0,
			(isnull(asqft9,0)+isnull(asqft10,0)+isnull(asqft11,0)+isnull(asqft12,0)+isnull(asqft13,0)+isnull(asqft14,0)+isnull(asqft15,0)+isnull(asqft16,0)+isnull(asqft17,0)+isnull(asqft18,0)+isnull(asqft19,0)+isnull(asqft20,0))/
			(isnull(psf9,0)+isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0))
			) as R12SalesGLA,
			(isnull(Amount9,0)+isnull(Amount10,0)+isnull(Amount11,0)+isnull(Amount12,0)+isnull(Amount13,0)+isnull(Amount14,0)+isnull(Amount15,0)+isnull(Amount16,0)+isnull(Amount17,0)+isnull(Amount18,0)+isnull(Amount19,0)+isnull(Amount20,0)) as R12Sales,
			(isnull(baserent9,0)+isnull(baserent10,0)+isnull(baserent11,0)+isnull(baserent12,0)+isnull(baserent13,0)+isnull(baserent14,0)+isnull(baserent15,0)+isnull(baserent16,0)+isnull(baserent17,0)+isnull(baserent18,0)+isnull(baserent19,0)+isnull(baserent20,0)) as R12BaseRent,
			(isnull(additional9,0)+isnull(additional10,0)+isnull(additional11,0)+isnull(additional12,0)+isnull(additional13,0)+isnull(additional14,0)+isnull(additional15,0)+isnull(additional16,0)+isnull(additional17,0)+isnull(additional18,0)+isnull(additional19,0)+isnull(additional20,0)) as  R12Additional        
			,stypid,psf9
			from #table where not amount9 is null and dateadd(month,-8,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-9,@StartDate) ,isnull(sqft10,0),isnull(asqft10,0),isnull(amount10,0) as sales,isnull(baserent10,0),isnull(additional10,0),isComparable10,iif(RentEnd<dateadd(month,-9,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft10,0)=0,0,1)+iif(isnull(sqft11,0)=0,0,1)+iif(isnull(sqft12,0)=0,0,1)+iif(isnull(sqft13,0)=0,0,1)+iif(isnull(sqft14,0)=0,0,1)+iif(isnull(sqft15,0)=0,0,1)+iif(isnull(sqft16,0)=0,0,1)+iif(isnull(sqft17,0)=0,0,1)+iif(isnull(sqft18,0)=0,0,1)+iif(isnull(sqft19,0)=0,0,1)+iif(isnull(sqft20,0)=0,0,1)+iif(isnull(sqft21,0)=0,0,1))=0,0,
			iif(isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)=0,0,
			(isnull(sqft10,0)+isnull(sqft11,0)+isnull(sqft12,0)+isnull(sqft13,0)+isnull(sqft14,0)+isnull(sqft15,0)+isnull(sqft16,0)+isnull(sqft17,0)+isnull(sqft18,0)+isnull(sqft19,0)+isnull(sqft20,0)+isnull(sqft21,0))/
			(isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0))
			) as R12GLA,
			iif(isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)=0,0,
			(isnull(asqft10,0)+isnull(asqft11,0)+isnull(asqft12,0)+isnull(asqft13,0)+isnull(asqft14,0)+isnull(asqft15,0)+isnull(asqft16,0)+isnull(asqft17,0)+isnull(asqft18,0)+isnull(asqft19,0)+isnull(asqft20,0)+isnull(asqft21,0))/
			(isnull(psf10,0)+isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0))
			) as R12SalesGLA,
			(isnull(Amount10,0)+isnull(Amount11,0)+isnull(Amount12,0)+isnull(Amount13,0)+isnull(Amount14,0)+isnull(Amount15,0)+isnull(Amount16,0)+isnull(Amount17,0)+isnull(Amount18,0)+isnull(Amount19,0)+isnull(Amount20,0)+isnull(Amount21,0)) as R12Sales,
			(isnull(baserent10,0)+isnull(baserent11,0)+isnull(baserent12,0)+isnull(baserent13,0)+isnull(baserent14,0)+isnull(baserent15,0)+isnull(baserent16,0)+isnull(baserent17,0)+isnull(baserent18,0)+isnull(baserent19,0)+isnull(baserent20,0)+isnull(baserent21,0)) as R12BaseRent,
			(isnull(additional10,0)+isnull(additional11,0)+isnull(additional12,0)+isnull(additional13,0)+isnull(additional14,0)+isnull(additional15,0)+isnull(additional16,0)+isnull(additional17,0)+isnull(additional18,0)+isnull(additional19,0)+isnull(additional20,0)+isnull(additional21,0)) as  R12Additional         
			,stypid,psf10
			from #table where not amount10 is null and dateadd(month,-9,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-10,@StartDate) ,isnull(sqft11,0),isnull(asqft11,0),isnull(amount11,0) as sales,isnull(baserent11,0),isnull(additional11,0),isComparable11,iif(RentEnd<dateadd(month,-10,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft11,0)=0,0,1)+iif(isnull(sqft12,0)=0,0,1)+iif(isnull(sqft13,0)=0,0,1)+iif(isnull(sqft14,0)=0,0,1)+iif(isnull(sqft15,0)=0,0,1)+iif(isnull(sqft16,0)=0,0,1)+iif(isnull(sqft17,0)=0,0,1)+iif(isnull(sqft18,0)=0,0,1)+iif(isnull(sqft19,0)=0,0,1)+iif(isnull(sqft20,0)=0,0,1)+iif(isnull(sqft21,0)=0,0,1)+iif(isnull(sqft22,0)=0,0,1))=0,0,
			iif(isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)=0,0,
			(isnull(sqft11,0)+isnull(sqft12,0)+isnull(sqft13,0)+isnull(sqft14,0)+isnull(sqft15,0)+isnull(sqft16,0)+isnull(sqft17,0)+isnull(sqft18,0)+isnull(sqft19,0)+isnull(sqft20,0)+isnull(sqft21,0)+isnull(sqft22,0))/
			(isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0))
			) as R12GLA,
			iif(isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)=0,0,
			(isnull(asqft11,0)+isnull(asqft12,0)+isnull(asqft13,0)+isnull(asqft14,0)+isnull(asqft15,0)+isnull(asqft16,0)+isnull(asqft17,0)+isnull(asqft18,0)+isnull(asqft19,0)+isnull(asqft20,0)+isnull(asqft21,0)+isnull(asqft22,0))/
			(isnull(psf11,0)+isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0))
			) as R12SalesGLA,
			(isnull(Amount11,0)+isnull(Amount12,0)+isnull(Amount13,0)+isnull(Amount14,0)+isnull(Amount15,0)+isnull(Amount16,0)+isnull(Amount17,0)+isnull(Amount18,0)+isnull(Amount19,0)+isnull(Amount20,0)+isnull(Amount21,0)+isnull(Amount22,0)) as R12Sales,
			(isnull(baserent11,0)+isnull(baserent12,0)+isnull(baserent13,0)+isnull(baserent14,0)+isnull(baserent15,0)+isnull(baserent16,0)+isnull(baserent17,0)+isnull(baserent18,0)+isnull(baserent19,0)+isnull(baserent20,0)+isnull(baserent21,0)+isnull(baserent22,0)) as R12BaseRent,
			(isnull(additional11,0)+isnull(additional12,0)+isnull(additional13,0)+isnull(additional14,0)+isnull(additional15,0)+isnull(additional16,0)+isnull(additional17,0)+isnull(additional18,0)+isnull(additional19,0)+isnull(additional20,0)+isnull(additional21,0)+isnull(additional22,0)) as  R12Additional          
			,stypid,psf11
			from #table where not amount11 is null and dateadd(month,-10,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-11,@StartDate) ,isnull(sqft12,0),isnull(asqft12,0),isnull(amount12,0) as sales,isnull(baserent12,0),isnull(additional12,0),isComparable12,iif(RentEnd<dateadd(month,-11,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft12,0)=0,0,1)+iif(isnull(sqft13,0)=0,0,1)+iif(isnull(sqft14,0)=0,0,1)+iif(isnull(sqft15,0)=0,0,1)+iif(isnull(sqft16,0)=0,0,1)+iif(isnull(sqft17,0)=0,0,1)+iif(isnull(sqft18,0)=0,0,1)+iif(isnull(sqft19,0)=0,0,1)+iif(isnull(sqft20,0)=0,0,1)+iif(isnull(sqft21,0)=0,0,1)+iif(isnull(sqft22,0)=0,0,1)+iif(isnull(sqft23,0)=0,0,1))=0,0,
			iif(isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)=0,0,
			(isnull(sqft12,0)+isnull(sqft13,0)+isnull(sqft14,0)+isnull(sqft15,0)+isnull(sqft16,0)+isnull(sqft17,0)+isnull(sqft18,0)+isnull(sqft19,0)+isnull(sqft20,0)+isnull(sqft21,0)+isnull(sqft22,0)+isnull(sqft23,0))/
			(isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0))
			) as R12GLA,
			iif(isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)=0,0,
			(isnull(asqft12,0)+isnull(asqft13,0)+isnull(asqft14,0)+isnull(asqft15,0)+isnull(asqft16,0)+isnull(asqft17,0)+isnull(asqft18,0)+isnull(asqft19,0)+isnull(asqft20,0)+isnull(asqft21,0)+isnull(asqft22,0)+isnull(asqft23,0))/
			(isnull(psf12,0)+isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0))
			) as R12SalesGLA,
			(isnull(Amount12,0)+isnull(Amount13,0)+isnull(Amount14,0)+isnull(Amount15,0)+isnull(Amount16,0)+isnull(Amount17,0)+isnull(Amount18,0)+isnull(Amount19,0)+isnull(Amount20,0)+isnull(Amount21,0)+isnull(Amount22,0)+isnull(Amount23,0)) as R12Sales,
			(isnull(baserent12,0)+isnull(baserent13,0)+isnull(baserent14,0)+isnull(baserent15,0)+isnull(baserent16,0)+isnull(baserent17,0)+isnull(baserent18,0)+isnull(baserent19,0)+isnull(baserent20,0)+isnull(baserent21,0)+isnull(baserent22,0)+isnull(baserent23,0)) as R12BaseRent,
			(isnull(additional12,0)+isnull(additional13,0)+isnull(additional14,0)+isnull(additional15,0)+isnull(additional16,0)+isnull(additional17,0)+isnull(additional18,0)+isnull(additional19,0)+isnull(additional20,0)+isnull(additional21,0)+isnull(additional22,0)+isnull(additional23,0)) as  R12Additional           
			,stypid,psf12
			from #table where not amount12 is null and dateadd(month,-11,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-12,@StartDate) ,isnull(sqft13,0),isnull(asqft13,0),isnull(Amount13,0) as sales,isnull(baserent13,0),isnull(additional13,0),isComparable13,iif(RentEnd<dateadd(month,-12,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft13,0)=0,0,1)+iif(isnull(sqft14,0)=0,0,1)+iif(isnull(sqft15,0)=0,0,1)+iif(isnull(sqft16,0)=0,0,1)+iif(isnull(sqft17,0)=0,0,1)+iif(isnull(sqft18,0)=0,0,1)+iif(isnull(sqft19,0)=0,0,1)+iif(isnull(sqft20,0)=0,0,1)+iif(isnull(sqft21,0)=0,0,1)+iif(isnull(sqft22,0)=0,0,1)+iif(isnull(sqft23,0)=0,0,1)+iif(isnull(sqft24,0)=0,0,1))=0,0,
			iif(isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)=0,0,
			(isnull(sqft13,0)+isnull(sqft14,0)+isnull(sqft15,0)+isnull(sqft16,0)+isnull(sqft17,0)+isnull(sqft18,0)+isnull(sqft19,0)+isnull(sqft20,0)+isnull(sqft21,0)+isnull(sqft22,0)+isnull(sqft23,0)+isnull(sqft24,0))/
			(isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0))
			) as R12GLA,
			iif(isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)=0,0,
			(isnull(asqft13,0)+isnull(asqft14,0)+isnull(asqft15,0)+isnull(asqft16,0)+isnull(asqft17,0)+isnull(asqft18,0)+isnull(asqft19,0)+isnull(asqft20,0)+isnull(asqft21,0)+isnull(asqft22,0)+isnull(asqft23,0)+isnull(asqft24,0))/
			(isnull(psf13,0)+isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0))
			) as R12SalesGLA,
			(isnull(Amount13,0)+isnull(Amount14,0)+isnull(Amount15,0)+isnull(Amount16,0)+isnull(Amount17,0)+isnull(Amount18,0)+isnull(Amount19,0)+isnull(Amount20,0)+isnull(Amount21,0)+isnull(Amount22,0)+isnull(Amount23,0)+isnull(Amount24,0)) as R12Sales,
			(isnull(baserent13,0)+isnull(baserent14,0)+isnull(baserent15,0)+isnull(baserent16,0)+isnull(baserent17,0)+isnull(baserent18,0)+isnull(baserent19,0)+isnull(baserent20,0)+isnull(baserent21,0)+isnull(baserent22,0)+isnull(baserent23,0)+isnull(baserent24,0)) as R12BaseRent,
			(isnull(additional13,0)+isnull(additional14,0)+isnull(additional15,0)+isnull(additional16,0)+isnull(additional17,0)+isnull(additional18,0)+isnull(additional19,0)+isnull(additional20,0)+isnull(additional21,0)+isnull(additional22,0)+isnull(additional23,0)+isnull(additional24,0)) as  R12Additional            
			,stypid,psf13
			from #table where not amount13 is null and dateadd(month,-12,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-13,@StartDate) ,isnull(sqft14,0),isnull(asqft14,0),isnull(Amount14,0) as sales,isnull(baserent14,0),isnull(additional14,0),isComparable14,iif(RentEnd<dateadd(month,-13,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft14,0)=0,0,1)+iif(isnull(sqft15,0)=0,0,1)+iif(isnull(sqft16,0)=0,0,1)+iif(isnull(sqft17,0)=0,0,1)+iif(isnull(sqft18,0)=0,0,1)+iif(isnull(sqft19,0)=0,0,1)+iif(isnull(sqft20,0)=0,0,1)+iif(isnull(sqft21,0)=0,0,1)+iif(isnull(sqft22,0)=0,0,1)+iif(isnull(sqft23,0)=0,0,1)+iif(isnull(sqft24,0)=0,0,1)+iif(isnull(sqft25,0)=0,0,1))=0,0,
			iif(isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)=0,0,
			(isnull(sqft14,0)+isnull(sqft15,0)+isnull(sqft16,0)+isnull(sqft17,0)+isnull(sqft18,0)+isnull(sqft19,0)+isnull(sqft20,0)+isnull(sqft21,0)+isnull(sqft22,0)+isnull(sqft23,0)+isnull(sqft24,0)+isnull(sqft25,0))/
			(isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0))
			) as R12GLA,
			iif(isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)=0,0,
			(isnull(asqft14,0)+isnull(asqft15,0)+isnull(asqft16,0)+isnull(asqft17,0)+isnull(asqft18,0)+isnull(asqft19,0)+isnull(asqft20,0)+isnull(asqft21,0)+isnull(asqft22,0)+isnull(asqft23,0)+isnull(asqft24,0)+isnull(asqft25,0))/
			(isnull(psf14,0)+isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0))
			) as R12SalesGLA,
			(isnull(Amount14,0)+isnull(Amount15,0)+isnull(Amount16,0)+isnull(Amount17,0)+isnull(Amount18,0)+isnull(Amount19,0)+isnull(Amount20,0)+isnull(Amount21,0)+isnull(Amount22,0)+isnull(Amount23,0)+isnull(Amount24,0)+isnull(Amount25,0)) as R12Sales,
			(isnull(baserent14,0)+isnull(baserent15,0)+isnull(baserent16,0)+isnull(baserent17,0)+isnull(baserent18,0)+isnull(baserent19,0)+isnull(baserent20,0)+isnull(baserent21,0)+isnull(baserent22,0)+isnull(baserent23,0)+isnull(baserent24,0)+isnull(baserent25,0)) as R12BaseRent,
			(isnull(additional14,0)+isnull(additional15,0)+isnull(additional16,0)+isnull(additional17,0)+isnull(additional18,0)+isnull(additional19,0)+isnull(additional20,0)+isnull(additional21,0)+isnull(additional22,0)+isnull(additional23,0)+isnull(additional24,0)+isnull(additional25,0)) as  R12Additional             
			,stypid,psf14
			from #table where not amount14 is null and dateadd(month,-13,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-14,@StartDate) ,isnull(sqft15,0),isnull(asqft15,0),isnull(Amount15,0) as sales,isnull(baserent15,0),isnull(additional15,0),isComparable15,iif(RentEnd<dateadd(month,-14,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft15,0)=0,0,1)+iif(isnull(sqft16,0)=0,0,1)+iif(isnull(sqft17,0)=0,0,1)+iif(isnull(sqft18,0)=0,0,1)+iif(isnull(sqft19,0)=0,0,1)+iif(isnull(sqft20,0)=0,0,1)+iif(isnull(sqft21,0)=0,0,1)+iif(isnull(sqft22,0)=0,0,1)+iif(isnull(sqft23,0)=0,0,1)+iif(isnull(sqft24,0)=0,0,1)+iif(isnull(sqft25,0)=0,0,1)+iif(isnull(sqft26,0)=0,0,1))=0,0,
			iif(isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)=0,0,
			(isnull(sqft15,0)+isnull(sqft16,0)+isnull(sqft17,0)+isnull(sqft18,0)+isnull(sqft19,0)+isnull(sqft20,0)+isnull(sqft21,0)+isnull(sqft22,0)+isnull(sqft23,0)+isnull(sqft24,0)+isnull(sqft25,0)+isnull(sqft26,0))/
			(isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0))
			) as R12GLA,
			iif(isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)=0,0,
			(isnull(asqft15,0)+isnull(asqft16,0)+isnull(asqft17,0)+isnull(asqft18,0)+isnull(asqft19,0)+isnull(asqft20,0)+isnull(asqft21,0)+isnull(asqft22,0)+isnull(asqft23,0)+isnull(asqft24,0)+isnull(asqft25,0)+isnull(asqft26,0))/
			(isnull(psf15,0)+isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0))
			) as R12SalesGLA,
			(isnull(Amount15,0)+isnull(Amount16,0)+isnull(Amount17,0)+isnull(Amount18,0)+isnull(Amount19,0)+isnull(Amount20,0)+isnull(Amount21,0)+isnull(Amount22,0)+isnull(Amount23,0)+isnull(Amount24,0)+isnull(Amount25,0)+isnull(Amount26,0)) as R12Sales,
			(isnull(baserent15,0)+isnull(baserent16,0)+isnull(baserent17,0)+isnull(baserent18,0)+isnull(baserent19,0)+isnull(baserent20,0)+isnull(baserent21,0)+isnull(baserent22,0)+isnull(baserent23,0)+isnull(baserent24,0)+isnull(baserent25,0)+isnull(baserent26,0)) as R12BaseRent,
			(isnull(additional15,0)+isnull(additional16,0)+isnull(additional17,0)+isnull(additional18,0)+isnull(additional19,0)+isnull(additional20,0)+isnull(additional21,0)+isnull(additional22,0)+isnull(additional23,0)+isnull(additional24,0)+isnull(additional25,0)+isnull(additional26,0)) as  R12Additional  
			,stypid,psf15
			from #table where not amount15 is null and dateadd(month,-14,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-15,@StartDate) ,isnull(sqft16,0),isnull(asqft16,0),isnull(Amount16,0) as sales,isnull(baserent16,0),isnull(additional16,0),isComparable16,iif(RentEnd<dateadd(month,-15,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft16,0)=0,0,1)+iif(isnull(sqft17,0)=0,0,1)+iif(isnull(sqft18,0)=0,0,1)+iif(isnull(sqft19,0)=0,0,1)+iif(isnull(sqft20,0)=0,0,1)+iif(isnull(sqft21,0)=0,0,1)+iif(isnull(sqft22,0)=0,0,1)+iif(isnull(sqft23,0)=0,0,1)+iif(isnull(sqft24,0)=0,0,1)+iif(isnull(sqft25,0)=0,0,1)+iif(isnull(sqft26,0)=0,0,1)+iif(isnull(sqft27,0)=0,0,1))=0,0,
			iif(isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)=0,0,
			(isnull(sqft16,0)+isnull(sqft17,0)+isnull(sqft18,0)+isnull(sqft19,0)+isnull(sqft20,0)+isnull(sqft21,0)+isnull(sqft22,0)+isnull(sqft23,0)+isnull(sqft24,0)+isnull(sqft25,0)+isnull(sqft26,0)+isnull(sqft27,0))/
			(isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0))
			) as R12GLA,
			iif(isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)=0,0,
			(isnull(asqft16,0)+isnull(asqft17,0)+isnull(asqft18,0)+isnull(asqft19,0)+isnull(asqft20,0)+isnull(asqft21,0)+isnull(asqft22,0)+isnull(asqft23,0)+isnull(asqft24,0)+isnull(asqft25,0)+isnull(asqft26,0)+isnull(asqft27,0))/
			(isnull(psf16,0)+isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0))
			) as R12SalesGLA,
			(isnull(Amount16,0)+isnull(Amount17,0)+isnull(Amount18,0)+isnull(Amount19,0)+isnull(Amount20,0)+isnull(Amount21,0)+isnull(Amount22,0)+isnull(Amount23,0)+isnull(Amount24,0)+isnull(Amount25,0)+isnull(Amount26,0)+isnull(Amount27,0)) as R12Sales,
			(isnull(baserent16,0)+isnull(baserent17,0)+isnull(baserent18,0)+isnull(baserent19,0)+isnull(baserent20,0)+isnull(baserent21,0)+isnull(baserent22,0)+isnull(baserent23,0)+isnull(baserent24,0)+isnull(baserent25,0)+isnull(baserent26,0)+isnull(baserent27,0)) as R12BaseRent,
			(isnull(additional16,0)+isnull(additional17,0)+isnull(additional18,0)+isnull(additional19,0)+isnull(additional20,0)+isnull(additional21,0)+isnull(additional22,0)+isnull(additional23,0)+isnull(additional24,0)+isnull(additional25,0)+isnull(additional26,0)+isnull(additional27,0)) as  R12Additional   
			,stypid,psf16
			from #table where not amount16 is null and dateadd(month,-15,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-16,@StartDate) ,isnull(sqft17,0),isnull(asqft17,0),isnull(Amount17,0) as sales,isnull(baserent17,0),isnull(additional17,0),isComparable17,iif(RentEnd<dateadd(month,-16,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft17,0)=0,0,1)+iif(isnull(sqft18,0)=0,0,1)+iif(isnull(sqft19,0)=0,0,1)+iif(isnull(sqft20,0)=0,0,1)+iif(isnull(sqft21,0)=0,0,1)+iif(isnull(sqft22,0)=0,0,1)+iif(isnull(sqft23,0)=0,0,1)+iif(isnull(sqft24,0)=0,0,1)+iif(isnull(sqft25,0)=0,0,1)+iif(isnull(sqft26,0)=0,0,1)+iif(isnull(sqft27,0)=0,0,1)+iif(isnull(sqft28,0)=0,0,1))=0,0,
			iif(isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)=0,0,
			(isnull(sqft17,0)+isnull(sqft18,0)+isnull(sqft19,0)+isnull(sqft20,0)+isnull(sqft21,0)+isnull(sqft22,0)+isnull(sqft23,0)+isnull(sqft24,0)+isnull(sqft25,0)+isnull(sqft26,0)+isnull(sqft27,0)+isnull(sqft28,0))/
			(isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0))
			) as R12GLA,
			iif(isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)=0,0,
			(isnull(asqft17,0)+isnull(asqft18,0)+isnull(asqft19,0)+isnull(asqft20,0)+isnull(asqft21,0)+isnull(asqft22,0)+isnull(asqft23,0)+isnull(asqft24,0)+isnull(asqft25,0)+isnull(asqft26,0)+isnull(asqft27,0)+isnull(asqft28,0))/
			(isnull(psf17,0)+isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0))
			) as R12SalesGLA,
			(isnull(Amount17,0)+isnull(Amount18,0)+isnull(Amount19,0)+isnull(Amount20,0)+isnull(Amount21,0)+isnull(Amount22,0)+isnull(Amount23,0)+isnull(Amount24,0)+isnull(Amount25,0)+isnull(Amount26,0)+isnull(Amount27,0)+isnull(Amount28,0)) as R12Sales,
			(isnull(baserent17,0)+isnull(baserent18,0)+isnull(baserent19,0)+isnull(baserent20,0)+isnull(baserent21,0)+isnull(baserent22,0)+isnull(baserent23,0)+isnull(baserent24,0)+isnull(baserent25,0)+isnull(baserent26,0)+isnull(baserent27,0)+isnull(baserent28,0)) as R12BaseRent,
			(isnull(additional17,0)+isnull(additional18,0)+isnull(additional19,0)+isnull(additional20,0)+isnull(additional21,0)+isnull(additional22,0)+isnull(additional23,0)+isnull(additional24,0)+isnull(additional25,0)+isnull(additional26,0)+isnull(additional27,0)+isnull(additional28,0)) as  R12Additional    
			,stypid,psf17
			from #table where not amount17 is null and dateadd(month,-16,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-17,@StartDate) ,isnull(sqft18,0),isnull(asqft18,0),isnull(Amount18,0) as sales,isnull(baserent18,0),isnull(additional18,0),isComparable18,iif(RentEnd<dateadd(month,-17,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft18,0)=0,0,1)+iif(isnull(sqft19,0)=0,0,1)+iif(isnull(sqft20,0)=0,0,1)+iif(isnull(sqft21,0)=0,0,1)+iif(isnull(sqft22,0)=0,0,1)+iif(isnull(sqft23,0)=0,0,1)+iif(isnull(sqft24,0)=0,0,1)+iif(isnull(sqft25,0)=0,0,1)+iif(isnull(sqft26,0)=0,0,1)+iif(isnull(sqft27,0)=0,0,1)+iif(isnull(sqft28,0)=0,0,1)+iif(isnull(sqft29,0)=0,0,1))=0,0,
			iif(isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)=0,0,
			(isnull(sqft18,0)+isnull(sqft19,0)+isnull(sqft20,0)+isnull(sqft21,0)+isnull(sqft22,0)+isnull(sqft23,0)+isnull(sqft24,0)+isnull(sqft25,0)+isnull(sqft26,0)+isnull(sqft27,0)+isnull(sqft28,0)+isnull(sqft29,0))/
			(isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0))
			) as R12GLA,
			iif(isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)=0,0,
			(isnull(asqft18,0)+isnull(asqft19,0)+isnull(asqft20,0)+isnull(asqft21,0)+isnull(asqft22,0)+isnull(asqft23,0)+isnull(asqft24,0)+isnull(asqft25,0)+isnull(asqft26,0)+isnull(asqft27,0)+isnull(asqft28,0)+isnull(asqft29,0))/
			(isnull(psf18,0)+isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0))
			) as R12SalesGLA,
			(isnull(Amount18,0)+isnull(Amount19,0)+isnull(Amount20,0)+isnull(Amount21,0)+isnull(Amount22,0)+isnull(Amount23,0)+isnull(Amount24,0)+isnull(Amount25,0)+isnull(Amount26,0)+isnull(Amount27,0)+isnull(Amount28,0)+isnull(Amount29,0)) as R12Sales,
			(isnull(baserent18,0)+isnull(baserent19,0)+isnull(baserent20,0)+isnull(baserent21,0)+isnull(baserent22,0)+isnull(baserent23,0)+isnull(baserent24,0)+isnull(baserent25,0)+isnull(baserent26,0)+isnull(baserent27,0)+isnull(baserent28,0)+isnull(baserent29,0)) as R12BaseRent,
			(isnull(additional18,0)+isnull(additional19,0)+isnull(additional20,0)+isnull(additional21,0)+isnull(additional22,0)+isnull(additional23,0)+isnull(additional24,0)+isnull(additional25,0)+isnull(additional26,0)+isnull(additional27,0)+isnull(additional28,0)+isnull(additional29,0)) as  R12Additional     
			,stypid,psf18
			from #table where not amount18 is null and dateadd(month,-17,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-18,@StartDate) ,isnull(sqft19,0),isnull(asqft19,0),isnull(Amount19,0) as sales,isnull(baserent19,0),isnull(additional19,0),isComparable19,iif(RentEnd<dateadd(month,-18,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft19,0)=0,0,1)+iif(isnull(sqft20,0)=0,0,1)+iif(isnull(sqft21,0)=0,0,1)+iif(isnull(sqft22,0)=0,0,1)+iif(isnull(sqft23,0)=0,0,1)+iif(isnull(sqft24,0)=0,0,1)+iif(isnull(sqft25,0)=0,0,1)+iif(isnull(sqft26,0)=0,0,1)+iif(isnull(sqft27,0)=0,0,1)+iif(isnull(sqft28,0)=0,0,1)+iif(isnull(sqft29,0)=0,0,1)+iif(isnull(sqft30,0)=0,0,1))=0,0,
			iif(isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)=0,0,
			(isnull(sqft19,0)+isnull(sqft20,0)+isnull(sqft21,0)+isnull(sqft22,0)+isnull(sqft23,0)+isnull(sqft24,0)+isnull(sqft25,0)+isnull(sqft26,0)+isnull(sqft27,0)+isnull(sqft28,0)+isnull(sqft29,0)+isnull(sqft30,0))/
			(isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0))
			) as R12GLA,
			iif(isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)=0,0,
			(isnull(asqft19,0)+isnull(asqft20,0)+isnull(asqft21,0)+isnull(asqft22,0)+isnull(asqft23,0)+isnull(asqft24,0)+isnull(asqft25,0)+isnull(asqft26,0)+isnull(asqft27,0)+isnull(asqft28,0)+isnull(asqft29,0)+isnull(asqft30,0))/
			(isnull(psf19,0)+isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0))
			) as R12SalesGLA,
			(isnull(Amount19,0)+isnull(Amount20,0)+isnull(Amount21,0)+isnull(Amount22,0)+isnull(Amount23,0)+isnull(Amount24,0)+isnull(Amount25,0)+isnull(Amount26,0)+isnull(Amount27,0)+isnull(Amount28,0)+isnull(Amount29,0)+isnull(Amount30,0)) as R12Sales,
			(isnull(baserent19,0)+isnull(baserent20,0)+isnull(baserent21,0)+isnull(baserent22,0)+isnull(baserent23,0)+isnull(baserent24,0)+isnull(baserent25,0)+isnull(baserent26,0)+isnull(baserent27,0)+isnull(baserent28,0)+isnull(baserent29,0)+isnull(baserent30,0)) as R12BaseRent,
			(isnull(additional19,0)+isnull(additional20,0)+isnull(additional21,0)+isnull(additional22,0)+isnull(additional23,0)+isnull(additional24,0)+isnull(additional25,0)+isnull(additional26,0)+isnull(additional27,0)+isnull(additional28,0)+isnull(additional29,0)+isnull(additional30,0)) as  R12Additional      
			,stypid,psf19
			from #table where not amount19 is null and dateadd(month,-18,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-19,@StartDate) ,isnull(sqft20,0),isnull(asqft20,0),isnull(Amount20,0) as sales,isnull(baserent20,0),isnull(additional20,0),isComparable20,iif(RentEnd<dateadd(month,-19,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft20,0)=0,0,1)+iif(isnull(sqft21,0)=0,0,1)+iif(isnull(sqft22,0)=0,0,1)+iif(isnull(sqft23,0)=0,0,1)+iif(isnull(sqft24,0)=0,0,1)+iif(isnull(sqft25,0)=0,0,1)+iif(isnull(sqft26,0)=0,0,1)+iif(isnull(sqft27,0)=0,0,1)+iif(isnull(sqft28,0)=0,0,1)+iif(isnull(sqft29,0)=0,0,1)+iif(isnull(sqft30,0)=0,0,1)+iif(isnull(sqft31,0)=0,0,1))=0,0,
			iif(isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)=0,0,
			(isnull(sqft20,0)+isnull(sqft21,0)+isnull(sqft22,0)+isnull(sqft23,0)+isnull(sqft24,0)+isnull(sqft25,0)+isnull(sqft26,0)+isnull(sqft27,0)+isnull(sqft28,0)+isnull(sqft29,0)+isnull(sqft30,0)+isnull(sqft31,0))/
			(isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0))
			) as R12GLA,
			iif(isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)=0,0,
			(isnull(asqft20,0)+isnull(asqft21,0)+isnull(asqft22,0)+isnull(asqft23,0)+isnull(asqft24,0)+isnull(asqft25,0)+isnull(asqft26,0)+isnull(asqft27,0)+isnull(asqft28,0)+isnull(asqft29,0)+isnull(asqft30,0)+isnull(asqft31,0))/
			(isnull(psf20,0)+isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0))
			) as R12SalesGLA,
			(isnull(Amount20,0)+isnull(Amount21,0)+isnull(Amount22,0)+isnull(Amount23,0)+isnull(Amount24,0)+isnull(Amount25,0)+isnull(Amount26,0)+isnull(Amount27,0)+isnull(Amount28,0)+isnull(Amount29,0)+isnull(Amount30,0)+isnull(Amount31,0)) as R12Sales,
			(isnull(baserent20,0)+isnull(baserent21,0)+isnull(baserent22,0)+isnull(baserent23,0)+isnull(baserent24,0)+isnull(baserent25,0)+isnull(baserent26,0)+isnull(baserent27,0)+isnull(baserent28,0)+isnull(baserent29,0)+isnull(baserent30,0)+isnull(baserent31,0)) as R12BaseRent,
			(isnull(additional20,0)+isnull(additional21,0)+isnull(additional22,0)+isnull(additional23,0)+isnull(additional24,0)+isnull(additional25,0)+isnull(additional26,0)+isnull(additional27,0)+isnull(additional28,0)+isnull(additional29,0)+isnull(additional30,0)+isnull(additional31,0)) as  R12Additional       
			,stypid,psf20
			from #table where not amount20 is null and dateadd(month,-19,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-20,@StartDate) ,isnull(sqft21,0),isnull(asqft21,0),isnull(Amount21,0) as sales,isnull(baserent21,0),isnull(additional21,0),isComparable21,iif(RentEnd<dateadd(month,-20,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft21,0)=0,0,1)+iif(isnull(sqft22,0)=0,0,1)+iif(isnull(sqft23,0)=0,0,1)+iif(isnull(sqft24,0)=0,0,1)+iif(isnull(sqft25,0)=0,0,1)+iif(isnull(sqft26,0)=0,0,1)+iif(isnull(sqft27,0)=0,0,1)+iif(isnull(sqft28,0)=0,0,1)+iif(isnull(sqft29,0)=0,0,1)+iif(isnull(sqft30,0)=0,0,1)+iif(isnull(sqft31,0)=0,0,1)+iif(isnull(sqft32,0)=0,0,1))=0,0,
			iif(isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)=0,0,
			(isnull(sqft21,0)+isnull(sqft22,0)+isnull(sqft23,0)+isnull(sqft24,0)+isnull(sqft25,0)+isnull(sqft26,0)+isnull(sqft27,0)+isnull(sqft28,0)+isnull(sqft29,0)+isnull(sqft30,0)+isnull(sqft31,0)+isnull(sqft32,0))/
			(isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0))
			) as R12GLA,
			iif(isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)=0,0,
			(isnull(asqft21,0)+isnull(asqft22,0)+isnull(asqft23,0)+isnull(asqft24,0)+isnull(asqft25,0)+isnull(asqft26,0)+isnull(asqft27,0)+isnull(asqft28,0)+isnull(asqft29,0)+isnull(asqft30,0)+isnull(asqft31,0)+isnull(asqft32,0))/
			(isnull(psf21,0)+isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0))
			) as R12SalesGLA,
			(isnull(Amount21,0)+isnull(Amount22,0)+isnull(Amount23,0)+isnull(Amount24,0)+isnull(Amount25,0)+isnull(Amount26,0)+isnull(Amount27,0)+isnull(Amount28,0)+isnull(Amount29,0)+isnull(Amount30,0)+isnull(Amount31,0)+isnull(Amount32,0)) as R12Sales,
			(isnull(baserent21,0)+isnull(baserent22,0)+isnull(baserent23,0)+isnull(baserent24,0)+isnull(baserent25,0)+isnull(baserent26,0)+isnull(baserent27,0)+isnull(baserent28,0)+isnull(baserent29,0)+isnull(baserent30,0)+isnull(baserent31,0)+isnull(baserent32,0)) as R12BaseRent,
			(isnull(additional21,0)+isnull(additional22,0)+isnull(additional23,0)+isnull(additional24,0)+isnull(additional25,0)+isnull(additional26,0)+isnull(additional27,0)+isnull(additional28,0)+isnull(additional29,0)+isnull(additional30,0)+isnull(additional31,0)+isnull(additional32,0)) as  R12Additional        
			,stypid,psf21
			from #table where not amount21 is null and dateadd(month,-20,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-21,@StartDate) ,isnull(sqft22,0),isnull(asqft22,0),isnull(Amount22,0) as sales,isnull(baserent22,0),isnull(additional22,0),isComparable22,iif(RentEnd<dateadd(month,-21,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft22,0)=0,0,1)+iif(isnull(sqft23,0)=0,0,1)+iif(isnull(sqft24,0)=0,0,1)+iif(isnull(sqft25,0)=0,0,1)+iif(isnull(sqft26,0)=0,0,1)+iif(isnull(sqft27,0)=0,0,1)+iif(isnull(sqft28,0)=0,0,1)+iif(isnull(sqft29,0)=0,0,1)+iif(isnull(sqft30,0)=0,0,1)+iif(isnull(sqft31,0)=0,0,1)+iif(isnull(sqft32,0)=0,0,1)+iif(isnull(sqft33,0)=0,0,1))=0,0,
			iif(isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)=0,0,
			(isnull(sqft22,0)+isnull(sqft23,0)+isnull(sqft24,0)+isnull(sqft25,0)+isnull(sqft26,0)+isnull(sqft27,0)+isnull(sqft28,0)+isnull(sqft29,0)+isnull(sqft30,0)+isnull(sqft31,0)+isnull(sqft32,0)+isnull(sqft33,0))/
			(isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0))
			) as R12GLA,
			iif(isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)=0,0,
			(isnull(asqft22,0)+isnull(asqft23,0)+isnull(asqft24,0)+isnull(asqft25,0)+isnull(asqft26,0)+isnull(asqft27,0)+isnull(asqft28,0)+isnull(asqft29,0)+isnull(asqft30,0)+isnull(asqft31,0)+isnull(asqft32,0)+isnull(asqft33,0))/
			(isnull(psf22,0)+isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0))
			) as R12SalesGLA,
			(isnull(Amount22,0)+isnull(Amount23,0)+isnull(Amount24,0)+isnull(Amount25,0)+isnull(Amount26,0)+isnull(Amount27,0)+isnull(Amount28,0)+isnull(Amount29,0)+isnull(Amount30,0)+isnull(Amount31,0)+isnull(Amount32,0)+isnull(Amount33,0)) as R12Sales,
			(isnull(baserent22,0)+isnull(baserent23,0)+isnull(baserent24,0)+isnull(baserent25,0)+isnull(baserent26,0)+isnull(baserent27,0)+isnull(baserent28,0)+isnull(baserent29,0)+isnull(baserent30,0)+isnull(baserent31,0)+isnull(baserent32,0)+isnull(baserent33,0)) as R12BaseRent,
			(isnull(additional22,0)+isnull(additional23,0)+isnull(additional24,0)+isnull(additional25,0)+isnull(additional26,0)+isnull(additional27,0)+isnull(additional28,0)+isnull(additional29,0)+isnull(additional30,0)+isnull(additional31,0)+isnull(additional32,0)+isnull(additional33,0)) as  R12Additional         
			,stypid,psf22
			from #table where not amount22 is null and dateadd(month,-21,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-22,@StartDate) ,isnull(sqft23,0),isnull(asqft23,0),isnull(Amount23,0) as sales,isnull(baserent23,0),isnull(additional23,0),isComparable23,iif(RentEnd<dateadd(month,-22,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft23,0)=0,0,1)+iif(isnull(sqft24,0)=0,0,1)+iif(isnull(sqft25,0)=0,0,1)+iif(isnull(sqft26,0)=0,0,1)+iif(isnull(sqft27,0)=0,0,1)+iif(isnull(sqft28,0)=0,0,1)+iif(isnull(sqft29,0)=0,0,1)+iif(isnull(sqft30,0)=0,0,1)+iif(isnull(sqft31,0)=0,0,1)+iif(isnull(sqft32,0)=0,0,1)+iif(isnull(sqft33,0)=0,0,1)+iif(isnull(sqft34,0)=0,0,1))=0,0,
			iif(isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)=0,0,
			(isnull(sqft23,0)+isnull(sqft24,0)+isnull(sqft25,0)+isnull(sqft26,0)+isnull(sqft27,0)+isnull(sqft28,0)+isnull(sqft29,0)+isnull(sqft30,0)+isnull(sqft31,0)+isnull(sqft32,0)+isnull(sqft33,0)+isnull(sqft34,0))/
			(isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0))
			) as R12GLA,
			iif(isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)=0,0,
			(isnull(asqft23,0)+isnull(asqft24,0)+isnull(asqft25,0)+isnull(asqft26,0)+isnull(asqft27,0)+isnull(asqft28,0)+isnull(asqft29,0)+isnull(asqft30,0)+isnull(asqft31,0)+isnull(asqft32,0)+isnull(asqft33,0)+isnull(asqft34,0))/
			(isnull(psf23,0)+isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0))
			) as R12SalesGLA,
			(isnull(Amount23,0)+isnull(Amount24,0)+isnull(Amount25,0)+isnull(Amount26,0)+isnull(Amount27,0)+isnull(Amount28,0)+isnull(Amount29,0)+isnull(Amount30,0)+isnull(Amount31,0)+isnull(Amount32,0)+isnull(Amount33,0)+isnull(Amount34,0)) as R12Sales,
			(isnull(baserent23,0)+isnull(baserent24,0)+isnull(baserent25,0)+isnull(baserent26,0)+isnull(baserent27,0)+isnull(baserent28,0)+isnull(baserent29,0)+isnull(baserent30,0)+isnull(baserent31,0)+isnull(baserent32,0)+isnull(baserent33,0)+isnull(baserent34,0)) as R12BaseRent,
			(isnull(additional23,0)+isnull(additional24,0)+isnull(additional25,0)+isnull(additional26,0)+isnull(additional27,0)+isnull(additional28,0)+isnull(additional29,0)+isnull(additional30,0)+isnull(additional31,0)+isnull(additional32,0)+isnull(additional33,0)+isnull(additional34,0)) as  R12Additional          
			,stypid,psf23
			from #table where not amount23 is null and dateadd(month,-22,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-23,@StartDate) ,isnull(sqft24,0),isnull(asqft24,0),isnull(Amount24,0) as sales,isnull(baserent24,0),isnull(additional24,0),isComparable24,iif(RentEnd<dateadd(month,-23,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft24,0)=0,0,1)+iif(isnull(sqft25,0)=0,0,1)+iif(isnull(sqft26,0)=0,0,1)+iif(isnull(sqft27,0)=0,0,1)+iif(isnull(sqft28,0)=0,0,1)+iif(isnull(sqft29,0)=0,0,1)+iif(isnull(sqft30,0)=0,0,1)+iif(isnull(sqft31,0)=0,0,1)+iif(isnull(sqft32,0)=0,0,1)+iif(isnull(sqft33,0)=0,0,1)+iif(isnull(sqft34,0)=0,0,1)+iif(isnull(sqft35,0)=0,0,1))=0,0,
			iif(isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)=0,0,
			(isnull(sqft24,0)+isnull(sqft25,0)+isnull(sqft26,0)+isnull(sqft27,0)+isnull(sqft28,0)+isnull(sqft29,0)+isnull(sqft30,0)+isnull(sqft31,0)+isnull(sqft32,0)+isnull(sqft33,0)+isnull(sqft34,0)+isnull(sqft35,0))/
			(isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0))
			) as R12GLA,
			iif(isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)=0,0,
			(isnull(asqft24,0)+isnull(asqft25,0)+isnull(asqft26,0)+isnull(asqft27,0)+isnull(asqft28,0)+isnull(asqft29,0)+isnull(asqft30,0)+isnull(asqft31,0)+isnull(asqft32,0)+isnull(asqft33,0)+isnull(asqft34,0)+isnull(asqft35,0))/
			(isnull(psf24,0)+isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0))
			) as R12SalesGLA,
			(isnull(Amount24,0)+isnull(Amount25,0)+isnull(Amount26,0)+isnull(Amount27,0)+isnull(Amount28,0)+isnull(Amount29,0)+isnull(Amount30,0)+isnull(Amount31,0)+isnull(Amount32,0)+isnull(Amount33,0)+isnull(Amount34,0)+isnull(Amount35,0)) as R12Sales,
			(isnull(baserent24,0)+isnull(baserent25,0)+isnull(baserent26,0)+isnull(baserent27,0)+isnull(baserent28,0)+isnull(baserent29,0)+isnull(baserent30,0)+isnull(baserent31,0)+isnull(baserent32,0)+isnull(baserent33,0)+isnull(baserent34,0)+isnull(baserent35,0)) as R12BaseRent,
			(isnull(additional24,0)+isnull(additional25,0)+isnull(additional26,0)+isnull(additional27,0)+isnull(additional28,0)+isnull(additional29,0)+isnull(additional30,0)+isnull(additional31,0)+isnull(additional32,0)+isnull(additional33,0)+isnull(additional34,0)+isnull(additional35,0)) as  R12Additional           
			,stypid,psf24
			from #table where not amount24 is null and dateadd(month,-23,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-24,@StartDate) ,isnull(sqft25,0),isnull(asqft25,0),isnull(Amount25,0) as sales,isnull(baserent25,0),isnull(additional25,0),isComparable25,iif(RentEnd<dateadd(month,-24,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft25,0)=0,0,1)+iif(isnull(sqft26,0)=0,0,1)+iif(isnull(sqft27,0)=0,0,1)+iif(isnull(sqft28,0)=0,0,1)+iif(isnull(sqft29,0)=0,0,1)+iif(isnull(sqft30,0)=0,0,1)+iif(isnull(sqft31,0)=0,0,1)+iif(isnull(sqft32,0)=0,0,1)+iif(isnull(sqft33,0)=0,0,1)+iif(isnull(sqft34,0)=0,0,1)+iif(isnull(sqft35,0)=0,0,1)+iif(isnull(sqft36,0)=0,0,1))=0,0,
			iif(isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)=0,0,
			(isnull(sqft25,0)+isnull(sqft26,0)+isnull(sqft27,0)+isnull(sqft28,0)+isnull(sqft29,0)+isnull(sqft30,0)+isnull(sqft31,0)+isnull(sqft32,0)+isnull(sqft33,0)+isnull(sqft34,0)+isnull(sqft35,0)+isnull(sqft36,0))/
			(isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0))
			) as R12GLA,
			iif(isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)=0,0,
			(isnull(asqft25,0)+isnull(asqft26,0)+isnull(asqft27,0)+isnull(asqft28,0)+isnull(asqft29,0)+isnull(asqft30,0)+isnull(asqft31,0)+isnull(asqft32,0)+isnull(asqft33,0)+isnull(asqft34,0)+isnull(asqft35,0)+isnull(asqft36,0))/
			(isnull(psf25,0)+isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0))
			) as R12SalesGLA,
			(isnull(Amount25,0)+isnull(Amount26,0)+isnull(Amount27,0)+isnull(Amount28,0)+isnull(Amount29,0)+isnull(Amount30,0)+isnull(Amount31,0)+isnull(Amount32,0)+isnull(Amount33,0)+isnull(Amount34,0)+isnull(Amount35,0)+isnull(Amount36,0)) as R12Sales,
			(isnull(baserent25,0)+isnull(baserent26,0)+isnull(baserent27,0)+isnull(baserent28,0)+isnull(baserent29,0)+isnull(baserent30,0)+isnull(baserent31,0)+isnull(baserent32,0)+isnull(baserent33,0)+isnull(baserent34,0)+isnull(baserent35,0)+isnull(baserent36,0)) as R12BaseRent,
			(isnull(additional25,0)+isnull(additional26,0)+isnull(additional27,0)+isnull(additional28,0)+isnull(additional29,0)+isnull(additional30,0)+isnull(additional31,0)+isnull(additional32,0)+isnull(additional33,0)+isnull(additional34,0)+isnull(additional35,0)+isnull(additional36,0)) as  R12Additional            
			,stypid,psf25
			from #table where not amount25 is null and dateadd(month,-24,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-25,@StartDate) ,isnull(sqft26,0),isnull(asqft26,0),isnull(Amount26,0) as sales,isnull(baserent26,0),isnull(additional26,0),isComparable26,iif(RentEnd<dateadd(month,-25,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft26,0)=0,0,1)+iif(isnull(sqft27,0)=0,0,1)+iif(isnull(sqft28,0)=0,0,1)+iif(isnull(sqft29,0)=0,0,1)+iif(isnull(sqft30,0)=0,0,1)+iif(isnull(sqft31,0)=0,0,1)+iif(isnull(sqft32,0)=0,0,1)+iif(isnull(sqft33,0)=0,0,1)+iif(isnull(sqft34,0)=0,0,1)+iif(isnull(sqft35,0)=0,0,1)+iif(isnull(sqft36,0)=0,0,1)+iif(isnull(sqft37,0)=0,0,1))=0,0,
			iif(isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)=0,0,
			(isnull(sqft26,0)+isnull(sqft27,0)+isnull(sqft28,0)+isnull(sqft29,0)+isnull(sqft30,0)+isnull(sqft31,0)+isnull(sqft32,0)+isnull(sqft33,0)+isnull(sqft34,0)+isnull(sqft35,0)+isnull(sqft36,0)+isnull(sqft37,0))/
			(isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0))
			) as R12GLA,
			iif(isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)=0,0,
			(isnull(asqft26,0)+isnull(asqft27,0)+isnull(asqft28,0)+isnull(asqft29,0)+isnull(asqft30,0)+isnull(asqft31,0)+isnull(asqft32,0)+isnull(asqft33,0)+isnull(asqft34,0)+isnull(asqft35,0)+isnull(asqft36,0)+isnull(asqft37,0))/
			(isnull(psf26,0)+isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0))
			) as R12SalesGLA,
			(isnull(Amount26,0)+isnull(Amount27,0)+isnull(Amount28,0)+isnull(Amount29,0)+isnull(Amount30,0)+isnull(Amount31,0)+isnull(Amount32,0)+isnull(Amount33,0)+isnull(Amount34,0)+isnull(Amount35,0)+isnull(Amount36,0)+isnull(Amount37,0)) as R12Sales,
			(isnull(baserent26,0)+isnull(baserent27,0)+isnull(baserent28,0)+isnull(baserent29,0)+isnull(baserent30,0)+isnull(baserent31,0)+isnull(baserent32,0)+isnull(baserent33,0)+isnull(baserent34,0)+isnull(baserent35,0)+isnull(baserent36,0)+isnull(baserent37,0)) as R12BaseRent,
			(isnull(additional26,0)+isnull(additional27,0)+isnull(additional28,0)+isnull(additional29,0)+isnull(additional30,0)+isnull(additional31,0)+isnull(additional32,0)+isnull(additional33,0)+isnull(additional34,0)+isnull(additional35,0)+isnull(additional36,0)+isnull(additional37,0)) as  R12Additional             
			,stypid,psf26
			from #table where not amount26 is null and dateadd(month,-25,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-26,@StartDate) ,isnull(sqft27,0),isnull(asqft27,0),isnull(Amount27,0) as sales,isnull(baserent27,0),isnull(additional27,0),isComparable27,iif(RentEnd<dateadd(month,-26,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft27,0)=0,0,1)+iif(isnull(sqft28,0)=0,0,1)+iif(isnull(sqft29,0)=0,0,1)+iif(isnull(sqft30,0)=0,0,1)+iif(isnull(sqft31,0)=0,0,1)+iif(isnull(sqft32,0)=0,0,1)+iif(isnull(sqft33,0)=0,0,1)+iif(isnull(sqft34,0)=0,0,1)+iif(isnull(sqft35,0)=0,0,1)+iif(isnull(sqft36,0)=0,0,1)+iif(isnull(sqft37,0)=0,0,1)+iif(isnull(sqft38,0)=0,0,1))=0,0,
			iif(isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)=0,0,
			(isnull(sqft27,0)+isnull(sqft28,0)+isnull(sqft29,0)+isnull(sqft30,0)+isnull(sqft31,0)+isnull(sqft32,0)+isnull(sqft33,0)+isnull(sqft34,0)+isnull(sqft35,0)+isnull(sqft36,0)+isnull(sqft37,0)+isnull(sqft38,0))/
			(isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0))
			) as R12GLA,
			iif(isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)=0,0,
			(isnull(asqft27,0)+isnull(asqft28,0)+isnull(asqft29,0)+isnull(asqft30,0)+isnull(asqft31,0)+isnull(asqft32,0)+isnull(asqft33,0)+isnull(asqft34,0)+isnull(asqft35,0)+isnull(asqft36,0)+isnull(asqft37,0)+isnull(asqft38,0))/
			(isnull(psf27,0)+isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0))
			) as R12SalesGLA,
			(isnull(Amount27,0)+isnull(Amount28,0)+isnull(Amount29,0)+isnull(Amount30,0)+isnull(Amount31,0)+isnull(Amount32,0)+isnull(Amount33,0)+isnull(Amount34,0)+isnull(Amount35,0)+isnull(Amount36,0)+isnull(Amount37,0)+isnull(Amount38,0)) as R12Sales,
			(isnull(baserent27,0)+isnull(baserent28,0)+isnull(baserent29,0)+isnull(baserent30,0)+isnull(baserent31,0)+isnull(baserent32,0)+isnull(baserent33,0)+isnull(baserent34,0)+isnull(baserent35,0)+isnull(baserent36,0)+isnull(baserent37,0)+isnull(baserent38,0)) as R12BaseRent,
			(isnull(additional27,0)+isnull(additional28,0)+isnull(additional29,0)+isnull(additional30,0)+isnull(additional31,0)+isnull(additional32,0)+isnull(additional33,0)+isnull(additional34,0)+isnull(additional35,0)+isnull(additional36,0)+isnull(additional37,0)+isnull(additional38,0)) as  R12Additional              
			,stypid,psf27
			from #table where not amount27 is null and dateadd(month,-26,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-27,@StartDate) ,isnull(sqft28,0),isnull(asqft28,0),isnull(Amount28,0) as sales,isnull(baserent28,0),isnull(additional28,0),isComparable28,iif(RentEnd<dateadd(month,-27,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft28,0)=0,0,1)+iif(isnull(sqft29,0)=0,0,1)+iif(isnull(sqft30,0)=0,0,1)+iif(isnull(sqft31,0)=0,0,1)+iif(isnull(sqft32,0)=0,0,1)+iif(isnull(sqft33,0)=0,0,1)+iif(isnull(sqft34,0)=0,0,1)+iif(isnull(sqft35,0)=0,0,1)+iif(isnull(sqft36,0)=0,0,1)+iif(isnull(sqft37,0)=0,0,1)+iif(isnull(sqft38,0)=0,0,1)+iif(isnull(sqft39,0)=0,0,1))=0,0,
			iif(isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)=0,0,
			(isnull(sqft28,0)+isnull(sqft29,0)+isnull(sqft30,0)+isnull(sqft31,0)+isnull(sqft32,0)+isnull(sqft33,0)+isnull(sqft34,0)+isnull(sqft35,0)+isnull(sqft36,0)+isnull(sqft37,0)+isnull(sqft38,0)+isnull(sqft39,0))/
			(isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0))
			) as R12GLA,
			iif(isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)=0,0,
			(isnull(asqft28,0)+isnull(asqft29,0)+isnull(asqft30,0)+isnull(asqft31,0)+isnull(asqft32,0)+isnull(asqft33,0)+isnull(asqft34,0)+isnull(asqft35,0)+isnull(asqft36,0)+isnull(asqft37,0)+isnull(asqft38,0)+isnull(asqft39,0))/
			(isnull(psf28,0)+isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0))
			) as R12SalesGLA,
			(isnull(Amount28,0)+isnull(Amount29,0)+isnull(Amount30,0)+isnull(Amount31,0)+isnull(Amount32,0)+isnull(Amount33,0)+isnull(Amount34,0)+isnull(Amount35,0)+isnull(Amount36,0)+isnull(Amount37,0)+isnull(Amount38,0)+isnull(Amount39,0)) as R12Sales,
			(isnull(baserent28,0)+isnull(baserent29,0)+isnull(baserent30,0)+isnull(baserent31,0)+isnull(baserent32,0)+isnull(baserent33,0)+isnull(baserent34,0)+isnull(baserent35,0)+isnull(baserent36,0)+isnull(baserent37,0)+isnull(baserent38,0)+isnull(baserent39,0)) as R12BaseRent,
			(isnull(additional28,0)+isnull(additional29,0)+isnull(additional30,0)+isnull(additional31,0)+isnull(additional32,0)+isnull(additional33,0)+isnull(additional34,0)+isnull(additional35,0)+isnull(additional36,0)+isnull(additional37,0)+isnull(additional38,0)+isnull(additional39,0)) as  R12Additional               
			,stypid,psf28
			from #table where not amount28 is null and dateadd(month,-27,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-28,@StartDate) ,isnull(sqft29,0),isnull(asqft29,0),isnull(Amount29,0) as sales,isnull(baserent29,0),isnull(additional29,0),isComparable29 ,iif(RentEnd<dateadd(month,-28,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft29,0)=0,0,1)+iif(isnull(sqft30,0)=0,0,1)+iif(isnull(sqft31,0)=0,0,1)+iif(isnull(sqft32,0)=0,0,1)+iif(isnull(sqft33,0)=0,0,1)+iif(isnull(sqft34,0)=0,0,1)+iif(isnull(sqft35,0)=0,0,1)+iif(isnull(sqft36,0)=0,0,1)+iif(isnull(sqft37,0)=0,0,1)+iif(isnull(sqft38,0)=0,0,1)+iif(isnull(sqft39,0)=0,0,1)+iif(isnull(sqft40,0)=0,0,1))=0,0,
			iif(isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)=0,0,
			(isnull(sqft29,0)+isnull(sqft30,0)+isnull(sqft31,0)+isnull(sqft32,0)+isnull(sqft33,0)+isnull(sqft34,0)+isnull(sqft35,0)+isnull(sqft36,0)+isnull(sqft37,0)+isnull(sqft38,0)+isnull(sqft39,0)+isnull(sqft40,0))/
			(isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0))
			) as R12GLA,
			iif(isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)=0,0,
			(isnull(asqft29,0)+isnull(asqft30,0)+isnull(asqft31,0)+isnull(asqft32,0)+isnull(asqft33,0)+isnull(asqft34,0)+isnull(asqft35,0)+isnull(asqft36,0)+isnull(asqft37,0)+isnull(asqft38,0)+isnull(asqft39,0)+isnull(asqft40,0))/
			(isnull(psf29,0)+isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0))
			) as R12SalesGLA,
			(isnull(Amount29,0)+isnull(Amount30,0)+isnull(Amount31,0)+isnull(Amount32,0)+isnull(Amount33,0)+isnull(Amount34,0)+isnull(Amount35,0)+isnull(Amount36,0)+isnull(Amount37,0)+isnull(Amount38,0)+isnull(Amount39,0)+isnull(Amount40,0)) as R12Sales,
			(isnull(baserent29,0)+isnull(isnull(baserent30,0),0)+isnull(baserent31,0)+isnull(baserent32,0)+isnull(baserent33,0)+isnull(baserent34,0)+isnull(baserent35,0)+isnull(baserent36,0)+isnull(baserent37,0)+isnull(baserent38,0)+isnull(baserent39,0)+isnull(baserent40,0)) as R12BaseRent,
			(isnull(additional29,0)+isnull(additional30,0)+isnull(additional31,0)+isnull(additional32,0)+isnull(additional33,0)+isnull(additional34,0)+isnull(additional35,0)+isnull(additional36,0)+isnull(additional37,0)+isnull(additional38,0)+isnull(additional39,0)+isnull(additional40,0)) as  R12Additional               
			,stypid,psf29
			from #table where not amount29 is null and dateadd(month,-28,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-29,@StartDate) ,isnull(sqft30,0),isnull(asqft30,0),isnull(Amount30,0) as sales,isnull(baserent30,0),isnull(additional30,0),isComparable30,iif(RentEnd<dateadd(month,-29,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft30,0)=0,0,1)+iif(isnull(sqft31,0)=0,0,1)+iif(isnull(sqft32,0)=0,0,1)+iif(isnull(sqft33,0)=0,0,1)+iif(isnull(sqft34,0)=0,0,1)+iif(isnull(sqft35,0)=0,0,1)+iif(isnull(sqft36,0)=0,0,1)+iif(isnull(sqft37,0)=0,0,1)+iif(isnull(sqft38,0)=0,0,1)+iif(isnull(sqft39,0)=0,0,1)+iif(isnull(sqft40,0)=0,0,1)+iif(isnull(sqft41,0)=0,0,1))=0,0,
			iif(isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)=0,0,
			(isnull(sqft30,0)+isnull(sqft31,0)+isnull(sqft32,0)+isnull(sqft33,0)+isnull(sqft34,0)+isnull(sqft35,0)+isnull(sqft36,0)+isnull(sqft37,0)+isnull(sqft38,0)+isnull(sqft39,0)+isnull(sqft40,0)+isnull(sqft41,0))/
			(isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0))
			) as R12GLA,
			iif(isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)=0,0,
			(isnull(asqft30,0)+isnull(asqft31,0)+isnull(asqft32,0)+isnull(asqft33,0)+isnull(asqft34,0)+isnull(asqft35,0)+isnull(asqft36,0)+isnull(asqft37,0)+isnull(asqft38,0)+isnull(asqft39,0)+isnull(asqft40,0)+isnull(asqft41,0))/
			(isnull(psf30,0)+isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0))
			) as R12SalesGLA,
			(isnull(Amount30,0)+isnull(Amount31,0)+isnull(Amount32,0)+isnull(Amount33,0)+isnull(Amount34,0)+isnull(Amount35,0)+isnull(Amount36,0)+isnull(Amount37,0)+isnull(Amount38,0)+isnull(Amount39,0)+isnull(Amount40,0)+isnull(Amount41,0)) as R12Sales,
			(isnull(baserent30,0)+isnull(baserent31,0)+isnull(baserent32,0)+isnull(baserent33,0)+isnull(baserent34,0)+isnull(baserent35,0)+isnull(baserent36,0)+isnull(baserent37,0)+isnull(baserent38,0)+isnull(baserent39,0)+isnull(baserent40,0)+baserent41) as R12BaseRent,
			(isnull(additional30,0)+isnull(additional31,0)+isnull(additional32,0)+isnull(additional33,0)+isnull(additional34,0)+isnull(additional35,0)+isnull(additional36,0)+isnull(additional37,0)+isnull(additional38,0)+isnull(additional39,0)+isnull(additional40,0)+isnull(additional41,0)) as  R12Additional                
			,stypid,psf30
			from #table where not amount30 is null and dateadd(month,-29,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-30,@StartDate) ,isnull(sqft31,0),isnull(asqft31,0),isnull(Amount31,0) as sales,isnull(baserent31,0),isnull(additional31,0),isComparable30,iif(RentEnd<dateadd(month,-30,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft31,0)=0,0,1)+iif(isnull(sqft32,0)=0,0,1)+iif(isnull(sqft33,0)=0,0,1)+iif(isnull(sqft34,0)=0,0,1)+iif(isnull(sqft35,0)=0,0,1)+iif(isnull(sqft36,0)=0,0,1)+iif(isnull(sqft37,0)=0,0,1)+iif(isnull(sqft38,0)=0,0,1)+iif(isnull(sqft39,0)=0,0,1)+iif(isnull(sqft40,0)=0,0,1)+iif(isnull(sqft41,0)=0,0,1)+iif(isnull(sqft42,0)=0,0,1))=0,0,
			iif(isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)=0,0,
			(isnull(sqft31,0)+isnull(sqft32,0)+isnull(sqft33,0)+isnull(sqft34,0)+isnull(sqft35,0)+isnull(sqft36,0)+isnull(sqft37,0)+isnull(sqft38,0)+isnull(sqft39,0)+isnull(sqft40,0)+isnull(sqft41,0)+isnull(sqft42,0))/
			(isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0))
			) as R12GLA,
			iif(isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)=0,0,
			(isnull(asqft31,0)+isnull(asqft32,0)+isnull(asqft33,0)+isnull(asqft34,0)+isnull(asqft35,0)+isnull(asqft36,0)+isnull(asqft37,0)+isnull(asqft38,0)+isnull(asqft39,0)+isnull(asqft40,0)+isnull(asqft41,0)+isnull(asqft42,0))/
			(isnull(psf31,0)+isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0))
			) as R12SalesGLA,
			(isnull(Amount31,0)+isnull(Amount32,0)+isnull(Amount33,0)+isnull(Amount34,0)+isnull(Amount35,0)+isnull(Amount36,0)+isnull(Amount37,0)+isnull(Amount38,0)+isnull(Amount39,0)+isnull(Amount40,0)+isnull(Amount41,0)+isnull(Amount42,0)) as R12Sales,
			(isnull(baserent31,0)+isnull(baserent32,0)+isnull(baserent33,0)+isnull(baserent34,0)+isnull(baserent35,0)+isnull(baserent36,0)+isnull(baserent37,0)+isnull(baserent38,0)+isnull(baserent39,0)+isnull(baserent40,0)+isnull(baserent41,0)+isnull(baserent42,0)) as R12BaseRent,
			(isnull(additional31,0)+isnull(additional32,0)+isnull(additional33,0)+isnull(additional34,0)+isnull(additional35,0)+isnull(additional36,0)+isnull(additional37,0)+isnull(additional38,0)+isnull(additional39,0)+isnull(additional40,0)+isnull(additional41,0)+isnull(additional42,0)) as  R12Additional                 
			,stypid,psf31
			from #table where not amount31 is null and dateadd(month,-30,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-31,@StartDate) ,isnull(sqft32,0),isnull(asqft32,0),isnull(Amount32,0) as sales,isnull(baserent32,0),isnull(additional32,0),isComparable30,iif(RentEnd<dateadd(month,-31,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft32,0)=0,0,1)+iif(isnull(sqft33,0)=0,0,1)+iif(isnull(sqft34,0)=0,0,1)+iif(isnull(sqft35,0)=0,0,1)+iif(isnull(sqft36,0)=0,0,1)+iif(isnull(sqft37,0)=0,0,1)+iif(isnull(sqft38,0)=0,0,1)+iif(isnull(sqft39,0)=0,0,1)+iif(isnull(sqft40,0)=0,0,1)+iif(isnull(sqft41,0)=0,0,1)+iif(isnull(sqft42,0)=0,0,1)+iif(isnull(sqft43,0)=0,0,1))=0,0,
			iif(isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)=0,0,
			(isnull(sqft32,0)+isnull(sqft33,0)+isnull(sqft34,0)+isnull(sqft35,0)+isnull(sqft36,0)+isnull(sqft37,0)+isnull(sqft38,0)+isnull(sqft39,0)+isnull(sqft40,0)+isnull(sqft41,0)+isnull(sqft42,0)+isnull(sqft43,0))/
			(isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0))
			) as R12GLA,
			iif(isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)=0,0,
			(isnull(asqft32,0)+isnull(asqft33,0)+isnull(asqft34,0)+isnull(asqft35,0)+isnull(asqft36,0)+isnull(asqft37,0)+isnull(asqft38,0)+isnull(asqft39,0)+isnull(asqft40,0)+isnull(asqft41,0)+isnull(asqft42,0)+isnull(asqft43,0))/
			(isnull(psf32,0)+isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0))
			) as R12SalesGLA,
			(isnull(Amount32,0)+isnull(Amount33,0)+isnull(Amount34,0)+isnull(Amount35,0)+isnull(Amount36,0)+isnull(Amount37,0)+isnull(Amount38,0)+isnull(Amount39,0)+isnull(Amount40,0)+isnull(Amount41,0)+isnull(Amount42,0)+isnull(Amount43,0)) as R12Sales,
			(isnull(baserent32,0)+isnull(baserent33,0)+isnull(baserent34,0)+isnull(baserent35,0)+isnull(baserent36,0)+isnull(baserent37,0)+isnull(baserent38,0)+isnull(baserent39,0)+isnull(baserent40,0)+isnull(baserent41,0)+isnull(baserent42,0)+isnull(baserent43,0)) as R12BaseRent,
			(isnull(additional32,0)+isnull(additional33,0)+isnull(additional34,0)+isnull(additional35,0)+isnull(additional36,0)+isnull(additional37,0)+isnull(additional38,0)+isnull(additional39,0)+isnull(additional40,0)+isnull(additional41,0)+isnull(additional42,0)+isnull(additional43,0)) as  R12Additional                  
			,stypid,psf32
			from #table where not amount32 is null and dateadd(month,-31,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-32,@StartDate) ,isnull(sqft33,0),isnull(asqft33,0),isnull(Amount33,0) as sales,isnull(baserent33,0),isnull(additional33,0),isComparable30,iif(RentEnd<dateadd(month,-32,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft33,0)=0,0,1)+iif(isnull(sqft34,0)=0,0,1)+iif(isnull(sqft35,0)=0,0,1)+iif(isnull(sqft36,0)=0,0,1)+iif(isnull(sqft37,0)=0,0,1)+iif(isnull(sqft38,0)=0,0,1)+iif(isnull(sqft39,0)=0,0,1)+iif(isnull(sqft40,0)=0,0,1)+iif(isnull(sqft41,0)=0,0,1)+iif(isnull(sqft42,0)=0,0,1)+iif(isnull(sqft43,0)=0,0,1)+iif(isnull(sqft44,0)=0,0,1))=0,0,
			iif(isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)=0,0,
			(isnull(sqft33,0)+isnull(sqft34,0)+isnull(sqft35,0)+isnull(sqft36,0)+isnull(sqft37,0)+isnull(sqft38,0)+isnull(sqft39,0)+isnull(sqft40,0)+isnull(sqft41,0)+isnull(sqft42,0)+isnull(sqft43,0)+isnull(sqft44,0))/
			(isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0))
			) as R12GLA,
			iif(isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)=0,0,
			(isnull(asqft33,0)+isnull(asqft34,0)+isnull(asqft35,0)+isnull(asqft36,0)+isnull(asqft37,0)+isnull(asqft38,0)+isnull(asqft39,0)+isnull(asqft40,0)+isnull(asqft41,0)+isnull(asqft42,0)+isnull(asqft43,0)+isnull(asqft44,0))/
			(isnull(psf33,0)+isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0))
			) as R12SalesGLA,
			(isnull(Amount33,0)+isnull(Amount34,0)+isnull(Amount35,0)+isnull(Amount36,0)+isnull(Amount37,0)+isnull(Amount38,0)+isnull(Amount39,0)+isnull(Amount40,0)+isnull(Amount41,0)+isnull(Amount42,0)+isnull(Amount43,0)+isnull(Amount44,0)) as R12Sales,
			(isnull(baserent33,0)+isnull(baserent34,0)+isnull(baserent35,0)+isnull(baserent36,0)+isnull(baserent37,0)+isnull(baserent38,0)+isnull(baserent39,0)+isnull(baserent40,0)+isnull(baserent41,0)+isnull(baserent42,0)+isnull(baserent43,0)+isnull(baserent44,0)) as R12BaseRent,
			(isnull(additional33,0)+isnull(additional34,0)+isnull(additional35,0)+isnull(additional36,0)+isnull(additional37,0)+isnull(additional38,0)+isnull(additional39,0)+isnull(additional40,0)+isnull(additional41,0)+isnull(additional42,0)+isnull(additional43,0)+isnull(additional44,0)) as  R12Additional                   
			,stypid,psf33
			from #table where not amount33 is null and dateadd(month,-32,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-33,@StartDate) ,isnull(sqft34,0),isnull(asqft34,0),isnull(Amount34,0) as sales,isnull(baserent34,0),isnull(additional34,0),isComparable30 ,iif(RentEnd<dateadd(month,-33,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft34,0)=0,0,1)+iif(isnull(sqft35,0)=0,0,1)+iif(isnull(sqft36,0)=0,0,1)+iif(isnull(sqft37,0)=0,0,1)+iif(isnull(sqft38,0)=0,0,1)+iif(isnull(sqft39,0)=0,0,1)+iif(isnull(sqft40,0)=0,0,1)+iif(isnull(sqft41,0)=0,0,1)+iif(isnull(sqft42,0)=0,0,1)+iif(isnull(sqft43,0)=0,0,1)+iif(isnull(sqft44,0)=0,0,1)+iif(isnull(sqft45,0)=0,0,1))=0,0,
			iif(isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)+isnull(psf45,0)=0,0,
			(isnull(sqft34,0)+isnull(sqft35,0)+isnull(sqft36,0)+isnull(sqft37,0)+isnull(sqft38,0)+isnull(sqft39,0)+isnull(sqft40,0)+isnull(sqft41,0)+isnull(sqft42,0)+isnull(sqft43,0)+isnull(sqft44,0)+isnull(sqft45,0))/
			(isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)+isnull(psf45,0))
			) as R12GLA,
			iif(isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)+isnull(psf45,0)=0,0,
			(isnull(asqft34,0)+isnull(asqft35,0)+isnull(asqft36,0)+isnull(asqft37,0)+isnull(asqft38,0)+isnull(asqft39,0)+isnull(asqft40,0)+isnull(asqft41,0)+isnull(asqft42,0)+isnull(asqft43,0)+isnull(asqft44,0)+isnull(asqft45,0))/
			(isnull(psf34,0)+isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)+isnull(psf45,0))
			) as R12SalesGLA,
			(isnull(Amount34,0)+isnull(Amount35,0)+isnull(Amount36,0)+isnull(Amount37,0)+isnull(Amount38,0)+isnull(Amount39,0)+isnull(Amount40,0)+isnull(Amount41,0)+isnull(Amount42,0)+isnull(Amount43,0)+isnull(Amount44,0)+isnull(Amount45,0)) as R12Sales,
			(isnull(baserent34,0)+isnull(baserent35,0)+isnull(baserent36,0)+isnull(baserent37,0)+isnull(baserent38,0)+isnull(baserent39,0)+isnull(baserent40,0)+isnull(baserent41,0)+isnull(baserent42,0)+isnull(baserent43,0)+isnull(baserent44,0)+isnull(baserent45,0)) as R12BaseRent,
			(isnull(additional34,0)+isnull(additional35,0)+isnull(additional36,0)+isnull(additional37,0)+isnull(additional38,0)+isnull(additional39,0)+isnull(additional40,0)+isnull(additional41,0)+isnull(additional42,0)+isnull(additional43,0)+isnull(additional44,0)+isnull(additional45,0)) as  R12Additional                   
			,stypid,psf34
			from #table where not amount34 is null and dateadd(month,-33,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-34,@StartDate) ,isnull(sqft35,0),isnull(asqft35,0),isnull(Amount35,0) as sales,isnull(baserent35,0),isnull(additional35,0),isComparable30,iif(RentEnd<dateadd(month,-34,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft35,0)=0,0,1)+iif(isnull(sqft36,0)=0,0,1)+iif(isnull(sqft37,0)=0,0,1)+iif(isnull(sqft38,0)=0,0,1)+iif(isnull(sqft39,0)=0,0,1)+iif(isnull(sqft40,0)=0,0,1)+iif(isnull(sqft41,0)=0,0,1)+iif(isnull(sqft42,0)=0,0,1)+iif(isnull(sqft43,0)=0,0,1)+iif(isnull(sqft44,0)=0,0,1)+iif(isnull(sqft45,0)=0,0,1)+iif(isnull(sqft46,0)=0,0,1))=0,0,
			iif(isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)+isnull(psf45,0)+isnull(psf46,0)=0,0,
			(isnull(sqft35,0)+isnull(sqft36,0)+isnull(sqft37,0)+isnull(sqft38,0)+isnull(sqft39,0)+isnull(sqft40,0)+isnull(sqft41,0)+isnull(sqft42,0)+isnull(sqft43,0)+isnull(sqft44,0)+isnull(sqft45,0)+isnull(sqft46,0))/
			(isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)+isnull(psf45,0)+isnull(psf46,0))
			) as R12GLA,
			iif(isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)+isnull(psf45,0)+isnull(psf46,0)=0,0,
			(isnull(asqft35,0)+isnull(asqft36,0)+isnull(asqft37,0)+isnull(asqft38,0)+isnull(asqft39,0)+isnull(asqft40,0)+isnull(asqft41,0)+isnull(asqft42,0)+isnull(asqft43,0)+isnull(asqft44,0)+isnull(asqft45,0)+isnull(asqft46,0))/
			(isnull(psf35,0)+isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)+isnull(psf45,0)+isnull(psf46,0))
			) as R12SalesGLA,
			(isnull(Amount35,0)+isnull(Amount36,0)+isnull(Amount37,0)+isnull(Amount38,0)+isnull(Amount39,0)+isnull(Amount40,0)+isnull(Amount41,0)+isnull(Amount42,0)+isnull(Amount43,0)+isnull(Amount44,0)+isnull(Amount45,0)+isnull(Amount46,0)) as R12Sales,
			(isnull(baserent35,0)+isnull(baserent36,0)+isnull(baserent37,0)+isnull(baserent38,0)+isnull(baserent39,0)+isnull(baserent40,0)+isnull(baserent41,0)+isnull(baserent42,0)+isnull(baserent43,0)+isnull(baserent44,0)+isnull(baserent45,0)+isnull(baserent46,0)) as R12BaseRent,
			(isnull(additional35,0)+isnull(additional36,0)+isnull(additional37,0)+isnull(additional38,0)+isnull(additional39,0)+isnull(additional40,0)+isnull(additional41,0)+isnull(additional42,0)+isnull(additional43,0)+isnull(additional44,0)+isnull(additional45,0)+isnull(additional46,0)) as  R12Additional                    
			,stypid,psf35
			from #table where not amount35 is null and dateadd(month,-34,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())

			insert into #table1 (bldgid,tenantid,unitid,tdate,sqft,SalesGLA , sales,baserent,additional,IsComparable,MtM, R12GLA ,R12SalesGLA , R12Sales,R12BaseRent , R12Additional,stypid,psf)
			select bldgid,tenantid,unitid, dateadd(month,-35,@StartDate) ,isnull(sqft36,0),isnull(asqft36,0),isnull(Amount36,0) as sales,isnull(baserent36,0),isnull(additional36,0),isComparable30,iif(RentEnd<dateadd(month,-35,@StartDate),1,0) as MtM,
			--iif((iif(isnull(sqft36,0)=0,0,1)+iif(isnull(sqft37,0)=0,0,1)+iif(isnull(sqft38,0)=0,0,1)+iif(isnull(sqft39,0)=0,0,1)+iif(isnull(sqft40,0)=0,0,1)+iif(isnull(sqft41,0)=0,0,1)+iif(isnull(sqft42,0)=0,0,1)+iif(isnull(sqft43,0)=0,0,1)+iif(isnull(sqft44,0)=0,0,1)+iif(isnull(sqft45,0)=0,0,1)+iif(isnull(sqft46,0)=0,0,1)+iif(isnull(sqft47,0)=0,0,1))=0,0,
			iif(isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)+isnull(psf45,0)+isnull(psf46,0)+isnull(psf47,0)=0,0,
			(isnull(sqft36,0)+isnull(sqft37,0)+isnull(sqft38,0)+isnull(sqft39,0)+isnull(sqft40,0)+isnull(sqft41,0)+isnull(sqft42,0)+isnull(sqft43,0)+isnull(sqft44,0)+isnull(sqft45,0)+isnull(sqft46,0)+isnull(sqft47,0))/
			(isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)+isnull(psf45,0)+isnull(psf46,0)+isnull(psf47,0))
			) as R12GLA,
			iif(isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)+isnull(psf45,0)+isnull(psf46,0)+isnull(psf47,0)=0,0,
			(isnull(asqft36,0)+isnull(asqft37,0)+isnull(asqft38,0)+isnull(asqft39,0)+isnull(asqft40,0)+isnull(asqft41,0)+isnull(asqft42,0)+isnull(asqft43,0)+isnull(asqft44,0)+isnull(asqft45,0)+isnull(asqft46,0)+isnull(asqft47,0))/
			(isnull(psf36,0)+isnull(psf37,0)+isnull(psf38,0)+isnull(psf39,0)+isnull(psf40,0)+isnull(psf41,0)+isnull(psf42,0)+isnull(psf43,0)+isnull(psf44,0)+isnull(psf45,0)+isnull(psf46,0)+isnull(psf47,0))
			) as R12SalesGLA,
			(isnull(Amount36,0)+isnull(Amount37,0)+isnull(Amount38,0)+isnull(Amount39,0)+isnull(Amount40,0)+isnull(Amount41,0)+isnull(Amount42,0)+isnull(Amount43,0)+isnull(Amount44,0)+isnull(Amount45,0)+isnull(Amount46,0)+isnull(Amount47,0)) as R12Sales,
			(isnull(baserent36,0)+isnull(baserent37,0)+isnull(baserent38,0)+isnull(baserent39,0)+isnull(baserent40,0)+isnull(baserent41,0)+isnull(baserent42,0)+isnull(baserent43,0)+isnull(baserent44,0)+isnull(baserent45,0)+isnull(baserent46,0)+isnull(baserent47,0)) as R12BaseRent,
			(isnull(additional36,0)+isnull(additional37,0)+isnull(additional38,0)+isnull(additional39,0)+isnull(additional40,0)+isnull(additional41,0)+isnull(additional42,0)+isnull(additional43,0)+isnull(additional44,0)+isnull(additional45,0)+isnull(additional46,0)+isnull(additional47,0)) as  R12Additional                     
			,stypid,psf36
			from #table where not amount36 is null and dateadd(month,-35,@StartDate) between iif(day(rentstrt)=1,rentstrt,dateadd(day,(day(rentstrt)-1)*-1,rentstrt)) and isnull(rentend,getdate())


			insert into #table2 
			select distinct bldgid, tenantid ,unitid ,tenant ,descrptn ,stypid ,NTenant ,ChainName , isLargeSL, RentStrt ,RentEnd ,unittype 
			from #table 


			delete from #yardi_TenantDimension_test
			insert into #yardi_TenantDimension_test (tenantid,propertyid,nationaltenant,tenant,category,categoryGrouped,unittype,leasetype,LeaseStartDate,LeaseEndDate,stypid)
			--select t.tenantid,t.bldgid,t.NTenant ,t.ChainName ,t.descrptn,iif(isnull(g.groupname,'')='','An@Yardi_TenantTransactional_testchor Stores',g.groupname),iif(t.islargesl=1,'Large Format','CRU'),iif(t.stypid='spl','special',t.unittype),RentStrt ,rentend,stypid   
			select t.tenantid,t.bldgid,t.NTenant ,t.ChainName ,t.descrptn,iif(isnull(g.groupname,'')='','NA',g.groupname),iif(t.islargesl=1,'Large Format','CRU'),iif(t.stypid='spl','special',t.unittype),RentStrt ,rentend,stypid   
			from #table2 t
			left join yardi_categorygroup g on g.id=t.stypid 


			delete from #Yardi_TenantTransactional_test
			insert into #Yardi_TenantTransactional_test (tenantid,propertyid,date,samestore,monthtomonth,weightedgla,SalesGLA ,sales,baserent,additionalrent,r12weightedgla,R12SalesGLA ,r12sales,r12baserent,r12additionalrent,stypid,psf)
			select t1.tenantid,t1.bldgid,t1.tdate,t1.IsComparable,0 as mtm,t1.sqft,t1.SalesGLA ,t1.sales,t1.baserent,t1.additional ,t1.R12GLA ,t1.R12SalesGLA ,t1.R12Sales ,t1.R12BaseRent ,t1.R12Additional,t1.stypid,t1.psf   
			from #table1 t1
			--where sales<>0 or sqft<>0 or r12sales<>0

			update #Yardi_TenantTransactional_test set monthtomonth=1
			from
			(select d.propertyid as pid,d.tenantid as tid,t.date as tdate,t.stypid as sid 
			from #yardi_TenantDimension_test d
			inner join #Yardi_TenantTransactional_test t on t.tenantid=d.tenantid and t.stypid=d.stypid
			where t.date >isnull(d.leaseenddate,getdate()) ) l where l.pid=propertyid and l.tid=tenantid and l.tdate=date and stypid=l.sid 

			update #Yardi_TenantTransactional_test set monthtomonth=1 from
			(select t.tenantid as tid,t.propertyid as pid, t.date as tdate
			from #Yardi_TenantTransactional_test t
			inner join Yardi_CommAmendments a on a.hTenant =t.tenantid and itype=5 
			where (t.date between dtMovein and isnull(dtmoveout,dtend)) or (t.date>=dtMovein and isnull(dtmoveout,dtend) is null) ) l where l.tid=tenantid and l.pid=propertyid and l.tdate=date

			delete from #Yardi_TenantTransactional_test from
			(SELECT propertyid as pid,tenantid as did, date as tdate, count(*) as icount
			FROM #Yardi_TenantTransactional_test
			GROUP BY propertyid,tenantid, date
			HAVING COUNT(*)>1 ) l where l.pid=propertyid and l.did=tenantid and l.tdate =date and sales=0 and weightedgla=0 /*added to prevent delete R12 Sales 2021-08-06*/ and R12Sales =0 and SalesGLA =0

			update #yardi_TenantDimension_test set leasestartdate=iif(k.maxdate<>leaseenddate,leasestartdate,k.mindate),leaseenddate=k.maxdate from
			(select t.tenantid as tid,t.propertyid as pid,t.stypid as sid, 
			(select min(date) from #Yardi_TenantTransactional_test tt where tt.tenantid=t.tenantid and tt.propertyid=t.propertyid and tt.stypid=t.stypid) as mindate ,
			(select max(date) from #Yardi_TenantTransactional_test tt where tt.tenantid=t.tenantid and tt.propertyid=t.propertyid and tt.stypid=t.stypid) as maxdate 
			from #yardi_TenantDimension_test t 
			inner join (SELECT propertyid as pid,tenantid as tid,leasestartdate, count(*) as lcount
			FROM #yardi_TenantDimension_test
			GROUP BY propertyid,tenantid,leasestartdate
			HAVING COUNT(*)>1 ) l on l.tid=t.tenantid and l.pid=t.propertyid) k where k.tid=tenantid and k.pid=propertyid and k.sid=stypid

			update #yardi_TenantDimension_test set leasestartdate=k.mindate from
			(
			select top 1 t.tenantid as tid,t.propertyid as pid,t.stypid as sid, t.leasestartdate,t.leaseenddate,
			(select min(date) from #Yardi_TenantTransactional_test tt where tt.tenantid=t.tenantid and tt.propertyid=t.propertyid and tt.stypid=t.stypid) as mindate ,
			(select max(date) from #Yardi_TenantTransactional_test tt where tt.tenantid=t.tenantid and tt.propertyid=t.propertyid and tt.stypid=t.stypid) as maxdate 
			from #yardi_TenantDimension_test t 
			inner join (SELECT propertyid as pid,tenantid as tid,leasestartdate, count(*) as lcount
			FROM #yardi_TenantDimension_test
			GROUP BY propertyid,tenantid,leasestartdate
			HAVING COUNT(*)>1 ) l on l.tid=t.tenantid and l.pid=t.propertyid order by LeaseEndDate desc) k where k.tid=tenantid and k.pid=propertyid and k.sid=stypid


 
 /***********************************************************************************/
		drop table if exists  tmp.Yardi_TenantTransactional
        Select * into tmp.Yardi_TenantTransactional from Yardi_TenantTransactional 
/***********************************************************************************/
		Truncate table Yardi_TenantTransactional 
/***********************************************************************************/
 		insert into Yardi_TenantTransactional(tenantid,propertyid,date,samestore,monthtomonth,weightedGLA,SalesGLA ,Sales,BaseRent,additionalrent,r12weightedgla,R12SalesGLA , r12sales,r12baserent,r12additionalrent,psf)
		select tenantid,propertyid,date,samestore,monthtomonth,weightedGLA,SalesGLA , Sales,BaseRent,additionalrent,r12weightedgla,R12SalesGLA ,r12sales,r12baserent,r12additionalrent,psf 
		from #Yardi_TenantTransactional_test
/***********************************************************************************/
		if @@Error >0
			Begin
			Set IDENTITY_INSERT Yardi_TenantTransactional  ON
			Truncate table Yardi_TenantTransactional 
			Insert into Yardi_TenantTransactional (
					TenantId, 
					PropertyId, 
					[Date], 
					SameStore, 
					MonthToMonth, 
					WeightedGLA, 
					Sales, 
					BaseRent, 
					AdditionalRent, 
					R12WeightedGLA, 
					R12Sales, 
					R12BaseRent, 
					R12AdditionalRent, 
					SalesGLA, 
					R12SalesGLA, 
					psf, 
					LastLease, 
					Yardi_TenantTransactional_Id
					)
			SELECT 
					TenantId, 
					PropertyId, 
					[Date], 
					SameStore, 
					MonthToMonth, 
					WeightedGLA, 
					Sales, 
					BaseRent, 
					AdditionalRent, 
					R12WeightedGLA, 
					R12Sales, 
					R12BaseRent, 
					R12AdditionalRent, 
					SalesGLA, 
					R12SalesGLA, 
					psf, 
					LastLease, 
					Yardi_TenantTransactional_Id
			FROM   tmp.Yardi_TenantTransactional
			Set IDENTITY_INSERT Yardi_TenantTransactional  OFF
			End

		Else
		Begin
		update Yardi_TenantTransactional set SalesGLA=0 where Sales=0
		update Yardi_TenantTransactional set SalesGLA=0, R12SalesGLA=0 where TenantId in
		(select d.TenantId 
		from Yardi_TenantTransactional t
		inner join Yardi_TenantDimension d on d.TenantId =t.TenantId 
		group by d.TenantId ,d.Category ,d.CategoryGrouped ,d.LeaseStartDate ,d.LeaseEndDate ,d.LeaseType ,d.NationalTenant 
		having sum(sales)=0)

		drop table if exists  tmp.Yardi_TenantTransactional
		End
/***********************************************************************************/


 	end try
	begin catch
			select  
				ERROR_NUMBER() AS [error_number],  
				ERROR_SEVERITY() AS [error_severity],  
				ERROR_STATE() AS [error_state],  
				ERROR_PROCEDURE() AS [error_procedure],  
				ERROR_LINE() AS [error_line],  
				ERROR_MESSAGE() AS [error_message];  
	end catch

end

GO



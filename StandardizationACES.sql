USE [ACES]
GO
/****** Object:  StoredProcedure [dbo].[usp_ACESNotesMaster]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[usp_ACESNotesMaster]

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

Declare @i bigint,
@x bigint,
@y bigint

set @y = 100000
Set @i = (Select count(*) from  PAStkFeedProduction with (nolock))
set @x =1
while @x < @i
Begin

	exec [dbo].[usp_insertACESNotes] @x, @y
		
	set @x = @x + 100000
	set @y = @y + 100000
End  
END
GO
/****** Object:  StoredProcedure [dbo].[usp_duplicatefitment]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		GM
-- Create date: 8/30/2018
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[usp_duplicatefitment]
@lc char(2)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;


INSERT INTO [dbo].[duplicateexceptions]
           ([sfran]
           ,[Basevehicle]
           ,[SubModel]
           ,[PartTerminologyID]
           ,[MfrBodyCode]
           ,[PositionID]
           ,[EngineBase]
           ,[EngineDesignation]
           ,[DriveType]
           ,[FuelType]
           ,[FuelDeliveryType]
           ,[CylinderHeadType]
           ,[BodyType]
           ,[Aspiration]
           ,[TransmissionControlType]
           ,[BedType]
           ,[BodyNumDoors]
           ,[BrakeABS]
           ,[BrakeSystem]
           ,[FrontBrakeType]
           ,[FrontSpringType]
           ,[IgnitionSystemType]
           ,[FuelDeliverySubType]
           ,[FuelSystemControlType]
           ,[FuelSystemDesign]
           ,[RearBrakeType]
           ,[RearSpringType]
           ,[SteeringSystem]
           ,[SteeringType]
           ,[TransmissionMfr]
           ,[TransmissionNumSpeeds]
           ,[TransmissionType]
           ,[ValvesPerEngine]
           ,[WheelBase]
           ,[EngineMfr]
           ,[EngineVin]
           ,[EngineVersion]
		   ,[Note1]
           ,[Note2]
           ,[Note3]
           ,[Note4]
           ,[Note5]
           ,[Note6]
           ,[Note7]
           ,[Note8]
           ,[Note9]
           ,[Note10]
           ,[Note11]
           ,[Note12]
           ,[Note13]
           ,[Note14]
           ,[Note15]
           ,[Note16]
		   ,mfrlabel
           ,[spart_count]
           ,[sku])
	SELECT  [sfran]
,[Basevehicle]
,isnull([SubModel], '') SubModel
,[PartTerminologyID]
,isnull([MfrBodyCode], '') MfrBodyCode
,isnull([PositionID], '') PositionID
,isnull([EngineBase], '') EngineBase
,isnull([EngineDesignation], '') EngineDesignation
,isnull([DriveType], '') DriveType
,isnull([FuelType], '') FuelType
,isnull([FuelDeliveryType], '') FuelDeliveryType
,isnull([CylinderHeadType], '') CylinderHeadType
,isnull(BodyType, '') BodyType
,isnull(Aspiration, '') Aspiration
,isnull(TransmissionControlType,'') TransmissionControlType 
,isnull(BedType, '') BedType, isnull(BodyNumDoors, '') BodyNumDoors, 
isnull(BrakeABS, '') BrakeABS, isnull(BrakeSystem, '') BrakeSystem, 
isnull(FrontBrakeType, '') FrontBrakeType, isnull(FrontSpringType, '') FrontSpringType, isnull(IgnitionSystemType, '') IgnitionSystemType,
isnull(FuelDeliverySubType, '') FuelDeliverySubType, isnull(FuelSystemControlType, '') FuelSystemControlType, 
isnull(FuelSystemDesign, '') FuelSystemDesign, isnull(RearBrakeType, '') RearBrakeType, 
isnull(RearSpringType, '') RearSpringType,  isnull(SteeringSystem, '') SteeringSystem, isnull(SteeringType, '') SteeringType, 
isnull(TransmissionMfr, '') TransmissionMfr, isnull(TransmissionNumSpeeds, '') TransmissionNumSpeeds,
isnull(TransmissionType, '') TransmissionType, isnull(ValvesPerEngine, '') ValvesPerEngine, 
isnull(WheelBase, '') WheelBase, isnull(EngineMfr, '') EngineMfr, isnull(EngineVin, '') EngineVin, 
isnull(EngineVersion, '') EngineVersion, 
isnull([Note1], '') Note1,
isnull([Note2], '') Note2,
isnull([Note3], '') Note3,
isnull([Note4], '') Note4,
isnull([Note5], '') Note5,
isnull([Note6], '') Note6,
isnull([Note7], '') Note7,
isnull([Note8], '') Note8,
isnull([Note9], '') Note9,
isnull([Note10], '') Note10,
isnull([Note11], '') Note11,
isnull([Note12], '') Note12,
isnull([Note13], '') Note13,
isnull([Note14], '') Note14,
isnull([Note15], '') Note15,
isnull([Note16], '') Note16,
isnull(mfrlabel, '') mfrlabel,
count (spart) spart_count
,(select distinct ', ' + sku from dbo.PAStkFeedStage B with (nolock) where A.Basevehicle = b.Basevehicle and A.PartTerminologyID = B.PartTerminologyID and A.sfran = B.sfran and
isnull(A.SubModel, '') = isnull(B.SubModel, '') and  isnull(A.[MfrBodyCode], '') = isnull(B.[MfrBodyCode], '') and isnull(A.[PositionID], '') = isnull(B.[PositionID], '')
and isnull(A.[EngineBase], '') = isnull(B.[EngineBase], '') and isnull(A.[EngineDesignation], '') = isnull(B.[EngineDesignation], '')  and isnull(A.[DriveType], '') = isnull(B.[DriveType], '')
and isnull(A.[FuelType], '') = isnull(B.[FuelType], '') and isnull(A.[FuelDeliveryType], '') = isnull(B.[FuelDeliveryType], '')  and isnull(A.[CylinderHeadType], '') = isnull(B.[CylinderHeadType], '')
and isnull(A.[BodyType], '') = isnull(B.[BodyType], '') and isnull(A.[Aspiration], '') = isnull(B.[Aspiration], '') and isnull(A.TransmissionControlType,'') = isnull(B.TransmissionControlType,'')
and isnull(A.BedType, '') = isnull(B.BedType, '') and isnull(A.BodyNumDoors, '') = isnull(B.BodyNumDoors, '') 
and isnull(A.BrakeABS, '') = isnull(B.BrakeABS, '') and isnull(A.BrakeSystem, '') =  isnull(B.BrakeSystem, '') and isnull(A.FrontBrakeType, '') = isnull(B.FrontBrakeType, '')
and isnull(A.FrontSpringType, '') = isnull(B.FrontSpringType, '') and isnull(A.IgnitionSystemType, '') =  isnull(B.IgnitionSystemType, '') and
isnull(A.FuelDeliverySubType, '') = isnull(B.FuelDeliverySubType, '') and isnull(A.FuelSystemControlType, '') = isnull(B.FuelSystemControlType, '')
and isnull(A.FuelSystemDesign, '') = isnull(B.FuelSystemDesign, '') and isnull(A.RearBrakeType, '') =  isnull(B.RearBrakeType, '') and
isnull(A.RearSpringType, '') = isnull(B.RearSpringType, '')  and isnull(A.SteeringSystem, '') =  isnull(B.SteeringSystem, '') and isnull(A.SteeringType, '') = isnull(B.SteeringType, '')
and isnull(A.TransmissionMfr, '') = isnull(B.TransmissionMfr, '') and isnull(A.TransmissionNumSpeeds, '') = isnull(B.TransmissionNumSpeeds, '') and
isnull(A.TransmissionType, '') = isnull(B.TransmissionType, '') and  isnull(A.ValvesPerEngine, '') =  isnull(B.ValvesPerEngine, '') and
isnull(A.WheelBase, '') = isnull(B.WheelBase, '') and isnull(A.EngineMfr, '') = isnull(B.EngineMfr, '') and isnull(A.EngineVin, '') = isnull(B.EngineVin, '') and 
isnull(A.EngineVersion, '') = isnull(B.EngineVersion, '') and
isnull(A.[Note1], '') = isnull(B.[Note1], '') and isnull(A.[Note2], '') = isnull(B.[Note2], '') and
isnull(A.[Note3], '') = isnull(B.[Note3], '') and isnull(A.[Note4], '') = isnull(B.[Note4], '') and
isnull(A.[Note5], '') = isnull(B.[Note5], '') and isnull(A.[Note6], '') = isnull(B.[Note6], '') and
isnull(A.[Note7], '') = isnull(B.[Note7], '') and isnull(A.[Note8], '') = isnull(B.[Note8], '') and
isnull(A.[Note9], '') = isnull(B.[Note9], '') and isnull(A.[Note10], '') = isnull(B.[Note10], '') and
isnull(A.[Note11], '') = isnull(B.[Note11], '') and isnull(A.[Note12], '') = isnull(B.[Note12], '') and
isnull(A.[Note13], '') = isnull(B.[Note13], '') and isnull(A.[Note14], '') = isnull(B.[Note14], '') and
isnull(A.[Note15], '') = isnull(B.[Note15], '') and isnull(A.[Note15], '') = isnull(B.[Note15], '') and
isnull(A.[mfrlabel], '') = isnull(B.[mfrlabel], '')
 For XML Path('')) as sku
FROM [dbo].PAStkFeedStage A with (nolock)
where  sfran = @lc 
group by [sfran]
,[Basevehicle]
,isnull([SubModel],'')
,[PartTerminologyID]
,isnull([MfrBodyCode], '')
,isnull([PositionID], '')
,isnull([EngineBase], '')
,isnull([EngineDesignation], '')
,isnull([DriveType], '')
,isnull([FuelType], '')
,isnull([FuelDeliveryType], '')
,isnull([CylinderHeadType], '')
,isnull(bodytype, '')
,isnull(Aspiration, '')
,isnull(TransmissionControlType,'')
,isnull(BedType, ''), isnull(BodyNumDoors, ''),
 isnull(BrakeABS, ''), isnull(BrakeSystem, ''),  isnull(FrontBrakeType, ''), isnull(FrontSpringType, ''), 
 isnull(IgnitionSystemType, ''), isnull(FuelDeliverySubType, ''), isnull(FuelSystemControlType, ''), 
isnull(FuelSystemDesign, ''), isnull(RearBrakeType, ''), 
isnull(RearSpringType, ''), isnull(SteeringSystem, ''), isnull(SteeringType, ''),  isnull(TransmissionMfr, ''),
isnull(TransmissionNumSpeeds, ''), isnull(TransmissionType, ''), isnull(ValvesPerEngine, ''), 
isnull(WheelBase, ''), isnull(EngineMfr, ''), isnull(EngineVin, ''), 
isnull(EngineVersion, ''),
isnull([Note1], ''),
isnull([Note2], ''),
isnull([Note3], ''),
isnull([Note4], ''),
isnull([Note5], ''),
isnull([Note6], ''),
isnull([Note7], ''),
isnull([Note8], ''),
isnull([Note9], ''),
isnull([Note10], ''),
isnull([Note11], ''),
isnull([Note12], ''),
isnull([Note13], ''),
isnull([Note14], ''),
isnull([Note15], ''),
isnull([Note16], ''),
isnull(mfrlabel, '')
Having count (spart) > 1

END
GO
/****** Object:  StoredProcedure [dbo].[usp_insertACESNotes]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		GM
-- Create date: 6/14/2018
-- Description:	load aces notes into one column
--put the 15 note fields into one column called notes.  
-- =============================================
CREATE PROCEDURE [dbo].[usp_insertACESNotes]
	-- Add the parameters for the stored procedure here
	@x bigint,
	@y bigint
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;


    -- Insert statements for procedure here

Declare @DT datetime2(7)
set @DT = GetDate()
	


;

With cteNotes(PAStockID, [sfran]
      ,[spart]
      ,[sku]
      ,[Basevehicle]
      ,[PartTerminologyID]
      ,[EngineBase]
      ,[SubModel]
      ,[FuelType]
	  ,[FuelDeliveryType]
	  ,[Aspiration]
	  ,[EngineDesignation]
	  ,[CylinderHeadType]
	  ,[BodyType]
	  ,[MfrBodyCode]
	  ,[PositionID]
       ,NotesType, Notes)
  AS
(
select PAStockID, [sfran]
      ,[spart]
      ,[sku]
      ,[Basevehicle]
      ,[PartTerminologyID]
      ,[EngineBase]
      ,[SubModel]
      ,[FuelType]
	  ,[FuelDeliveryType]
	  ,[Aspiration]
	  ,[EngineDesignation]
	  ,[CylinderHeadType]
	  ,[BodyType]
	  ,[MfrBodyCode]
	  ,[PositionID]
       ,NotesType, Notes
from 
(select A.PAStockID, A.[sfran]
      ,A.[spart]
      ,A.[sku]
      ,A.[udfSPART]
      ,A.[Basevehicle]
      ,A.[PartTerminologyID]
      ,A.[EngineBase]
      ,A.[SubModel]
	  ,A.[FuelType]
	  ,A.[FuelDeliveryType]
	  ,A.[Aspiration]
	  ,A.[EngineDesignation]
	  ,A.[CylinderHeadType]
	  ,A.[BodyType]
	  ,A.[MfrBodyCode]
      ,A.[PositionID]
      ,Note1, Note2, Note3, Note4,  Note5, Note6,   Note7,  Note8, Note9,    
Note10, Note11, Note12,  Note13,  Note14,  Note15,  Note16
from dbo.PAStkFeedProduction A with (NOLOCK) left outer join dbo.ACESNotes B with (nolock) on A.PAStockID = B.PASTOCKID where  A.PAStockID between @x and @y) stu
unpivot
(
Notes for NotesType in (Note1, Note2, Note3, Note4, Note5, Note6, Note7, Note8, Note9, Note10, Note11, Note12, Note13, Note14, Note15, Note16) 
) as NT where len(notes) > 0)


INSERT INTO [dbo].[ACESNotes]
           ([PASTOCKID]
           ,[sfran]
           ,[spart]
           ,[sku]
           ,[Basevehicle]
           ,[PartTerminologyID]
           ,[EngineBase]
           ,[SubModel]
           ,[FuelType]
           ,[FuelDeliveryType]
           ,[Aspiration]
           ,[EngineDesignation]
           ,[CylinderHeadType]
           ,[BodyType]
           ,[MfrBodyCode]
           ,[PositionID]
           ,[NotesType]
           ,[Notes]
          ,[CreateDT])
Select PASTOCKID, [sfran]
      ,[spart]
      ,[sku]
      ,[Basevehicle]
      ,[PartTerminologyID]
      ,[EngineBase]
      ,[SubModel]
	  ,[FuelType]
           ,[FuelDeliveryType]
           ,[Aspiration]
           ,[EngineDesignation]
           ,[CylinderHeadType]
           ,[BodyType]
           ,[MfrBodyCode]
      ,[PositionID]
       ,NotesType, Ltrim(rtrim(value)) as Notes, @DT as CreateDT from cteNotes CROSS APPLY STRING_SPLIT(Notes, ';');





End  



GO
/****** Object:  StoredProcedure [dbo].[usp_InvalidVCDBApplications]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[usp_InvalidVCDBApplications] 

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
		SET NOCOUNT ON;

Declare @i bigint,
@x bigint,
@y bigint

set @y = 100000
Set @i = (Select count(*) from  PAStkFeedProduction with (nolock))
set @x = 1
while @x < @i
Begin

	--Aspiration	
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a
	where (pastockid between @x and @y) and invalid is NULL and a.Aspiration not in (select  aspirationid from [vcdb].dbo.Aspiration) and a.Aspiration is not null
	;

	--BodyType
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a
	where (pastockid between @x and @y) and invalid is NULL and a.BodyType not in (select BodyTypeID from [vcdb].dbo.BodyType) and a.BodyType is not null 
	;

	--submodel
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a left outer join
	vcdb.dbo.SubModel b on a.SubModel = b.SubModelID
	where (pastockid between @x and @y) and invalid is NULL and b.SubModelID is null and a.SubModel is not null
	;
	
	--mfrbodycode
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a left outer join
	vcdb.dbo.MfrBodyCode b on a.OrgMfrBodyCode = b.MfrBodyCodeID
	where (pastockid between @x and @y) and invalid is NULL  and b.MfrBodyCodeID is null and a.MfrBodyCode is not null
	;
	
	--cylinderheadtype
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a left outer join
	vcdb.dbo.CylinderHeadType b on a.CylinderHeadType = b.CylinderHeadTypeID
	where (pastockid between @x and @y) and invalid is NULL and b.CylinderHeadTypeID is null and a.CylinderHeadType is not null 
	;

	--EngineDesignation
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a left outer join
	vcdb.dbo.EngineDesignation b on a.EngineDesignation = b.EngineDesignationID
	where (pastockid between @x and @y) and invalid is NULL and b.EngineDesignationID is null and a.EngineDesignation is not null 
	;

	--DriveType
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a left outer join
	vcdb.dbo.DriveType b on a.DriveType = b.DriveTypeID
	where (pastockid between @x and @y) and invalid is NULL and b.DriveTypeID is null and a.DriveType is not null 
	;

	--TransmissionControlType
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a left outer join
	vcdb.dbo.TransmissionControlType b on a.TransmissionControlType = b.TransmissionControlTypeID
	where (pastockid between @x and @y) and invalid is NULL and b.TransmissionControlTypeID is null and a.TransmissionControlType is not null 
	;

		--FuelDeliveryType
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a left outer join
	vcdb.dbo.FuelDeliveryType b on a.FuelDeliveryType = b.FuelDeliveryTypeID
	where (pastockid between @x and @y) and invalid is NULL and b.FuelDeliveryTypeID is null and a.FuelDeliveryType is not null 
	;

	--FuelType
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a left outer join
	vcdb.dbo.FuelType b on a.FuelType = b.FuelTypeID
	where (pastockid between @x and @y) and invalid is NULL and b.FuelTypeID is null and a.FuelType is not null 
	;


	set @x = @x + 100000
	set @y = @y + 100000

End  
END
GO
/****** Object:  StoredProcedure [dbo].[usp_missingkeyattribute]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[usp_missingkeyattribute]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

   Declare @missingattrib table
(
sku varchar(50),
missingattrib varchar(50),
basevehicle int,
partterm int,
submodel varchar(50),
positionid int,
totalcount int,
minusnullcount int
)



DECLARE @lc VARCHAR(2),   
@bv int,
@part int,
@submodel varchar(50),
@positionid int

DECLARE db_cursor CURSOR FOR   
select sfran, Basevehicle, PartTerminologyID, isnull(submodel, '') as submodel, PositionID from  PAStkFeedStage
where sfran = 'AC' 
group by sfran, Basevehicle, PartTerminologyID, isnull(submodel, ''), PositionID
having count (sku) > 1


OPEN db_cursor    
FETCH NEXT FROM db_cursor INTO @lc, @bv, @part, @submodel, @positionid

WHILE @@FETCH_STATUS = 0    
BEGIN 



insert into @missingattrib(sku, basevehicle, partterm, submodel, positionid, missingattrib, totalcount, minusnullcount)
Select sku, @bv as basevehicle, @part as partterm, @submodel as submodel, @positionid as positionid, 'EngineBase' as missingattrib, count(distinct EngineBase) totalCount, 
(select count(distinct EngineBase) MinusNullCount  from PAStkFeedStage b where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid  and SubModel = @submodel and sfran = @lc and a.sku= b.sku and ISNULL(b.EngineBase, '') > 0) from PAStkFeedStage a
where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid and SubModel = @submodel and sfran = @lc
Group by sku
having count(distinct EngineBase) > 1
;

insert into @missingattrib(sku, basevehicle, partterm, submodel, positionid, missingattrib, totalcount, minusnullcount)
Select sku, @bv as basevehicle, @part as partterm, @submodel as submodel, @positionid as positionid, 'BodyType' as missingattrib, count(distinct BodyType) totalCount, (select count(distinct BodyType) MinusNullCount  from PAStkFeedStage b where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid  and SubModel = @submodel and sfran = @lc and a.sku= b.sku and ISNULL(b.BodyType, '') > 0) from PAStkFeedStage a
where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid and SubModel = @submodel and sfran = @lc
Group by sku
having count(distinct BodyType) > 1
;


insert into @missingattrib(sku, basevehicle, partterm, submodel, positionid, missingattrib, totalcount, minusnullcount)
Select sku, @bv as basevehicle, @part as partterm, @submodel as submodel, @positionid as positionid, 'Aspiration' as missingattrib, count(distinct Aspiration) totalCount, (select count(distinct Aspiration) MinusNullCount  from PAStkFeedStage b where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid  and SubModel = @submodel and sfran = @lc and a.sku= b.sku and ISNULL(b.Aspiration, '') > 0) from PAStkFeedStage a
where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid and SubModel = @submodel and sfran = @lc
Group by sku
having count(distinct Aspiration) > 1
;


insert into @missingattrib(sku, basevehicle, partterm, submodel, positionid, missingattrib, totalcount, minusnullcount)
Select sku, @bv as basevehicle, @part as partterm, @submodel as submodel, @positionid as positionid, 'EngineDesignation' as missingattrib, count(distinct EngineDesignation) totalCount, (select count(distinct EngineDesignation) MinusNullCount
from PAStkFeedStage b where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid  and SubModel = @submodel and sfran = @lc
and a.sku= b.sku and ISNULL(b.EngineDesignation, '') > 0) from PAStkFeedStage a
where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid and SubModel = @submodel and sfran = @lc
Group by sku
having count(distinct EngineDesignation) > 1
;

insert into @missingattrib(sku, basevehicle, partterm, submodel, positionid, missingattrib, totalcount, minusnullcount)
Select sku, @bv as basevehicle, @part as partterm, @submodel as submodel, @positionid as positionid, 'FuelType' as missingattrib, count(distinct FuelType) totalCount, (select count(distinct FuelType) MinusNullCount
from PAStkFeedStage b where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid  and SubModel = @submodel and sfran = @lc
and a.sku= b.sku and ISNULL(b.FuelType, '') > 0) from PAStkFeedStage a
where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid and SubModel = @submodel and sfran = @lc
Group by sku
having count(distinct FuelType) > 1
;

insert into @missingattrib(sku, basevehicle, partterm, submodel, positionid, missingattrib, totalcount, minusnullcount)
Select sku, @bv as basevehicle, @part as partterm, @submodel as submodel, @positionid as positionid, 'DriveType' as missingattrib, count(distinct DriveType) totalCount, (select count(distinct DriveType) MinusNullCount
from PAStkFeedStage b where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid  and SubModel = @submodel and sfran = @lc
and a.sku= b.sku and ISNULL(b.DriveType, '') > 0) from PAStkFeedStage a
where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid and SubModel = @submodel and sfran = @lc
Group by sku
having count(distinct DriveType) > 1
;

insert into @missingattrib(sku, basevehicle, partterm, submodel, positionid, missingattrib, totalcount, minusnullcount)
Select sku, @bv as basevehicle, @part as partterm, @submodel as submodel, @positionid as positionid, 'MfrBodyCode' as missingattrib, count(distinct MfrBodyCode) totalCount, (select count(distinct MfrBodyCode) MinusNullCount
from PAStkFeedStage b where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid  and SubModel = @submodel and sfran = @lc
and a.sku= b.sku and ISNULL(b.MfrBodyCode, '') > 0) from PAStkFeedStage a
where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid and SubModel = @submodel and sfran = @lc
Group by sku
having count(distinct MfrBodyCode) > 1
;

insert into @missingattrib(sku, basevehicle, partterm, submodel, positionid, missingattrib, totalcount, minusnullcount)
Select sku, @bv as basevehicle, @part as partterm, @submodel as submodel, @positionid as positionid, 'CylinderHeadType' as missingattrib, count(distinct CylinderHeadType) totalCount, (select count(distinct CylinderHeadType) MinusNullCount
from PAStkFeedStage b where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid  and SubModel = @submodel and sfran = @lc
and a.sku= b.sku and ISNULL(b.CylinderHeadType, '') > 0) from PAStkFeedStage a
where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid and SubModel = @submodel and sfran = @lc
Group by sku
having count(distinct CylinderHeadType) > 1
;

insert into @missingattrib(sku, basevehicle, partterm, submodel, positionid, missingattrib, totalcount, minusnullcount)
Select sku, @bv as basevehicle, @part as partterm, @submodel as submodel, @positionid as positionid, 'FuelDeliveryType' as missingattrib, count(distinct FuelDeliveryType) totalCount, (select count(distinct FuelDeliveryType) MinusNullCount
from PAStkFeedStage b where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid  and SubModel = @submodel and sfran = @lc
and a.sku= b.sku and ISNULL(b.FuelDeliveryType, '') > 0) from PAStkFeedStage a
where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid and SubModel = @submodel and sfran = @lc
Group by sku
having count(distinct FuelDeliveryType) > 1
;

insert into @missingattrib(sku, basevehicle, partterm, submodel, positionid, missingattrib, totalcount, minusnullcount)
Select sku, @bv as basevehicle, @part as partterm, @submodel as submodel, @positionid as positionid, 'TransmissionControlType' as missingattrib, count(distinct TransmissionControlType) totalCount, (select count(distinct TransmissionControlType) MinusNullCount
from PAStkFeedStage b where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid  and SubModel = @submodel and sfran = @lc
and a.sku= b.sku and ISNULL(b.TransmissionControlType, '') > 0) from PAStkFeedStage a
where Basevehicle = @bv and PartTerminologyID = @part and PositionID = @positionid and SubModel = @submodel and sfran = @lc
Group by sku
having count(distinct TransmissionControlType) > 1
;

INSERT INTO [dbo].[missingkeyattribute]([sku]
           ,[basevehicle]
           ,[partterm]
           ,[submodel]
           ,[positionid]
		   ,[missingattrib])
Select sku, basevehicle, partterm, submodel, positionid, missingattrib from @missingattrib
where totalcount <> minusnullcount;


delete from @missingattrib;


FETCH NEXT FROM db_cursor INTO @lc, @bv, @part, @submodel, @positionid 



END    

CLOSE db_cursor    
DEALLOCATE db_cursor

End
GO
/****** Object:  StoredProcedure [dbo].[usp_NetChange]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[usp_NetChange]
	-- Add the parameters for the stored procedure here

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

Truncate Table [dbo].[PAStkFeedProduction]


Declare @i bigint,
@x bigint,
@y bigint

    Declare @DT datetime2(7) 

set @y = 100000
Set @i = (Select count(*) from  PAStkFeedStage with (nolock))
set @x =1
while @x < @i
Begin




	--Set Datetime variable
	set @DT = GetDate()

   Merge PAStkFeedProduction prod
   Using (SELECT [sfran]
      ,[spart]
      ,[Basevehicle]
      ,[PartTerminologyID]
      ,[ChildPartTermID]
      ,isnull([EngineBase],'') as EngineBase
      ,isnull([SubModel],'') as Submodel
      ,[FitNote]
      ,[MfrLabel]
      ,[Note1]
      ,[Note2]
      ,[Note3]
      ,[Note4]
      ,[Note5]
      ,[Note6]
      ,[Note7]
      ,[Note8]
      ,[Note9]
      ,[Note10]
      ,[Note11]
      ,[Note12]
      ,[Note13]
      ,[Note14]
      ,[Note15]
      ,[Note16]
      ,[PositionID]
      ,[ChildPositionID]
      ,isnull([DriveType],'') as DriveType
      ,isnull([Aspiration],'') as Aspiration
      ,[BedLength]
      ,[BedType]
      ,[BodyNumDoors]
      ,[BodyType]
      ,[BrakeABS]
      ,[BrakeSystem]
      ,isnull([CylinderHeadType],'') as CylinderHeadType
      ,isnull([EngineDesignation],'') as EngineDesignation
      ,[FrontBrakeType]
      ,[FrontSpringType]
      ,[IgnitionSystemType]
      ,[FuelDeliverySubType]
      ,isnull([FuelDeliveryType],'') as FuelDeliveryType
      ,[FuelSystemControlType]
      ,[FuelSystemDesign]
      ,isnull([FuelType],'') as FuelType
      ,isnull([MfrBodyCode],'') as MfrBodyCode
      ,[RearBrakeType]
      ,[RearSpringType]
      ,isnull([Region],'') as Region
      ,[SteeringSystem]
      ,[SteeringType]
      ,isnull([TransmissionControlType],'') as TransmissionControlType
      ,[TransmissionMfr]
      ,[TransmissionNumSpeeds]
      ,[TransmissionType]
      ,[ValvesPerEngine]
      ,[WheelBase]
      ,[EngineMfr]
      ,[EngineVin]
      ,[EngineVersion]
      ,[Percarqty]
      ,[sku]
      ,[udfSPART]
      ,[LoadDate]
  FROM [ACES].[dbo].[PAStkFeedStage] where RecordID between 1 and 100000) stg
   ON (prod.Origsku = stg.sku and prod.Basevehicle = stg.Basevehicle and prod.PartTerminologyID = stg.PartTerminologyID and
   isnull(prod.OrigEngineBase,'') = stg.EngineBase and isnull(prod.origsubmodel,'') = stg.submodel and isnull(prod.origDriveType,'') = stg.DriveType and
	isnull(prod.OrigCylinderHeadType,'') = stg.CylinderHeadType and  isnull(prod.OrigEngineDesignation,'') = stg.EngineDesignation and
	isnull(prod.OrigFuelDeliveryType,'') = stg.FuelDeliveryType and isnull(prod.OrigFuelType,'') = stg.FuelType and isnull(prod.OrgMfrBodyCode,'') = stg.MfrBodyCode
	and isnull(prod.OrigTransmissionControlType,'') = stg.TransmissionControlType and prod.positionid = stg.positionid)
 When not matched by target -- If line code and partnum does not exist insert record
 then INSERT ([sfran], [spart], [Basevehicle], [PartTerminologyID], [OrigPartTermID], [EngineBase], [OrigEngineBase],
              [SubModel],[OrigSubmodel], [FitNote], [MfrLabel], [Note1], [Note2], [Note3], [Note4], [Note5], [Note6],
              [Note7], [Note8], [Note9], [Note10], [Note11], [Note12], [Note13], [Note14], [Note15], [Note16], [PositionID],
              [OrigPositionID], [DriveType], [OrigDriveType], [Aspiration], [OrigAspiration], [BedLength], [BedType], [BodyNumDoors],
              [BodyType], [BrakeABS], [BrakeSystem], [CylinderHeadType], [OrigCylinderHeadType], [EngineDesignation], [OrigEngineDesignation],
              [FrontBrakeType], [FrontSpringType], [IgnitionSystemType], [FuelDeliverySubType], [FuelDeliveryType], [OrigFuelDeliveryType],
              [FuelSystemControlType], [FuelSystemDesign],[FuelType], [OrigFuelType], [MfrBodyCode], [OrgMfrBodyCode], [RearBrakeType],
              [RearSpringType], [Region], [SteeringSystem], [SteeringType], [TransmissionControlType], [OrigTransmissionControlType],
			  [TransmissionMfr], [TransmissionNumSpeeds], [TransmissionType], [ValvesPerEngine], [WheelBase], [EngineMfr], [EngineVin],
			  [EngineVersion], [Percarqty], [sku], [udfSPART], [LoadDT], [OrigBodyType], [Origsku])
     VALUES(stg.sfran, stg.spart, stg.[Basevehicle], stg.PartTerminologyID, stg.PartTerminologyID, stg.EngineBase, stg.EngineBase,
	 stg.Submodel, stg.Submodel, stg.FitNote, stg.MfrLabel, stg.Note1, stg.Note2, stg.Note3, stg.Note4, stg.Note5, stg.Note6, 
	 stg.Note7, stg.Note8, stg.Note9, stg.Note10, stg.Note11, stg.Note12, stg.Note13, stg.Note14, stg.Note15, stg.Note16, stg.PositionID, 
	 stg.PositionID, stg.DriveType, stg.DriveType, stg.Aspiration, stg.Aspiration, stg.BedLength, stg.BedType, stg.BodyNumDoors, 
	 stg.BodyType, stg.BrakeABS, stg.BrakeSystem, stg.CylinderHeadType, stg.CylinderHeadType, stg.EngineDesignation, stg.EngineDesignation, 
	 stg.FrontBrakeType, stg.FrontSpringType, stg.IgnitionSystemType, stg.FuelDeliverySubType, stg.FuelDeliveryType, stg.FuelDeliveryType, 
	 stg.FuelSystemControlType, stg.FuelSystemDesign, stg.FuelType, stg.FuelType, stg.MfrBodyCode, stg.MfrBodyCode, stg.RearBrakeType, 
	 stg.RearSpringType, stg.Region, stg.[SteeringSystem], stg.[SteeringType], stg.[TransmissionControlType], stg.[TransmissionControlType], 
	 stg.[TransmissionMfr], stg.[TransmissionNumSpeeds], stg.[TransmissionType],  stg.[ValvesPerEngine], stg.[WheelBase], stg.[EngineMfr], stg.[EngineVin], 
	 stg.[EngineVersion], stg.[Percarqty], stg.[sku], stg.[udfSPART], stg.[LoadDate], stg.Bodytype, stg.sku) 
	 When Matched and (prod.Note1 <> stg.Note1 and prod.Note2 <> stg.Note2 and prod.Note3 <> stg.Note3 and
	 prod.Note4 <> stg.Note4 and prod.Note5 <> stg.Note5 and prod.Note6 <> stg.Note6 and
	 prod.Note7 <> stg.Note7 and prod.Note8 <> stg.Note8 and prod.Note9 <> stg.Note9 and
     prod.Note10 <> stg.Note10 and prod.Note11 <> stg.Note11 and prod.Note12 <> stg.Note12 and
	 prod.Note13 <> stg.Note13 and prod.Note14 <> stg.Note14 and prod.Note15 <> stg.Note15 and prod.Note16 <> stg.Note16)
	 Then Update 
	set prod.[ModifiedDT] = @DT, prod.Note1 = stg.Note1, prod.Note2 = stg.Note2, prod.Note3 = stg.Note3,
	 prod.Note4 = stg.Note4, prod.Note5 = stg.Note5, prod.Note6 = stg.Note6, prod.Note7 = stg.Note7,
	 prod.Note8 = stg.Note8, prod.Note9 = stg.Note9, prod.Note10 = stg.Note10, prod.Note11 = stg.Note11, prod.Note12 = stg.Note12,
	 prod.Note13 = stg.Note13, prod.Note14  = stg.Note14, prod.Note15 = stg.Note15, prod.Note16 = stg.Note16
	 ;

	 
	set @x = @x + 100000
	set @y = @y + 100000



End  


END

GO
/****** Object:  StoredProcedure [dbo].[usp_nullfitment]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[usp_nullfitment]
@lc char(2)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

INSERT INTO [dbo].[nullfitment]
           ([sfran]
           ,[basevehicle]
           ,[PartTerminologyID]
           ,[submodel]
           ,[PositionID]
           ,[BodyType]
           ,[EngineBase]
           ,[Aspiration]
           ,[EngineDesignation]
           ,[FuelType]
           ,[DriveType]
           ,[MfrBodyCode]
           ,[CylinderHeadType]
           ,[FuelDeliveryType]
           ,[TransmissionControlType]
           ,[Note1]
           ,[Note2]
           ,[Note3]
           ,[Note4]
           ,[Note5]
           ,[Note6]
           ,[Note7]
           ,[Note8]
           ,[Note9]
           ,[Note10]
           ,[Note11]
           ,[Note12]
           ,[Note13]
           ,[Note14]
           ,[Note15]
           ,[Note16]
		   ,mfrlabel
		    ,[instances]
           ,[sku])
	Select  sfran, basevehicle, PartTerminologyID, submodel, [PositionID],  isnull(Bodytype, '') as BodyType, isnull(EngineBase, '') as EngineBase, isnull(Aspiration, '') as Aspiration, isnull(EngineDesignation, '') as EngineDesignation,
isnull(FuelType, '') as FuelType,  isnull(DriveType, '') as DriveType, isnull(MfrBodyCode, '') as MfrBodyCode,
isnull(CylinderHeadType,'') as CylinderHeadType, isnull(FuelDeliveryType,'') as FuelDeliveryType, isnull(A.TransmissionControlType,'') as TransmissionControlType, 
isnull([Note1], '') Note1,
isnull([Note2], '') Note2,
isnull([Note3], '') Note3,
isnull([Note4], '') Note4,
isnull([Note5], '') Note5,
isnull([Note6], '') Note6,
isnull([Note7], '') Note7,
isnull([Note8], '') Note8,
isnull([Note9], '') Note9,
isnull([Note10], '') Note10,
isnull([Note11], '') Note11,
isnull([Note12], '') Note12,
isnull([Note13], '') Note13,
isnull([Note14], '') Note14,
isnull([Note15], '') Note15,
isnull([Note16], '') Note16,
isnull(mfrlabel, '') mfrlabel,
count(distinct spart) as instances,
(select distinct ', ' + spart from [dbo].[PAStkFeedStage]  B with (nolock) where A.Basevehicle = b.Basevehicle and A.PartTerminologyID = B.PartTerminologyID and A.sfran = B.sfran and
isnull(A.SubModel, '') = isnull(B.SubModel, '') and   isnull(A.[PositionID], '') = isnull(B.[PositionID], '')
and isnull(A.[EngineBase], '') = isnull(B.[EngineBase], '') and isnull(A.[EngineDesignation], '') = isnull(B.[EngineDesignation], '')  and isnull(A.[DriveType], '') = isnull(B.[DriveType], '')
and isnull(A.[FuelType], '') = isnull(B.[FuelType], '') and isnull(A.[FuelDeliveryType], '') = isnull(B.[FuelDeliveryType], '')  and isnull(A.[CylinderHeadType], '') = isnull(B.[CylinderHeadType], '')
and isnull(A.[BodyType], '') = isnull(B.[BodyType], '') and isnull(A.[Aspiration], '') = isnull(B.[Aspiration], '') 
and isnull(A.TransmissionControlType,'') = isnull(B.TransmissionControlType,'') and
isnull(A.[Note1], '') = isnull(B.[Note1], '') and isnull(A.[Note2], '') = isnull(B.[Note2], '') and
isnull(A.[Note3], '') = isnull(B.[Note3], '') and isnull(A.[Note4], '') = isnull(B.[Note4], '') and
isnull(A.[Note5], '') = isnull(B.[Note5], '') and isnull(A.[Note6], '') = isnull(B.[Note6], '') and
isnull(A.[Note7], '') = isnull(B.[Note7], '') and isnull(A.[Note8], '') = isnull(B.[Note8], '') and
isnull(A.[Note9], '') = isnull(B.[Note9], '') and isnull(A.[Note10], '') = isnull(B.[Note10], '') and
isnull(A.[Note11], '') = isnull(B.[Note11], '') and isnull(A.[Note12], '') = isnull(B.[Note12], '') and
isnull(A.[Note13], '') = isnull(B.[Note13], '') and isnull(A.[Note14], '') = isnull(B.[Note14], '') and
isnull(A.[Note15], '') = isnull(B.[Note15], '') and isnull(A.[Note15], '') = isnull(B.[Note15], '') and
isnull(A.[mfrlabel], '') = isnull(B.[mfrlabel], '')
 For XML Path('')) as sku from [dbo].[PAStkFeedStage] A with (nolock)
where sfran = @lc and
len(isnull(Bodytype, '') + isnull(EngineBase, '') + isnull(Aspiration, '') + isnull(EngineDesignation, '') + isnull(MfrBodyCode, '') + isnull(DriveType, '') + 
 isnull(CylinderHeadType,'') + isnull(FuelDeliveryType,'') + isnull(TransmissionControlType,'') + isnull([FuelType], '')) = 0
Group by  sfran, basevehicle, PartTerminologyID, submodel, [PositionID], isnull(Bodytype, ''), isnull(EngineBase, ''), isnull(Aspiration, ''), 
isnull(EngineDesignation, ''), isnull(FuelType, ''), isnull(DriveType, ''),  isnull(MfrBodyCode, ''),
isnull(CylinderHeadType,''), isnull(FuelDeliveryType,''), isnull(A.TransmissionControlType,''), isnull([Note1], ''),
isnull([Note2], ''),
isnull([Note3], ''),
isnull([Note4], ''),
isnull([Note5], ''),
isnull([Note6], ''),
isnull([Note7], ''),
isnull([Note8], ''),
isnull([Note9], ''),
isnull([Note10], ''),
isnull([Note11], ''),
isnull([Note12], ''),
isnull([Note13], ''),
isnull([Note14], ''),
isnull([Note15], ''),
isnull([Note16], ''),
isnull(mfrlabel, '')
having count(distinct spart) > 1
END
GO
/****** Object:  StoredProcedure [dbo].[usp_PartsToBlackList]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		GM
-- Create date: 10/9/2018
-- Description:	Set invalid flag to true for sku that are in PartsToBlackList table
-- =============================================
CREATE PROCEDURE [dbo].[usp_PartsToBlackList]

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

Declare @i bigint,
@x bigint,
@y bigint

set @y = 100000
Set @i = (Select count(*) from  PAStkFeedProduction with (nolock))
set @x =1
while @x < @i
Begin

	--preform update to invalid field	
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a, 
	[dbo].[PartsToBlackList] b
	where a.sku = b.sku and pastockid between @x and @y 

	set @x = @x + 100000
	set @y = @y + 100000



End  


END
GO
/****** Object:  StoredProcedure [dbo].[usp_PAStkFeedProduction]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[usp_PAStkFeedProduction]

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

Truncate Table [dbo].[PAStkFeedProduction]


Declare @i bigint,
@x bigint,
@y bigint

    Declare @DT datetime2(7) 

set @y = 100000
Set @i = (Select count(*) from  PAStkFeedStage with (nolock))
set @x =1
while @x < @i
Begin




	--Set Datetime variable
	set @DT = GetDate()

   Insert into PAStkFeedProduction([sfran], [spart], [Basevehicle], [PartTerminologyID], [OrigPartTermID], [EngineBase], [OrigEngineBase],
              [SubModel],[OrigSubmodel], [FitNote], [MfrLabel], [Note1], [Note2], [Note3], [Note4], [Note5], [Note6],
              [Note7], [Note8], [Note9], [Note10], [Note11], [Note12], [Note13], [Note14], [Note15], [Note16], [PositionID],
              [OrigPositionID], [DriveType], [OrigDriveType], [Aspiration], [OrigAspiration], [BedLength], [BedType], [BodyNumDoors],
              [BodyType], [BrakeABS], [BrakeSystem], [CylinderHeadType], [OrigCylinderHeadType], [EngineDesignation], [OrigEngineDesignation],
              [FrontBrakeType], [FrontSpringType], [IgnitionSystemType], [FuelDeliverySubType], [FuelDeliveryType], [OrigFuelDeliveryType],
              [FuelSystemControlType], [FuelSystemDesign],[FuelType], [OrigFuelType], [MfrBodyCode], [OrgMfrBodyCode], [RearBrakeType],
              [RearSpringType], [Region], [SteeringSystem], [SteeringType], [TransmissionControlType], [OrigTransmissionControlType],
			  [TransmissionMfr], [TransmissionNumSpeeds], [TransmissionType], [ValvesPerEngine], [WheelBase], [EngineMfr], [EngineVin],
			  [EngineVersion], [Percarqty], [sku], [udfSPART], [LoadDT], [OrigBodyType], [Origsku])
     	Select stg.sfran, stg.spart, stg.[Basevehicle], 
		Case 
			when Ltrim(rtrim(stg.PartTerminologyID)) = '' then Null
			else  stg.PartTerminologyID
         End as PartTerminologyID,
		 Case 
			when Ltrim(rtrim(stg.PartTerminologyID)) = '' then Null
			else  stg.PartTerminologyID
         End as OrigPartTermID, 
		 Case 
			when Ltrim(rtrim(stg.EngineBase)) = '' then Null
			else  stg.EngineBase
         End as EngineBase, 
		 Case 
			when Ltrim(rtrim(stg.EngineBase)) = '' then Null
			else  stg.EngineBase
         End as OrigEngineBase,
		 Case 
			when Ltrim(rtrim(stg.Submodel)) = '' then Null
			else  stg.Submodel
         End as Submodel, 
		  Case 
			when Ltrim(rtrim(stg.Submodel)) = '' then Null
			else  stg.Submodel
         End as OrigSubmodel, stg.FitNote, stg.MfrLabel, stg.Note1, stg.Note2, stg.Note3, stg.Note4, stg.Note5, stg.Note6, 
	   stg.Note7, stg.Note8, stg.Note9, stg.Note10, stg.Note11, stg.Note12, stg.Note13, stg.Note14, stg.Note15, stg.Note16, 
	   Case 
			when Ltrim(rtrim(stg.PositionID)) = '' then Null
			else  stg.PositionID
       End as PositionID, 
	   Case 
			when Ltrim(rtrim(stg.PositionID)) = '' then Null
			else  stg.PositionID
       End as OrigPositionID, 
	   Case 
			when Ltrim(rtrim(stg.DriveType)) = '' then Null
			else  stg.DriveType
       End as DriveType,  
	   Case 
			when Ltrim(rtrim(stg.DriveType)) = '' then Null
			else  stg.DriveType
       End as DriveType, 
	    Case 
			when Ltrim(rtrim(stg.Aspiration)) = '' then Null
			else  stg.Aspiration
      End as Aspiration, 
	  Case 
			when Ltrim(rtrim(stg.Aspiration)) = '' then Null
			else  stg.Aspiration
      End as Aspiration, 
	  Case 
			when Ltrim(rtrim(stg.BedLength)) = '' then Null
			else  stg.BedLength
      End as BedLength, 
	  Case 
			when Ltrim(rtrim(stg.BedType)) = '' then Null
			else  stg.BedType
      End as BedType,  
	  Case 
			when Ltrim(rtrim(stg.BodyNumDoors)) = '' then Null
			else  stg.BodyNumDoors
      End as BodyNumDoors, 
	 Case 
			when Ltrim(rtrim(stg.BodyType)) = '' then Null
			else  stg.BodyType
      End as BodyType, 
	   Case 
			when Ltrim(rtrim(stg.BrakeABS)) = '' then Null
			else  stg.BrakeABS
      End as BrakeABS, 
	  Case 
			when Ltrim(rtrim(stg.BrakeSystem)) = '' then Null
			else  stg.BrakeSystem
      End as BrakeSystem, 
	  Case 
			when Ltrim(rtrim(stg.CylinderHeadType)) = '' then Null
			else  stg.CylinderHeadType
      End as CylinderHeadType, 
	  Case 
			when Ltrim(rtrim(stg.CylinderHeadType)) = '' then Null
			else  stg.CylinderHeadType
      End as OrigCylinderHeadType, 
	  Case 
			when Ltrim(rtrim(stg.EngineDesignation)) = '' then Null
			else  stg.EngineDesignation
      End as EngineDesignation,
	   Case 
			when Ltrim(rtrim(stg.EngineDesignation)) = '' then Null
			else  stg.EngineDesignation
      End as OrigEngineDesignation, 
	  Case 
			when Ltrim(rtrim(stg.FrontBrakeType)) = '' then Null
			else  stg.FrontBrakeType
      End as FrontBrakeType, 
	  Case 
			when Ltrim(rtrim(stg.FrontSpringType)) = '' then Null
			else  stg.FrontSpringType
      End as FrontSpringType, 
	  Case 
			when Ltrim(rtrim(stg.IgnitionSystemType)) = '' then Null
			else  stg.IgnitionSystemType
      End as IgnitionSystemType, 
	  Case 
			when Ltrim(rtrim(stg.FuelDeliverySubType)) = '' then Null
			else  stg.FuelDeliverySubType
      End as FuelDeliverySubType, 
	  Case 
			when Ltrim(rtrim(stg.FuelDeliveryType)) = '' then Null
			else  stg.FuelDeliveryType
      End as FuelDeliveryType, 
	  Case 
			when Ltrim(rtrim(stg.FuelDeliveryType)) = '' then Null
			else  stg.FuelDeliveryType
      End as OrigFuelDeliveryType,
	  Case 
			when Ltrim(rtrim(stg.FuelSystemControlType)) = '' then Null
			else  stg.FuelSystemControlType
      End as FuelSystemControlType, 
	  Case 
			when Ltrim(rtrim(stg.FuelSystemDesign)) = '' then Null
			else  stg.FuelSystemDesign
      End as FuelSystemDesign, 
	  Case 
			when Ltrim(rtrim(stg.FuelType)) = '' then Null
			else  stg.FuelType
      End as FuelType, 
	  Case 
			when Ltrim(rtrim(stg.FuelType)) = '' then Null
			else  stg.FuelType
      End as OrigFuelType, 
	  Case 
			when Ltrim(rtrim(stg.MfrBodyCode)) = '' then Null
			else  stg.MfrBodyCode
      End as MfrBodyCode, 
	  Case 
			when Ltrim(rtrim(stg.MfrBodyCode)) = '' then Null
			else  stg.MfrBodyCode
      End as OrigMfrBodyCode, 
	  Case 
			when Ltrim(rtrim(stg.RearBrakeType)) = '' then Null
			else  stg.RearBrakeType
      End as RearBrakeType, 
	  Case 
			when Ltrim(rtrim(stg.RearSpringType)) = '' then Null
			else  stg.RearSpringType
      End as RearSpringType, 
	  Case 
			when Ltrim(rtrim(stg.Region)) = '' then Null
			else  stg.Region
      End as Region, 
	 Case 
			when Ltrim(rtrim(stg.SteeringSystem)) = '' then Null
			else  stg.SteeringSystem
      End as [SteeringSystem], 
	 Case 
			when Ltrim(rtrim(stg.SteeringType)) = '' then Null
			else  stg.SteeringType
      End as [SteeringType], 
	  Case 
			when Ltrim(rtrim(stg.TransmissionControlType)) = '' then Null
			else  stg.TransmissionControlType
      End as [TransmissionControlType], 
	 Case 
			when Ltrim(rtrim(stg.TransmissionControlType)) = '' then Null
			else  stg.TransmissionControlType
      End as [OrigTransmissionControlType], 
	  Case 
			when Ltrim(rtrim(stg.TransmissionMfr)) = '' then Null
			else  stg.TransmissionMfr
      End as [TransmissionMfr], 
	 Case 
			when Ltrim(rtrim(stg.TransmissionNumSpeeds)) = '' then Null
			else  stg.TransmissionNumSpeeds
      End as [TransmissionNumSpeeds], 
	  Case 
			when Ltrim(rtrim(stg.TransmissionType)) = '' then Null
			else  stg.TransmissionType
      End as [TransmissionType],  
	 Case 
			when Ltrim(rtrim(stg.ValvesPerEngine)) = '' then Null
			else  stg.ValvesPerEngine
      End as [ValvesPerEngine], 
	 Case 
			when Ltrim(rtrim(stg.WheelBase)) = '' then Null
			else  stg.WheelBase
      End as [WheelBase], 
	 Case 
			when Ltrim(rtrim(stg.EngineMfr)) = '' then Null
			else  stg.EngineMfr
      End as [EngineMfr], 
	 Case 
			when Ltrim(rtrim(stg.EngineVin)) = '' then Null
			else  stg.EngineVin
      End as [EngineVin], 
	   Case 
			when Ltrim(rtrim(stg.EngineVersion)) = '' then Null
			else  stg.EngineVersion
      End as [EngineVersion], stg.[Percarqty], stg.[sku], stg.[udfSPART], stg.[LoadDate],  Case 
			when Ltrim(rtrim(stg.BodyType)) = '' then Null
			else  stg.BodyType
      End as OrigBodyType, stg.sku from dbo.PAStkFeedStage stg
	 where RecordID between @x and @y
	 ;


	 
	--set positionid = NULL when positionid is 0, 1
	update	PAStkFeedProduction
	set positionID = NULL
	where positionid in (0, 1) and pastockid between @x and @y;

	
	--preform update to position and child positionid	
	Update A
	set A.PositionID = b.ParentPositionID,
	A.origPositionID = A.PositionID
	from PAStkFeedProduction a, 
	[dbo].[PositionControl] b
	where a.PositionID = b.childpositionid and pastockid between @x and @y and A.PartTerminologyID = b.PartTermID;

	--Update imc parts
	Update A
	set udfSpart = b.ToThisPart,
	a.sku = b.sku,
	ModifiedDT = GetDate()
	from  [dbo].[PAStkFeedProduction] A, [dbo].[IMCPartsUpdate] B
	where A.udfSPART = b.UpdateThisPartInStockfeed and a.sfran = b.PALC
	and pastockid between @x and @y;

	--BaseVehicle
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a left outer join
	vcdb.dbo.BaseVehicle b on a.Basevehicle = b.BaseVehicleID 
	where pastockid between @i and @y and invalid is NULL and b.BaseVehicleID is null;

	--Partterm
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a left outer join
	pcdb.dbo.parts b on a.PartTerminologyID = b.[PartTerminologyID]
	where pastockid between @i and @y and invalid is NULL and  b.PartTerminologyID is null;

	 
	set @x = @x + 100000
	set @y = @y + 100000



End  


END

GO
/****** Object:  StoredProcedure [dbo].[usp_StandardAcesPass1]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		GM
-- Create date: 8/29/2018
-- Description:	step 1 update on basevehicle, submodel, BodyTypeID, EngineBaseID
-- =============================================
CREATE PROCEDURE [dbo].[usp_StandardAcesPass1] 
	-- Add the parameters for the stored procedure here
	@x bigint,
	@y bigint
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	

--Aspiration
Update B
set B.Aspiration = NULL,
ModifiedDT = GetDate()
from 
(select BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID, count(distinct [AspirationID]) as AspirationID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID
having count(distinct AspirationID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.Submodel and A.BodyTypeID = B.BodyType 
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and invalid is NULL and B.Aspiration is not NULL
and (PAStockID between @x and @y)
;

--DriveType
Update B
set B.DriveType = NULL,
ModifiedDT = GetDate()
from 
(select BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID, 
count(distinct [DriveTypeID]) as DriveTypeID from dbo.StandardVCDB
--where makename = @make
group by BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID
having count(distinct DriveTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.Submodel and A.BodyTypeID = B.BodyType 
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and invalid is NULL and B.DriveType is not NULL
and (PAStockID between @x and @y)
;

--EngineDesignation
Update B
set B.EngineDesignation = NULL,
ModifiedDT = GetDate()
from 
(select BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID, 
count(distinct [EngineDesignationid]) as mfrbodycodeid
from dbo.StandardVCDB
--where makename = @make
group by BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID
having count(distinct EngineDesignationid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.Submodel and A.BodyTypeID = B.BodyType 
and A.EngineBaseID = B.EngineBase and isnull(B.region, 1) = 1 and invalid is NULL and B.EngineDesignation is not null
and (PAStockID between @x and @y)
;

--FuelDeliveryType
Update B
set B.FuelDeliveryType = NULL,
ModifiedDT = GetDate()
from 
(select BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID,
count(distinct [FuelDeliveryTypeID]) as FuelDeliveryTypeID
from dbo.StandardVCDB
--where makename = @make
group by BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID
having count(distinct FuelDeliveryTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.Submodel and A.BodyTypeID = B.BodyType 
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and invalid is NULL and B.FuelDeliveryType is not null
and (PAStockID between @x and @y)
;

--FuelType
Update B
set B.FuelType = NULL,
ModifiedDT = GetDate()
from 
(select BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID, 
count(distinct [FuelTypeID]) as FuelType
from dbo.StandardVCDB
--where makename = @make
group by BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID
having count(distinct FuelTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.Submodel and A.BodyTypeID = B.BodyType 
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and invalid is NULL and B.FuelType is not null
and (PAStockID between @x and @y)
;

--CylinderHeadType
Update B
set B.CylinderHeadType = NULL,
ModifiedDT = GetDate()
from 
(select BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID, 
count(distinct [CylinderHeadTypeID]) as [CylinderHeadTypeID] 
from dbo.StandardVCDB
--where makename = @make
group by BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID
having count(distinct CylinderHeadTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.Submodel and A.BodyTypeID = B.BodyType 
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and invalid is NULL and B.CylinderHeadType is not null
and (PAStockID between @x and @y)
;

--Transmission ControlType
Update B
set B.TransmissionControlType = NULL,
ModifiedDT = GetDate()
from 
(select BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID, 
count(distinct [TransmissionControlTypeID]) as [TransmissionControlTypeID]
from dbo.StandardVCDB
--where makename = @make
group by BaseVehicleID, yearid,makename, modelname, SubModelID, BodyTypeID, EngineBaseID
having count(distinct TransmissionControlTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.Submodel and A.BodyTypeID = B.BodyType 
and A.EngineBaseID = B.EngineBase and isnull(B.region, 1) = 1  and invalid is NULL and B.TransmissionControlType is not null
and (PAStockID between @x and @y)
;
END
GO
/****** Object:  StoredProcedure [dbo].[usp_StandardAcesPass2]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		GM
-- Create date: 8/29/2018
--step 2
--update on basevehicle, submodel, BodyTypeID, 
-- =============================================
CREATE PROCEDURE [dbo].[usp_StandardAcesPass2]
	-- Add the parameters for the stored procedure here
	@x bigint,
	@y bigint

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here

Declare @DT datetime2(7)
set @DT = GetDate()

--EngineBase
Update B
set B.EngineBase = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID, count(distinct EngineBaseID) as EngineBaseID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID
having count(distinct EngineBaseID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel and A.BodyTypeID = B.Bodytype 
and isnull(B.region, 1) = 1 and B.EngineBase is not NULL and invalid is null
and (PAStockID between @x and @y)
;


--Aspiration
Update B
set B.Aspiration = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID, count(distinct [AspirationID]) as AspirationID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID
having count(distinct AspirationID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel and A.BodyTypeID = B.Bodytype 
and isnull(B.region, 1) = 1 and  B.Aspiration is not NULL  and invalid is null
and (PAStockID between @x and @y)
;

--EngineDesignationID
Update B
set B.EngineDesignation = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID, count(distinct [EngineDesignationID]) as EngineDesignationID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID
having count(distinct EngineDesignationID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel and A.BodyTypeID = B.Bodytype 
and isnull(B.region, 1) = 1 and B.EngineDesignation is not NULL  and invalid is null
and (PAStockID between @x and @y)
;

--mfrbodycodeid
Update B
set B.mfrbodycode = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID, 
count(distinct [mfrbodycodeid]) as mfrbodycodeid
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID
having count(distinct mfrbodycodeid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel and A.BodyTypeID = B.Bodytype 
and isnull(B.region, 1) = 1 and B.MfrBodyCode is not NULL  and invalid is null
and (PAStockID between @x and @y)
;

--DriveTypeID
Update B
set B.DriveType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID, count(distinct [DriveTypeID]) as DriveTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID
having count(distinct drivetypeid) = 1) A,dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel and A.BodyTypeID = B.Bodytype 
and isnull(B.region, 1) = 1 and B.DriveType is not NULL  and invalid is null
and (PAStockID between @x and @y)
;

--FuelDeliveryTypeID
Update B
set B.FuelDeliveryType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID,
count(distinct [FuelDeliveryTypeID]) as FuelDeliveryTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID
having count(distinct FuelDeliveryTypeid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel and A.BodyTypeID = B.Bodytype 
and isnull(B.region, 1) = 1 and B.FuelDeliveryType is not NULL  and invalid is null
and (PAStockID between @x and @y)
;

--FuelTypeID
Update B
set B.FuelType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID,  count(distinct [FuelTypeID]) as FuelType
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID
having count(distinct FuelTypeid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel and A.BodyTypeID = B.Bodytype  
and isnull(B.region, 1) = 1 and B.FuelType is not NULL  and invalid is null
and (PAStockID between @x and @y)
;

--CylinderHeadTypeID
Update B
set B.CylinderHeadType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID, 
count(distinct [CylinderHeadTypeID]) as [CylinderHeadTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID
having count(distinct CylinderHeadTypeid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel and A.BodyTypeID = B.Bodytype 
and isnull(B.region, 1) = 1 and B.CylinderHeadType is not NULL  and invalid is null
and (PAStockID between @x and @y)
;

--TransmissionControlType
Update B
set B.TransmissionControlType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID, 
count(distinct [TransmissionControlTypeID]) as [TransmissionControlTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, submodelid, BodyTypeID
having count(distinct TransmissionControlTypeid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel and A.BodyTypeID = B.Bodytype 
and isnull(B.region, 1) = 1 and B.TransmissionControlType is not NULL  and invalid is null
and (PAStockID between @x and @y)
;
END
GO
/****** Object:  StoredProcedure [dbo].[usp_StandardAcesPass3]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		GM
-- Create date: 8/29/2017
--step 3
--update on basevehicle, submodel, 
-- =============================================
CREATE PROCEDURE [dbo].[usp_StandardAcesPass3]
	-- Add the parameters for the stored procedure here
	@x bigint,
	@y bigint
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here

Declare @DT datetime2(7)
set @DT = GetDate()


--BodyType
Update B
set B.BodyType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, submodelid, count(distinct BodyTypeID) as BodyTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid
having count(distinct BodyTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle and A.SubModelID = B.submodel  
and isnull(B.region, 1) = 1 and BodyType is not NULL and invalid is NULL
and (PAStockID between @x and @y)

--EngineBase
Update B
set B.EngineBase = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, submodelid, 
count(distinct EngineBaseID) as EngineBaseID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, SubModelID
having count(distinct EngineBaseID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel 
and isnull(B.region, 1) = 1 and EngineBase is not NULL  and invalid is NULL
and (PAStockID between @x and @y)
;


--Aspiration
Update B
set B.Aspiration = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, submodelid, count(distinct [AspirationID]) as AspirationID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid
having count(distinct AspirationID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel 
and isnull(B.region, 1) = 1 and  Aspiration is not NULL  and invalid is NULL
and (PAStockID between @x and @y)
;

--EngineDesignationID
Update B
set B.EngineDesignation = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid as submodelid, 
 count(distinct [EngineDesignationID]) as EngineDesignationID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid
having count(distinct EngineDesignationID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel 
and isnull(B.region, 1) = 1 and  EngineDesignation is not NULL  and invalid is NULL
and (PAStockID between @x and @y)
;

--mfrbodycodeid
Update B
set B.mfrbodycode = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid as submodelid, 
count(distinct [mfrbodycodeid]) as mfrbodycodeid
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid
having count(distinct mfrbodycodeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel 
and isnull(B.region, 1) = 1 and  mfrbodycode is not NULL  and invalid is NULL
and (PAStockID between @x and @y)
;



--DriveTypeID
Update B
set B.DriveType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid as submodelid, count(distinct [DriveTypeID]) as DriveTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid
having count(distinct DriveTypeID) = 1) A,dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel 
and isnull(B.region, 1) = 1 and  DriveType is not NULL  and invalid is NULL
and (PAStockID between @x and @y)
;


--FuelDeliveryTypeID
Update B
set B.FuelDeliveryType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid as submodelid, 
count(distinct [FuelDeliveryTypeID]) as FuelDeliveryTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid
having count(distinct FuelDeliveryTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel  
and isnull(B.region, 1) = 1 and FuelDeliveryType is not NULL  and invalid is NULL
and (PAStockID between @x and @y)
;

--FuelTypeID
Update B
set B.FuelType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid as submodelid,  
count(distinct [FuelTypeID]) as FuelType
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid
having count(distinct FuelTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel  
and isnull(B.region, 1) = 1 and B.FuelType is not NULL  and invalid is NULL
and (PAStockID between @x and @y)
;


--CylinderHeadTypeID
Update B
set B.CylinderHeadType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid as submodelid, count(distinct [CylinderHeadTypeID]) as [CylinderHeadTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid
having count(distinct CylinderHeadTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel  
and isnull(B.region, 1) = 1 and  CylinderHeadType  is not NULL  and invalid is NULL
and (PAStockID between @x and @y)
;

--TransmissionControlType
Update B
set B.TransmissionControlType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid as submodelid, count(distinct [TransmissionControlTypeID]) as [TransmissionControlTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid
having count(distinct TransmissionControlTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel  
and isnull(B.region, 1) = 1 and  TransmissionControlType is not NULL  and invalid is NULL
and (PAStockID between @x and @y)
;

END
GO
/****** Object:  StoredProcedure [dbo].[usp_StandardAcesPass4]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		GM
-- Create date: 8/29/2018
--step 4
--update on basevehicle,
-- =============================================
CREATE PROCEDURE [dbo].[usp_StandardAcesPass4]
	-- Add the parameters for the stored procedure here
@x bigint,
@y bigint

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

Declare @DT datetime2(7)
set @DT = GetDate()

--BodyType
Update B
set B.submodel = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, count(distinct submodelID) as submodelid
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname
having count(distinct submodelid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and isnull(B.region, 1) = 1 and submodel is not null and  invalid is null
and (PAStockID between @x and @y)
;


--BodyType
Update B
set B.BodyType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, count(distinct BodyTypeID) as BodyTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname
having count(distinct BodyTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and isnull(B.region, 1) = 1 and bodytype is not null and  invalid is null
and (PAStockID between @x and @y)
;

--EngineBase
Update B
set B.EngineBase = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, 
count(distinct EngineBaseID) as EngineBaseID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname
having count(distinct EngineBaseID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle   and isnull(B.region, 1) = 1 and EngineBase is not null  and  invalid is null
and (PAStockID between @x and @y)
;



--Aspiration
Update B
set B.Aspiration = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, count(distinct [AspirationID]) as AspirationID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname
having count(distinct AspirationID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle   and isnull(B.region, 1) = 1 and Aspiration is not null  and  invalid is null
and (PAStockID between @x and @y)
;


--EngineDesignationID
Update B
set B.EngineDesignation = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, count(distinct submodelID) as submodelid,
 count(distinct [EngineDesignationID]) as EngineDesignationID
from dbo.StandardVCDB
Group by BaseVehicleID, yearid,makename, modelname
having count(distinct EngineDesignationID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and isnull(B.region, 1) = 1 and EngineDesignation is not null and  invalid is null
and (PAStockID between @x and @y)
;


--mfrbodycodeid
Update B
set B.mfrbodycode = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, count(distinct [mfrbodycodeid]) as mfrbodycodeid
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname
having count(distinct mfrbodycodeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and isnull(B.region, 1) = 1 and MfrBodyCode is not null  and  invalid is null
and (PAStockID between @x and @y)
;

--DriveTypeID
Update B
set B.DriveType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, count(distinct submodelID) as submodelid, count(distinct [DriveTypeID]) as DriveTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname
having count(distinct DriveTypeID) = 1) A,dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and isnull(B.region, 1) = 1 and DriveType is not null  and  invalid is null
and (PAStockID between @x and @y)
;

--FuelDeliveryTypeID
Update B
set B.FuelDeliveryType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, count(distinct [FuelDeliveryTypeID]) as FuelDeliveryTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname
having count(distinct FuelDeliveryTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and isnull(B.region, 1) = 1 and FuelDeliveryType is not null and  invalid is null
and (PAStockID between @x and @y)
;


--FuelTypeID
Update B
set B.FuelType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname,  count(distinct [FuelTypeID]) as FuelType
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname
having count(distinct FuelTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle   and isnull(B.region, 1) = 1 and B.FuelType is not null  and  invalid is null
and (PAStockID between @x and @y)
;

--CylinderHeadTypeID
Update B
set B.CylinderHeadType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, count(distinct [CylinderHeadTypeID]) as [CylinderHeadTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname
having count(distinct CylinderHeadTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle   and isnull(B.region, 1) = 1 and CylinderHeadType is not null  and  invalid is null
and (PAStockID between @x and @y)
;

--TransmissionControlType
Update B
set B.TransmissionControlType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, count(distinct [TransmissionControlTypeID]) as [TransmissionControlTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname
having count(distinct TransmissionControlTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and isnull(B.region, 1) = 1 and TransmissionControlType is not null  and  invalid is null
and (PAStockID between @x and @y)
;
END
GO
/****** Object:  StoredProcedure [dbo].[usp_StandardAcesPass5_7]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		GM
-- Create date: 8/29/2018
-- =============================================
CREATE PROCEDURE [dbo].[usp_StandardAcesPass5_7]
	-- Add the parameters for the stored procedure here
	@x bigint,
	@y bigint
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;


Declare @DT datetime2(7)
set @DT = GetDate()

--step 5
--update on basevehicle, BodyTypeID, EngineBaseID
--Aspiration
Update B
set B.Aspiration = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID, count(distinct [AspirationID]) as AspirationID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID
having count(distinct AspirationID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.BodyTypeID = B.BodyType
and A.EngineBaseID = B.EngineBase and isnull(B.region, 1) = 1 and Aspiration is not null and invalid is NULL
and (PAStockID between @x and @y)
;


--DriveType
Update B
set B.DriveType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID, 
count(distinct [DriveTypeID]) as DriveTypeID from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID
having count(distinct DriveTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.BodyTypeID = B.BodyType 
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and DriveType is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--EngineDesignation
Update B
set B.EngineDesignation = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID,
count(distinct [EngineDesignationid]) as mfrbodycodeid
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID
having count(distinct EngineDesignationid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.BodyTypeID = B.BodyType 
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and EngineDesignation is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--FuelDeliveryType
Update B
set B.FuelDeliveryType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID,
count(distinct [FuelDeliveryTypeID]) as FuelDeliveryTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID
having count(distinct FuelDeliveryTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.BodyTypeID = B.BodyType 
and A.EngineBaseID = B.EngineBase   and isnull(B.region, 1) = 1 and FuelDeliveryType  is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--FuelType
Update B
set B.FuelType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID, 
count(distinct [FuelTypeID]) as FuelType
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID
having count(distinct FuelTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.BodyTypeID = B.BodyType 
and A.EngineBaseID = B.EngineBase   and isnull(B.region, 1) = 1 and B.FuelType is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--CylinderHeadType
Update B
set B.CylinderHeadType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID, 
count(distinct [CylinderHeadTypeID]) as [CylinderHeadTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID
having count(distinct CylinderHeadTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.BodyTypeID = B.BodyType
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and CylinderHeadType is not null and invalid is NULL
and (PAStockID between @x and @y)
;


--Transmission ControlType
Update B
set B.TransmissionControlType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID, 
count(distinct [TransmissionControlTypeID]) as [TransmissionControlTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID
having count(distinct TransmissionControlTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.BodyTypeID = B.BodyType
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and len(TransmissionControlType) > 0 and invalid is NULL
and (PAStockID between @x and @y)
;

--step 6
--update on basevehicle, EngineBaseID
--Aspiration
Update B
set B.Aspiration = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname,  EngineBaseID, count(distinct [AspirationID]) as AspirationID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, EngineBaseID
having count(distinct AspirationID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and Aspiration is not null and invalid is NULL
and (PAStockID between @x and @y)
;


--DriveType
Update B
set B.DriveType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, EngineBaseID, 
count(distinct [DriveTypeID]) as DriveTypeID from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, EngineBaseID
having count(distinct DriveTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  
and A.EngineBaseID = B.EngineBase and isnull(B.region, 1) = 1 and DriveType is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--EngineDesignation
Update B
set B.EngineDesignation = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, EngineBaseID, 
count(distinct [EngineDesignationid]) as [EngineDesignationid]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, EngineBaseID
having count(distinct EngineDesignationid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and EngineDesignation is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--FuelDeliveryType
Update B
set B.FuelDeliveryType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, EngineBaseID,
count(distinct [FuelDeliveryTypeID]) as FuelDeliveryTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, EngineBaseID
having count(distinct FuelDeliveryTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and FuelDeliveryType is not null and invalid is NULL
and (PAStockID between @x and @y);

--FuelType
Update B
set B.FuelType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, EngineBaseID, 
count(distinct [FuelTypeID]) as FuelType
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, EngineBaseID
having count(distinct FuelTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and B.FuelType is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--CylinderHeadType
Update B
set B.CylinderHeadType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, EngineBaseID, 
count(distinct [CylinderHeadTypeID]) as [CylinderHeadTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, EngineBaseID
having count(distinct CylinderHeadTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and CylinderHeadType is not null and invalid is NULL
and (PAStockID between @x and @y)
;


--Transmission ControlType
Update B
set B.TransmissionControlType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, EngineBaseID, 
count(distinct [TransmissionControlTypeID]) as [TransmissionControlTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, BodyTypeID, EngineBaseID
having count(distinct TransmissionControlTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and  A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and TransmissionControlType is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--step 7
--update on basevehicle, submodel, EngineBaseID
--Aspiration
Update B
set B.Aspiration = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid, EngineBaseID, count(distinct [AspirationID]) as AspirationID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid, EngineBaseID
having count(distinct AspirationID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel 
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and Aspiration is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--DriveType
Update B
set B.DriveType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid,  EngineBaseID, 
count(distinct [DriveTypeID]) as DriveTypeID from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid,  EngineBaseID
having count(distinct DriveTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and DriveType is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--EngineDesignation
Update B
set B.EngineDesignation = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid, EngineBaseID, 
count(distinct [EngineDesignationid]) as EngineDesignationid
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid,  EngineBaseID
having count(distinct EngineDesignationid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel
and A.EngineBaseID = B.EngineBase and isnull(B.region, 1) = 1 and EngineDesignation is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--FuelDeliveryType
Update B
set B.FuelDeliveryType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid, EngineBaseID,
count(distinct [FuelDeliveryTypeID]) as FuelDeliveryTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid,  EngineBaseID
having count(distinct FuelDeliveryTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and FuelDeliveryType is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--FuelType
Update B
set B.FuelType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid, EngineBaseID, 
count(distinct [FuelTypeID]) as FuelType
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid, EngineBaseID
having count(distinct FuelTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and B.FuelType is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--CylinderHeadType
Update B
set B.CylinderHeadType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid, EngineBaseID, 
count(distinct [CylinderHeadTypeID]) as [CylinderHeadTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid, EngineBaseID
having count(distinct CylinderHeadTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and CylinderHeadType is not null and invalid is NULL
and (PAStockID between @x and @y)
;


--Transmission ControlType
Update B
set B.TransmissionControlType = null,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Submodelid,  EngineBaseID, 
count(distinct [TransmissionControlTypeID]) as [TransmissionControlTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Submodelid, EngineBaseID
having count(distinct TransmissionControlTypeID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.SubModelID = B.submodel
and A.EngineBaseID = B.EngineBase  and isnull(B.region, 1) = 1 and TransmissionControlType is not null and invalid is NULL
and (PAStockID between @x and @y)
;
END
GO
/****** Object:  StoredProcedure [dbo].[usp_StandardAcesPass8]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		GM
-- Create date: 8/29/2018
--step 8
--update on basevehicle, BodyTypeID, >
-- =============================================
CREATE PROCEDURE [dbo].[usp_StandardAcesPass8]
	-- Add the parameters for the stored procedure here
	@x bigint,
	@y bigint
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

Declare @DT datetime2(7)
set @DT = GetDate()

--EngineBase
Update B
set B.EngineBase = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname,  BodyTypeID, count(distinct EngineBaseID) as EngineBaseID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Bodytypeid
having count(distinct EngineBaseID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle   and A.BodyTypeID = B.BodyType and isnull(B.region, 1) = 1 and EngineBase is not null and invalid is NULL
and (PAStockID between @x and @y)
;



--Aspiration
Update B
set B.Aspiration = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Bodytypeid, count(distinct [AspirationID]) as AspirationID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, Bodytypeid
having count(distinct AspirationID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle   and A.BodyTypeID = B.BodyType and isnull(B.region, 1) = 1 and Aspiration is not null and invalid is NULL
and (PAStockID between @x and @y)
;


--EngineDesignationID
Update B
set B.EngineDesignation = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, Bodytypeid, count(distinct [EngineDesignationID]) as EngineDesignationID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname,Bodytypeid
having count(distinct EngineDesignationID) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle   and A.BodyTypeID = b.bodytype  and isnull(B.region, 1) = 1 and EngineDesignation is not null and invalid is NULL
and (PAStockID between @x and @y)
;


--mfrbodycodeid
Update B
set B.mfrbodycode = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, BodyTypeID  , 
count(distinct [mfrbodycodeid]) as mfrbodycodeid
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, BodyTypeID 
having count(distinct mfrbodycodeid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle   and A.BodyTypeID = b.bodytype  and isnull(B.region, 1) = 1 and MfrBodyCode is not null and invalid is NULL
and (PAStockID between @x and @y)
;




--DriveTypeID
Update B
set B.DriveType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname,BodyTypeID, count(distinct [DriveTypeID]) as DriveTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, BodytypeID
having count(distinct drivetypeid) = 1) A,dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle   and A.BodyTypeID = b.bodytype  and isnull(B.region, 1) = 1 and DriveType is not null and invalid is NULL
and (PAStockID between @x and @y)
;



--FuelDeliveryTypeID
Update B
set B.FuelDeliveryType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, BodytypeID  ,
count(distinct [FuelDeliveryTypeID]) as FuelDeliveryTypeID
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, BodytypeID
having count(distinct FuelDeliveryTypeid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.BodyTypeID = b.bodytype  and isnull(B.region, 1) = 1 and FuelDeliveryType is not null and invalid is NULL
and (PAStockID between @x and @y)
;


--FuelTypeID
Update B
set B.FuelType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, BodytypeID,  count(distinct [FuelTypeID]) as FuelType
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname,BodytypeID
having count(distinct FuelTypeid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and  A.BodyTypeID = b.bodytype and isnull(B.region, 1) = 1 and B.FuelType is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--CylinderHeadTypeID
Update B
set B.CylinderHeadType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, BodytypeID, 
count(distinct [CylinderHeadTypeID]) as [CylinderHeadTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname, BodytypeID
having count(distinct CylinderHeadTypeid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.BodyTypeID = b.bodytype and isnull(B.region, 1) = 1 and CylinderHeadType is not null and invalid is NULL
and (PAStockID between @x and @y)
;

--TransmissionControlType
Update B
set B.TransmissionControlType = NULL,
ModifiedDT = @DT
from 
(select BaseVehicleID, yearid,makename, modelname, BodytypeID  , 
count(distinct [TransmissionControlTypeID]) as [TransmissionControlTypeID]
from dbo.StandardVCDB
group by BaseVehicleID, yearid,makename, modelname,BodytypeID
having count(distinct TransmissionControlTypeid) = 1) A, dbo.PAStkFeedProduction B  
where A.basevehicleid = B.basevehicle  and A.BodyTypeID = b.bodytype  and isnull(B.region, 1) = 1 and TransmissionControlType is not null and invalid is NULL
and (PAStockID between @x and @y)
;
END
GO
/****** Object:  StoredProcedure [dbo].[usp_StandardizedVCDB]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		GM
-- Create date: 10/5/2018
-- Description:	standarized ACES by make
-- =============================================
CREATE PROCEDURE [dbo].[usp_StandardizedVCDB]
@make varchar(50)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	Declare @LoadDT datetime2(7)
	set @LoadDT = GetDate()


    INSERT INTO [dbo].[StandardVCDB]
           ([basevehicleid]
           ,[yearid]
           ,[makename]
           ,[modelname]
           ,[submodelid]
           ,[BodyTypeID]
           ,[EngineBaseID]
           ,[AspirationID]
           ,[EngineDesignationID]
           ,[mfrbodycodeid]
           ,[DriveTypeID]
           ,[FuelTypeID]
           ,[CylinderHeadTypeID]
           ,[FuelDeliveryTypeID]
           ,[TransmissionControlTypeID]
           ,[LoadDT])
	select distinct a.basevehicleid,b.yearid,c.makename,d.modelname, e.submodelid, g.BodyTypeID, j.EngineBaseID, AspirationID, EngineDesignationID, mfrbodycodeid,
	DriveTypeID, FuelTypeID, CylinderHeadTypeID, FuelDeliveryTypeID,  TransmissionControlTypeID, @LoadDT
	from  [VCDB].[DBO].[Vehicle] a
	inner join [VCDB].[DBO].[Region] reg on reg.RegionID=a.RegionID
    inner join vcdb.dbo.BaseVehicle b on a.basevehicleid=b.BaseVehicleID
    inner join vcdb.dbo.make c on b.makeid=c.MakeID
    inner join vcdb.dbo.model d on b.ModelID=d.ModelID
    inner join vcdb.dbo.SubModel e on a.submodelid=e.SubModelID
	inner join vcdb.dbo.VehicleToBodyStyleConfig f on a.VehicleID=f.VehicleID 
	inner join vcdb.dbo.BodyStyleConfig g on f.BodyStyleConfigID=g.BodyStyleConfigID
	inner join vcdb.dbo.SubModel h on a.SubmodelID=h.SubModelID
    inner join vcdb.dbo.VehicleToEngineConfig i on a.VehicleID = i.VehicleID
    inner join vcdb.dbo.EngineConfig j on j.EngineConfigID=i.EngineConfigID
    inner join vcdb.dbo.VehicleToBodyStyleConfig k on a.VehicleID=k.VehicleID
    inner join vcdb.dbo.bodystyleconfig l on k.BodyStyleConfigID=l.BodyStyleConfigID
    inner join vcdb.dbo.bodytype m on l.BodyTypeID=m.BodyTypeID
    inner join vcdb.dbo.vehicletomfrbodycode n on a.vehicleid=n.vehicleid
    inner join vcdb.dbo.VehicleToDriveType o on a.VehicleID=o.VehicleID
    inner join vcdb.dbo.VehicleToTransmission p on a.VehicleID=p.VehicleID
    inner join vcdb.dbo.Transmission q on p.TransmissionID=q.TransmissionID
    inner join vcdb.dbo.transmissionbase r on q.transmissionbaseid=r.transmissionbaseid
    inner join vcdb.[dbo].[FuelDeliveryConfig] s on j.FuelDeliveryConfigID = s.[FuelDeliveryConfigID]
    inner join vcdb.dbo.VehicleToBedConfig t on a.VehicleID = t.VehicleID
    inner join vcdb.[dbo].[BedConfig] u on t.BedConfigID = u.[BedConfigID]
    INNER JOIN VCDB.dbo.EngineBase X on j.EngineBaseID = x.EngineBaseID  
    where  reg.regionid=1 and c.MakeName = @make

END
GO
/****** Object:  StoredProcedure [dbo].[usp_updateACESNOTES]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		GM
-- Create date: 7/12/2018
-- Description:	update ACES NOTES by linecode
-- =============================================
Create PROCEDURE [dbo].[usp_updateACESNOTES]
	-- Add the parameters for the stored procedure here
	@lc nvarchar(2)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	  --step 1: update aces notes where len is less than 15 and PartTerminologyName is similar to Notes
  Update [dbo].[ACESNotes] 
  set notes = NULL 
  from [dbo].[ACESNotes]  A, PCDB.[dbo].[Parts] B
  where A.PartTerminologyID = B.[PartTerminologyID]
  and  len(notes) < 15 and soundex(PartTerminologyName) = soundex(Notes) and a.sfran = @lc;


  --step 2: Delete notes that = Vendor:
  Delete from [dbo].[ACESNotes] 
  where notes = 'Vendor:' and sfran = @lc;


  
  --step 3: update NoteKey based on sport package or sport brake
  --NoteKey Filter value 1 with Sports Brakes or Package

  Update  [dbo].[ACESNotes]
  set NoteKey = 1
  where contains(Notes, 'sport or sports or sprt') and contains(Notes, 'brk or brakes or brake or pkg or package or pkg.') 
  and (not contains(Notes, 'w/o or Exc or Without or Except or Excluding or Not or Non')) and sfran = @lc  and PartTerminologyID not in (1896, 1684);

 
  --step 4: update NoteKey based on sport package or sport brake
  --NoteKey Filter value 2 with out Sports Brakes or Package

  Update  [dbo].[ACESNotes]
  set NoteKey = 2
  where contains(Notes, 'sport or sports or sprt') and contains(Notes, 'brk or brakes or brake or pkg or package or pkg.') 
  and (contains(Notes, 'w/o or Exc or Without or Except or Excluding or Not or Non')) and sfran = @lc  and PartTerminologyID not in (1896, 1684);

    Update  [dbo].[ACESNotes]
  set NoteKey = 2 
  where contains(Notes, '"Base Package"') and PartTerminologyID in (1896, 1684) and sfran = @lc;

  Update  [dbo].[ACESNotes]
  set NoteKey = 2 
  where contains(Notes, '"Base Brks"') and PartTerminologyID in (1896, 1684) and sfran = @lc;

  Update  [dbo].[ACESNotes]
  set NoteKey = 2 
  where contains(Notes, 'Base') and not contains(Notes, 'Sport') and PartTerminologyID in (1896, 1684) and sfran = @lc;

  Update  [dbo].[ACESNotes]
  set NoteKey = 2 
  where contains(Notes, '"Base pkg"') and PartTerminologyID in (1896, 1684) and sfran = @lc;

  End
GO
/****** Object:  StoredProcedure [dbo].[usp_UpdateACESValues]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		GM
-- Create date: 9/21/2018
-- Description:	Update position based of positioncontrol table
-- set positionid = '' when positionid is 0, 1, or NULL
--set poistionid to parentposition id of control table
--set childpositionid = orignal position id
--join on parterm and child positionid from control table
--Mod date: 10/11/2018
--include the update of imc parts.
--Cycle by pastockid
-- =============================================
CREATE PROCEDURE [dbo].[usp_UpdateACESValues]

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

Declare @i bigint,
@x bigint,
@y bigint

set @y = 100000
Set @i = (Select count(*) from  PAStkFeedProduction with (nolock))
set @x =1
while @x < @i
Begin

	--partterm supersession
	update st 
	set PartTerminologyID=ps.NewPartTerminologyID
	,OrigPartTermID=ps.NewPartTerminologyID
	from [dbo].[PAStkFeedProduction]  st
	join [PCDB]..[PartsSupersession] ps on st.PartTerminologyID=ps.OldPartTerminologyID
	where pastockid between @x and @y; 
	
	--partterm consolidation
	update a set a.PartTerminologyID = b.ParentPartTermID
	FROM [dbo].[PAStkFeedProduction]  a
	join [dbo].[PartTermControl]  b on a.PartTerminologyID=b.ChildPartTermID
	where pastockid between @x and @y;

	--set positionid = NULL when positionid is 0, 1
	update	PAStkFeedProduction
	set positionID = NULL
	where positionid in (0, 1) and pastockid between @x and @y;

	
	--preform update to position and child positionid	
	Update A
	set A.PositionID = b.ParentPositionID,
	A.origPositionID = A.PositionID
	from PAStkFeedProduction a, 
	[dbo].[PositionControl] b
	where a.PositionID = b.childpositionid and pastockid between @x and @y and A.PartTerminologyID = b.PartTermID;

	--Update imc parts
	Update A
	set udfSpart = b.ToThisPart,
	a.sku = b.sku,
	ModifiedDT = GetDate()
	from  [dbo].[PAStkFeedProduction] A, [dbo].[IMCPartsUpdate] B
	where A.udfSPART = b.UpdateThisPartInStockfeed and a.sfran = b.PALC
	and pastockid between @x and @y;

		--BaseVehicle
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a left outer join
	vcdb.dbo.BaseVehicle b on a.Basevehicle = b.BaseVehicleID 
	where pastockid between @i and @y and invalid is NULL and isNULL(a.Basevehicle,'') <> '' and b.BaseVehicleID is null

	--Partterm
	Update A
	set invalid = 'True'
	from PAStkFeedProduction a left outer join
	pcdb.dbo.parts b on a.PartTerminologyID = b.[PartTerminologyID]
	where pastockid between @i and @y and invalid is NULL and isNULL(a.PartTerminologyID,'') <> '' and b.PartTerminologyID is null



	set @x = @x + 100000
	set @y = @y + 100000



End  


END
GO
/****** Object:  StoredProcedure [dbo].[usp_usp_StandardAcesPassMaster]    Script Date: 10/24/2018 2:59:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[usp_usp_StandardAcesPassMaster]

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

Declare @i bigint,
@x bigint,
@y bigint

set @y = 100000
Set @i = (Select count(*) from  PAStkFeedProduction with (nolock))
set @x =1
while @x < @i
Begin

	exec [dbo].[usp_StandardAcesPass1] @x, @y
	exec [dbo].[usp_StandardAcesPass2] @x, @y	
	exec [dbo].[usp_StandardAcesPass3] @x, @y
	exec [dbo].[usp_StandardAcesPass4] @x, @y
	exec [dbo].[usp_StandardAcesPass5_7] @x, @y
	exec [dbo].[usp_StandardAcesPass8] @x, @y

	set @x = @x + 100000
	set @y = @y + 100000
End  
END
GO

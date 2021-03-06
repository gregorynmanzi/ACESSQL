USE [ACES_Processing]
GO
/****** Object:  StoredProcedure [dbo].[usp_NetChanges]    Script Date: 10/16/2020 4:16:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		GM
-- Create date: 9/12/2020
-- Description:	Procedure that run entire net change process for stockfeed
-- =============================================
ALTER PROCEDURE [dbo].[usp_NetChanges]

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

  --Clear stockid table to store stockid
truncate table stg.Stockid;

--local variable
Declare @rc int
Declare @DT datetime2(7)

--trap datetime
set @DT = GetDate()

--step 1
--Delete records flagged as invalid 
Delete [stg].[PAStockFeed]
where Invalid  = 'True'

--step 2
--invalidate records that have corresponding matching record where DoNotChange = 'True' in stockfeed
--set QualID = 2
Update A
set a.QualID = '2'
from [stg].[PAStockFeed] A inner join ACES.[mst].[PAStockFeed_final] b
on A.SKU = B.SKU AND A.BaseVehicle = B.BaseVehicle 
and a.PartTerminologyid = b.PartTerminologyid 
where b.DoNotChange = 'True';

--step 3
---invalidate corresponding records in pastockfeednotes
--set invalid = 'True'
Update B
set b.[Invalid] = 'True'
from [stg].[PAStockFeed] A inner join [stg].[PAStockFeedNote] B on A.App_ID = b.[AppRecordID]
where a.QualID = '2'


--step 4
--trap pastockid for matching records
insert into stg.Stockid(pastockid)
Select distinct b.pastockid from [stg].[PAStockFeed]  A
inner join ACES.[mst].[PAStockFeed_final] b
on  a.sku = b.sku and a.fitmentvalues = b.fitmentvalues 
where  b.DoNotChange is NULL and b.invalid is null and QualID is null

--step 5
--Update MFRLABEL, Fileid(Last file app existed), timestamp
Update B
set b.fileID = A.controlid,
b.MfrLabel = a.MfrLabel,
UpdateDT = @DT
from (Select sku, fitmentvalues, Controlid, notes, QualText, MFRLabel from [stg].[PAStockFeed])  A inner join ACES.[mst].[PAStockFeed_final] b
on a.sku = b.sku and a.fitmentvalues = b.fitmentvalues 
where b.Invalid is null and b.DoNotChange is NULL
;

--step 6
--update staging table to indicate match
Update A  
set A.QualID = '1'
from [stg].[PAStockFeed]  A inner join ACES.[mst].[PAStockFeed_final] b
on a.sku = b.sku and a.fitmentvalues = b.fitmentvalues 
where b.DoNotChange is NULL and b.invalid is null;



--Delete existing stockids
Delete A 
from ACES.mst.PAStockfeed_Notes a  inner join stg.Stockid b
on a.PAStockID = b.pastockid

--insert pastockid/masternoteid cominations
Insert into ACES.mst.PAStockfeed_Notes([PAStockID],[NoteMasterID], invalid, LoadDT, UpdateDT)
select distinct c.pastockid, b.NoteMasterID, 0, @DT, @DT
from [stg].[PAStockFeed] A
inner join [stg].[PAStockFeedNote] b on A.App_ID = b.[AppRecordID]
inner join ACES.[mst].[PAStockFeed_final] c on a.sku = c.sku and a.fitmentvalues = c.fitmentvalues 
inner join stg.Stockid d on c.pastockid = d.pastockid
where A.QualID = '1' 


/*
--step 7
--update existing masternoteid/pastockid combo
Update d
set UpdateDT = @DT,
Invalid = 0
from [stg].[PAStockFeed] A
inner join [stg].[PAStockFeedNote] b on A.App_ID = b.[AppRecordID]
inner join ACES.[mst].[PAStockFeed_final] c  on a.sku = c.sku and a.fitmentvalues = c.fitmentvalues 
inner join ACES.mst.PAStockfeed_Notes d on c.pastockid = d.PAStockID and b.NoteMasterID = d.NoteMasterID
Where A.QualID = '1' and D.invalid = 1


--step 8
--insert pastockid/masternoteid not existent in PAStockfeed_Notes
Insert into ACES.mst.PAStockfeed_Notes([PAStockID],[NoteMasterID], invalid, LoadDT, UpdateDT)
select distinct c.pastockid, b.NoteMasterID, 0, @DT, @DT
from [stg].[PAStockFeed] A
inner join [stg].[PAStockFeedNote] b on A.App_ID = b.[AppRecordID]
inner join ACES.[mst].[PAStockFeed_final] c on a.sku = c.sku and a.fitmentvalues = c.fitmentvalues 
left outer join ACES.mst.PAStockfeed_Notes d on c.pastockid = d.PAStockID and b.NoteMasterID = d.NoteMasterID
where d.pastockid is null and A.QualID = '1';

--step 9
--remove notes pastockid/masternoteid not in staging tables
Update A
set A.Invalid = 1,
A.UpdateDT = @DT
from  ACES.mst.PAStockfeed_Notes A  inner join tempdb..#stockid b
on a.PAStockID = b.pastockid where (UpdateDT < @DT) 
*/

--loop through record id for new records
DECLARE db_cursor CURSOR FAST_FORWARD FOR
select  A.RecordID  from [stg].[PAStockFeed] A inner join [dbo].[vwPAStockRecMax] B on A.RecordID = B.[RecordID]
where qualID is null
order by 1
	
OPEN db_cursor
FETCH NEXT FROM db_cursor INTO @rc
WHILE @@FETCH_STATUS = 0
Begin

--step 10
--invalidate existing records whose fitment values have changed
Update A
set A.invalid = 'True',
UpdateDT = @DT,
InvalidReason = 99
FROM ACES.[mst].[PAStockFeed_final] a, [stg].[PAStockFeed] b
WHERE A.SKU = B.SKU AND A.BaseVehicle = B.BaseVehicle 
and a.PartTerminologyid = b.PartTerminologyid 
and 1=case when  a.cylinders is null or b.cylinders is null or a.cylinders = b.cylinders then 1 end
and 1=case when  a.liter is null or b.liter is null or a.liter = b.liter then 1 end
and 1=case when  a.blocktype is null or b.blocktype is null or a.blocktype = b.Blocktype then 1 end
 and A.fileID <> b.controlID and a.DoNotChange is NULL and a.Invalid is NULL and  b.RecordID = @RC;

 --step 11
 --insert new records
 INSERT INTO ACES.[mst].[PAStockFeed_final]
           ([ControlID]
           ,[sfran]
           ,[spart]
           ,[sku]
           ,[Aspiration]
           ,[BaseVehicle]
           ,[BedLength]
           ,[BedType]
           ,[BodyNumDoors]
           ,[BodyType]
           ,[BrakeABS]
           ,[BrakeSystem]
           ,[CylinderHeadType]
           ,[DriveType]
           ,[DisplayOrder]
           ,[EngineDesignation]
           ,[EngineMfr]
           ,[EngineVIN]
           ,[EngineVersion]
           ,[EngineBlock]
           ,[EngineBoreStroke]
           ,[EquipmentBase]
           ,[EquipmentModel]
           ,[FrontBrakeType]
           ,[FrontSpringType]
           ,[FuelDeliverySubType]
           ,[FuelSystemControlType]
           ,[FuelSystemDesign]
           ,[FuelDeliveryType]
           ,[FuelType]
           ,[IgnitionSystemType]
           ,[make]
           ,[mfr]
           ,[MfrBodyCode]
           ,[MfrLabel]
           ,[model]
           ,[Note1]
           ,[Note2]
           ,[PartTerminologyid]           
           ,[PositionID]
           ,[poweroutput]           
           ,[RearBrakeType]
           ,[RearSpringType]
           ,[Region]
           ,[SteeringSystem]
           ,[SteeringType]
           ,[Submodel]
           ,[TransferDate]
           ,[TransmissionControlType]
           ,[TransmissionMfr]
           ,[TransmissionNumSpeeds]
           ,[TransmissionType]
           ,[TransElecControlled]
           ,[TransmissionBase]
           ,[TransmissionMfrCode]
           ,[ValvesPerEngine]
           ,[VehicleType]
           ,[WheelBase]
           ,[PerCarQty]
           ,[Years]
           ,[cylinders]
           ,[blocktype]
           ,[liter]
           ,[Invalid]
           ,[udfspart]
           ,[LoadDT]
           ,[UpdateDT]
		   ,origsku
		   ,fileid
		   ,donotchange
		   ,fitmentvalues)
     Select distinct [ControlID]
           ,linecode
           ,PartNumber
           ,[sku]
           ,[Aspiration]
           ,[BaseVehicle]
           ,[BedLength]
           ,[BedType]
           ,[BodyNumDoors]
           ,[BodyType]
           ,[BrakeABS]
           ,[BrakeSystem]
           ,[CylinderHeadType]
           ,[DriveType]
           ,[DisplayOrder]
           ,[EngineDesignation]
           ,[EngineMfr]
           ,[EngineVIN]
           ,[EngineVersion]
           ,[EngineBlock]
           ,[EngineBoreStroke]
           ,[EquipmentBase]
           ,[EquipmentModel]
           ,[FrontBrakeType]
           ,[FrontSpringType]
           ,[FuelDeliverySubType]
           ,[FuelSystemControlType]
           ,[FuelSystemDesign]
           ,[FuelDeliveryType]
           ,[FuelType]
           ,[IgnitionSystemType]
           ,[make]
           ,[mfr]
           ,[MfrBodyCode]
           ,[MfrLabel]
           ,[model]
           ,[Notes]
           ,[QualText]
           ,[PartTerminologyid]           
           ,[Position]
           ,[poweroutput]           
           ,[RearBrakeType]
           ,[RearSpringType]
           ,[Region]
           ,[SteeringSystem]
           ,[SteeringType]
           ,[Submodel]
           ,[TransferDate]
           ,[TransmissionControlType]
           ,[TransmissionMfr]
           ,[TransmissionNumSpeeds]
           ,[TransmissionType]
           ,[TransElecControlled]
           ,[TransmissionBase]
           ,[TransmissionMfrCode]
           ,[ValvesPerEngine]
           ,[VehicleType]
           ,[WheelBase]
           ,[Qty]
           ,[Years]
           ,[cylinders]
           ,[blocktype]
           ,[liter]
           ,[Invalid]
           ,[udfspart]
           ,@DT
           ,@DT as UpdateDT, b.OrigPartNumber,  controlid as fileid, donotchange, fitmentvalues from [stg].[PAStockFeed] b where b.RecordID = @rc;

--step 12
--Add corresponding notes
Insert into ACES.[mst].[PAStockfeed_Notes]([PAStockID], [NoteMasterID], [LoadDT], UpdateDT, Invalid)
Select distinct @@IDENTITY as Pastockid,  b.[NoteMasterID], @DT, @DT, 0 from [stg].[PAStockFeed] A inner join [stg].[PAStockFeedNote] b on a.App_ID = b.[AppRecordID]
where a.RecordID = @rc

 

	FETCH NEXT FROM db_cursor INTO @rc

END



CLOSE db_cursor
DEALLOCATE db_cursor 

END

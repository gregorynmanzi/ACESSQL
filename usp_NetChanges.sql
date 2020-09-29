USE [ACES_Processing]
GO

/****** Object:  StoredProcedure [dbo].[usp_NetChanges_old]    Script Date: 9/29/2020 3:24:22 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		GM
-- Create date: 3/12/2019
-- Description:	Net Change Process
-- =============================================
CREATE PROCEDURE [dbo].[usp_NetChanges_old]
@ID as bigint
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

--set date variable
Declare @DT datetime2(7)
set @DT = GetDate()

Begin Try

--Delete invalid records
Delete stg.PAStockFeed where invalid = 'True';

--invalidate records in staging table that have corresponding matching record where DoNotChange = 'True'
Update A
set a.QualID = 1
from [stg].[PAStockFeed] A inner join ACES.[mst].[PAStockFeed_final] b
on A.SKU = B.SKU AND A.BaseVehicle = B.BaseVehicle 
and a.PartTerminologyid = b.PartTerminologyid 
where b.DoNotChange = 'True';


--Compare all fields in stg.PAStockFEED to mst.PAStockFeed_final
--for all matches controlid in mst.PAStockFeed_final fileid gets updated to match controlid in stg.PAStockFEED 
Update B
set b.fileID = A.controlid
from (Select * from [stg].[PAStockFeed] where QualID is NULL) A
inner join ACES.[mst].[PAStockFeed_final] b
on  a.sku = b.sku and a.fitmentvalues= b.fitmentvalues
where  b.DoNotChange is NULL and b.invalid is null  and A.ControlID <> b.ControlID;

--for matches in sku and fitment values, but not in notes or mfrlabel
--preform an update on Notes, MFRLabel and fileid
Update B
set b.fileID = A.controlid,
b.Note1 = a.Notes,
b.Note2 = a.QualText,
b.MfrLabel = a.MfrLabel,
UpdateDT = @DT
from (Select * from [stg].[PAStockFeed] where QualID is NULL) A
inner join ACES.[mst].[PAStockFeed_final] b
on  a.sku = b.sku and a.fitmentvalues = b.fitmentvalues
and (
ISnull(a.MfrLabel, '-1') <> ISnull(b.MfrLabel,'-1')
or ISnull(a.Notes, '-1') <> ISnull(b.Note1,'-1') 
or ISnull(a.QualText, '-1') <> ISnull(b.Note2,'-1'))
where  b.DoNotChange is NULL and b.invalid is null and A.ControlID <> b.ControlID;
;

--for all matches with sku and fitment values QualID get flag to 1 in stg.PAStockfeed
Update A
set a.QualID = 1
from (Select * from [stg].[PAStockFeed] where QualID is NULL) A inner join ACES.[mst].[PAStockFeed_final] b
on  a.sku = b.sku  and a.fitmentvalues = b.fitmentvalues
and A.ControlID = b.fileid
where b.invalid is null;


--Compare matching sku, basevehicle, parterm and enginebase with unmatched controlid
--from stg.PAStockFEED to mst.PAStockFeed_final
--flag records a invalid in [mst].[PAStockFeed_final]
Update A
set A.invalid = 'True',
UpdateDT = @DT,
InvalidReason = 99
FROM ACES.[mst].[PAStockFeed_final] a, [stg].[PAStockFeed] b
WHERE A.SKU = B.SKU AND A.BaseVehicle = B.BaseVehicle 
and a.PartTerminologyid = b.PartTerminologyid and ISnull(a.cylinders, '-1') = ISnull(b.cylinders,'-1') and ISnull(a.liter, '-1') = ISnull(b.liter,'-1') 
and ISnull(a.blocktype, '-1') = ISnull(b.blocktype,'-1') and A.fileID <> b.controlID and a.DoNotChange is NULL and a.Invalid is NULL and  b.QualID is NULL;

--log records
Update aces.mst.LogTable
set [ExpiredCount] = @@ROWCOUNT
where XMLFileDetId = @ID;

--Insert unmatched records into mst.PAStockFeed_final
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
           ,@DT as UpdateDT, b.OrigPartNumber,  controlid as fileid, donotchange, fitmentvalues from [stg].[PAStockFeed] b where QualID is null;

		   --log records
			Update aces.mst.LogTable
			set [NewRecords] = @@ROWCOUNT
			where XMLFileDetId = @ID;

End Try
Begin Catch

SELECT  
        ERROR_NUMBER() AS ErrorNumber  
        ,ERROR_SEVERITY() AS ErrorSeverity  
        ,ERROR_STATE() AS ErrorState  
        ,ERROR_PROCEDURE() AS ErrorProcedure  
        ,ERROR_MESSAGE() AS ErrorMessage;




End Catch


END
GO



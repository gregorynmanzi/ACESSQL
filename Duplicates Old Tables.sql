USE [ACES_Processing]
GO

/****** Object:  Table [dbo].[PAStockFeedAnalysis]    Script Date: 9/29/2020 3:23:48 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PAStockFeedAnalysis](
	[pastockid] [bigint] NOT NULL,
	[ControlID] [bigint] NULL,
	[sfran] [char](2) NOT NULL,
	[spart] [varchar](100) NOT NULL,
	[sku] [varchar](100) NOT NULL,
	[Aspiration] [int] NULL,
	[BaseVehicle] [int] NULL,
	[BedLength] [int] NULL,
	[BedType] [int] NULL,
	[BodyNumDoors] [int] NULL,
	[BodyType] [int] NULL,
	[BrakeABS] [int] NULL,
	[BrakeSystem] [int] NULL,
	[CylinderHeadType] [int] NULL,
	[DriveType] [int] NULL,
	[DisplayOrder] [int] NULL,
	[EngineBase] [int] NULL,
	[OrigEngineBase] [int] NULL,
	[EngineDesignation] [int] NULL,
	[EngineMfr] [int] NULL,
	[EngineVIN] [int] NULL,
	[EngineVersion] [int] NULL,
	[EngineBlock] [int] NULL,
	[EngineBoreStroke] [int] NULL,
	[EquipmentBase] [int] NULL,
	[EquipmentModel] [int] NULL,
	[FrontBrakeType] [int] NULL,
	[FrontSpringType] [int] NULL,
	[FuelDeliverySubType] [int] NULL,
	[FuelSystemControlType] [int] NULL,
	[FuelSystemDesign] [int] NULL,
	[FuelDeliveryType] [int] NULL,
	[FuelType] [int] NULL,
	[IgnitionSystemType] [int] NULL,
	[make] [int] NULL,
	[mfr] [int] NULL,
	[MfrBodyCode] [int] NULL,
	[MfrLabel] [varchar](500) NULL,
	[model] [int] NULL,
	[Note1] [varchar](max) NULL,
	[Note2] [varchar](max) NULL,
	[Note3] [varchar](1) NULL,
	[Note4] [varchar](1) NULL,
	[Note5] [varchar](1) NULL,
	[Note6] [varchar](1) NULL,
	[Note7] [varchar](1) NULL,
	[Note8] [varchar](1) NULL,
	[Note9] [varchar](1) NULL,
	[Note10] [varchar](1) NULL,
	[Note11] [varchar](1) NULL,
	[Note12] [varchar](1) NULL,
	[Note13] [varchar](1) NULL,
	[Note14] [varchar](1) NULL,
	[Note15] [varchar](1) NULL,
	[Note16] [varchar](1) NULL,
	[PartTerminologyid] [int] NULL,
	[origPartTermID] [int] NULL,
	[PositionID] [int] NULL,
	[poweroutput] [int] NULL,
	[origPositionID] [int] NULL,
	[FitNote] [varchar](1) NULL,
	[RearBrakeType] [int] NULL,
	[RearSpringType] [int] NULL,
	[Region] [int] NULL,
	[SteeringSystem] [int] NULL,
	[SteeringType] [int] NULL,
	[Submodel] [int] NULL,
	[TransferDate] [date] NULL,
	[TransmissionControlType] [int] NULL,
	[TransmissionMfr] [int] NULL,
	[TransmissionNumSpeeds] [int] NULL,
	[TransmissionType] [int] NULL,
	[TransElecControlled] [int] NULL,
	[TransmissionBase] [int] NULL,
	[TransmissionMfrCode] [int] NULL,
	[ValvesPerEngine] [int] NULL,
	[VehicleType] [int] NULL,
	[WheelBase] [int] NULL,
	[PerCarQty] [varchar](50) NULL,
	[Years] [int] NULL,
	[cylinders] [varchar](2) NULL,
	[blocktype] [varchar](2) NULL,
	[liter] [varchar](6) NULL,
	[Invalid] [bit] NULL,
	[udfspart] [varchar](100) NULL,
	[LoadDT] [datetime2](7) NULL,
	[UpdateDT] [datetime2](7) NULL,
 CONSTRAINT [PK_PAStockFeed_analysis] PRIMARY KEY CLUSTERED 
(
	[pastockid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [Secondary]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PaStockFeedDuplicates_old](
	[sku] [varchar](100) NOT NULL,
	[Aspiration] [int] NULL,
	[BaseVehicle] [int] NULL,
	[BedLength] [int] NULL,
	[BedType] [int] NULL,
	[BodyNumDoors] [int] NULL,
	[BodyType] [int] NULL,
	[BrakeABS] [int] NULL,
	[BrakeSystem] [int] NULL,
	[CylinderHeadType] [int] NULL,
	[DriveType] [int] NULL,
	[DisplayOrder] [int] NULL,
	[EngineDesignation] [int] NULL,
	[EngineMfr] [int] NULL,
	[EngineVIN] [int] NULL,
	[EngineVersion] [int] NULL,
	[EngineBlock] [int] NULL,
	[EngineBoreStroke] [int] NULL,
	[EquipmentBase] [int] NULL,
	[EquipmentModel] [int] NULL,
	[FrontBrakeType] [int] NULL,
	[FrontSpringType] [int] NULL,
	[FuelDeliverySubType] [int] NULL,
	[FuelSystemControlType] [int] NULL,
	[FuelSystemDesign] [int] NULL,
	[FuelDeliveryType] [int] NULL,
	[FuelType] [int] NULL,
	[IgnitionSystemType] [int] NULL,
	[make] [int] NULL,
	[mfr] [int] NULL,
	[MfrBodyCode] [int] NULL,
	[MfrLabel] [varchar](500) NULL,
	[model] [int] NULL,
	[Note1] [varchar](max) NULL,
	[Note2] [varchar](max) NULL,
	[PartTerminologyid] [int] NULL,
	[PositionID] [int] NULL,
	[poweroutput] [int] NULL,
	[RearBrakeType] [int] NULL,
	[RearSpringType] [int] NULL,
	[Region] [int] NULL,
	[SteeringSystem] [int] NULL,
	[SteeringType] [int] NULL,
	[Submodel] [int] NULL,
	[TransferDate] [date] NULL,
	[TransmissionControlType] [int] NULL,
	[TransmissionMfr] [int] NULL,
	[TransmissionNumSpeeds] [int] NULL,
	[TransmissionType] [int] NULL,
	[TransElecControlled] [int] NULL,
	[TransmissionBase] [int] NULL,
	[TransmissionMfrCode] [int] NULL,
	[ValvesPerEngine] [int] NULL,
	[VehicleType] [int] NULL,
	[WheelBase] [int] NULL,
	[PerCarQty] [varchar](50) NULL,
	[Years] [int] NULL,
	[cylinders] [varchar](2) NULL,
	[blocktype] [varchar](2) NULL,
	[liter] [varchar](6) NULL,
	[counter] [int] NULL
) ON [Secondary] TEXTIMAGE_ON [Secondary]
GO
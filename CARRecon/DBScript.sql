CREATE TABLE [dbo].[CARReconBatch](
	[ReconBatchID] [int] IDENTITY(1,1) NOT NULL,
	[ReconDate] [datetime] NOT NULL
) ON [PRIMARY]


CREATE TABLE [dbo].[CARReconResultDetail](
	[ReconBatchID] [int] NOT NULL,
	[ObjectName] [varchar](500) NOT NULL,
	[SystemID] [varchar](100) NOT NULL,
	[DBName] [varchar](200) NOT NULL,
	[Result] [int] NOT NULL
) ON [PRIMARY]



select CAR.ReconBatchID,CAR.ObjectName,CAR.SystemID,CAR.Result as CARResult,CDS.Result as CDSResult,CDSStage.Result as CDSStageResult
from CARReconResultDetail CAR 
	inner join CARReconResultDetail CDS on CAR.ReconBatchID = CDS.ReconBatchID
		and CAR.ObjectName = CDS.ObjectName
		and CAR.SystemID = CDS.SystemID
		and CAR.DBName = 'CAR' and CDS.DBName = 'CDS'
	inner join CARReconResultDetail CDSStage on CAR.ReconBatchID = CDSStage.ReconBatchID
		and CAR.ObjectName = CDSStage.ObjectName
		and CAR.SystemID = CDSStage.SystemID
		and CAR.DBName = 'CAR' and CDSStage.DBName = 'CDSStage'
where CAR.ReconBatchID = (select max(ReconBatchID) from CARReconBatch)
Order by 2
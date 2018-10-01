using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace CARRecon
{
    class CountObj
    {
        public string ObjectName { get; set; }
        public string Path { get; set; }
        public string SystemID { get; set; }
        public string dateCondition { get; set; }
    }


    class Program
    {
        const string CDSConnectionString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=master;Data Source=PRDPWSVEX0004\\SVEX04,54800;;Connection Timeout=600";
        const string CARConnectionString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=CAR_Sandbox;Data Source=PSQLCARREPORT,51662;Connection Timeout=600";
        const string PEConnectionString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=CAR_Sandbox;Data Source=PRDDSCOMM001;Connection Timeout=600";
        const string ResultDBConnectionString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=FanJiang;Data Source=DEVDSACSX001;Connection Timeout=600";

        const int retryTime = 2;
        static List<CountObj> failedList = new List<CountObj>();

        static string FindTableName(string objectName,string containername)
        {
            string tableName = "";
            using (SqlConnection objConn = new SqlConnection(PEConnectionString))
            {
                SqlCommand objComm = null;
                objConn.Open();
                using (objComm = new SqlCommand())
                {
                    objComm.Connection = objConn;
                    string sqlQuery = "";
                    switch(containername)
                    {
                        case "CAR":
                            sqlQuery = "select SUBSTRING(Path,LEN(Path)-charindex('.',reverse(Path))+2,LEN(Path)) from [dbo].[PE_ProcessList] where ProcessName = '"+objectName+"' and ProcessType in (102)"; 
                            break;
                        case "CDS":
                            sqlQuery = "select SUBSTRING(Path,LEN(Path)-charindex('.',reverse(Path))+2,LEN(Path)) from [dbo].[PE_ProcessList] where ProcessName = '" + objectName + "' and ProcessType in (100)";
                            break;
                        case "Stage":
                            sqlQuery = "select SUBSTRING(Path,LEN(Path)-charindex('.',reverse(Path))+2,LEN(Path)) from [dbo].[PE_ProcessList] where ProcessName = '" + objectName + "' and ProcessType in (100)";
                            break;
                    }
                    objComm.CommandText = sqlQuery;
                    SqlDataReader reader = objComm.ExecuteReader();
                    if(reader.Read())
                    {
                        tableName = reader.GetString(0);
                    }
                }
            }

                return tableName.Replace("CAR_","");
        }

        static void WriteResult(CountObj obj,int batchID,string DBName,int result)
        {
            using (SqlConnection sqlConn = new SqlConnection(ResultDBConnectionString))
            {
                sqlConn.Open();
                using (SqlCommand sqlComm = new SqlCommand())
                {
                    sqlComm.Connection = sqlConn;
                    string sqlResultUpdate = String.Format(@"IF Exists (select 1 from CARReconResultDetail where ReconBatchID = {0} and ObjectName = '{1}' and DBName = '{2}' )
                                delete CARReconResultDetail where  ReconBatchID = {0} and ObjectName = '{1}' and DBName = '{2}'
                        insert into CARReconResultDetail(ReconBatchID, ObjectName, SystemID,DBName, Result) values({0},'{1}','{2}','{3}'," + result.ToString()+")",
                        batchID.ToString(), obj.ObjectName,obj.SystemID, DBName);
                    sqlComm.CommandText = sqlResultUpdate;
                    sqlComm.ExecuteNonQuery();
                }
            }
        }

        static string GetDateCondition(CountObj obj)
        {
            return "XXX";
        }

        static void RecordCount(object obj, int batchID)
        {
            CountObj countObj = (CountObj)obj;

            string StageSQL = "";
            string CDSSQL = "";
            string CARSQL = "";

            int CDSstageCount = -1;
            int CDSCount = -1;
            int CARCount = -1;

            switch(countObj.SystemID)
            {
                case "CCS":
                    StageSQL = "select count(*) from [CCS_STAGE].[TMOBILE_CARE].[dbo].[" + FindTableName(countObj.ObjectName,"Stage") + "] Where 1=1";
                    CDSSQL = "select count(*) from [CCS].[dbo].[" + FindTableName(countObj.ObjectName, "CDS") + "] Where 1=1";
                    CARSQL = "select count(*) from [CAR_CCS].[CCS].["+ FindTableName(countObj.ObjectName, "CAR") + "] Where 1=1 and CAR_bitIsCurrent = 1";
                    break;
                case "ECS":
                    StageSQL = "select count(*) from [ECS_STAGE].[TMOBILE_EMPLOYEE_PROD].[dbo].[" + FindTableName(countObj.ObjectName, "Stage") + "] Where 1=1";
                    CDSSQL = "select count(*) from [ECS].[dbo].[" + FindTableName(countObj.ObjectName, "CDS") + "] Where 1=1";
                    CARSQL = "select count(*) from [CAR_ECS].[ECS].[" + FindTableName(countObj.ObjectName, "CAR") + "] Where 1=1 and CAR_bitIsCurrent = 1";
                    break;
                case "DCS":
                    StageSQL = "select count(*) from [DCS_STAGE].[TMOBILE_INDIRECT_PROD].[dbo].[" + FindTableName(countObj.ObjectName, "Stage") + "] Where 1=1";
                    CDSSQL = "select count(*) from [DCS].[dbo].[" + FindTableName(countObj.ObjectName, "CDS") + "] Where 1=1";
                    CARSQL = "select count(*) from [CAR_DCS].[DCS].[" + FindTableName(countObj.ObjectName, "CAR") + "] Where 1=1 and CAR_bitIsCurrent = 1";
                    break;
            }

#if DEBUG
            Console.WriteLine(CARSQL);
            Console.WriteLine(CDSSQL);
            Console.WriteLine(StageSQL);
#endif

            SqlConnection CDSConn = null;
            SqlConnection CARConn = null;
            SqlConnection resultConn = null;

            SqlCommand CDSComm = null;
            SqlCommand CARComm = null;
            SqlCommand ResultComm = null;

            try
            {
                CDSConn = new SqlConnection(CDSConnectionString);
                CARConn = new SqlConnection(CARConnectionString);
                resultConn = new SqlConnection(ResultDBConnectionString);
                CDSConn.Open();
                CARConn.Open();
                resultConn.Open();
                CDSComm = new SqlCommand();
                CDSComm.Connection = CDSConn;
                CARComm = new SqlCommand();
                CARComm.Connection = CARConn;
                ResultComm = new SqlCommand();
                ResultComm.Connection = resultConn;

                //CAR Count
                try
                {
                    CARComm.CommandText = CARSQL;
                    SqlDataReader CARRd = CARComm.ExecuteReader();
                    if (CARRd.Read())
                    {
                        CARCount = CARRd.GetInt32(0);
                    }
                    CARRd.Close();
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    CARCount = -1;
                    if(!failedList.Contains(countObj))
                    {
                        failedList.Add(countObj);
                    }
                }



                //CDS count
                SqlDataReader CDSRd = null;
                try
                {
                    CDSComm.CommandText = CDSSQL;
                    CDSRd = CDSComm.ExecuteReader();
                    if (CDSRd.Read())
                    {
                        CDSCount = CDSRd.GetInt32(0);
                    }
                    CDSRd.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    CDSCount = -1;
                    if (!failedList.Contains(countObj))
                    {
                        failedList.Add(countObj);
                    }
                }

                //CDS Stage count
                try
                {
                    CDSComm.CommandText = StageSQL;
                    CDSRd = CDSComm.ExecuteReader();
                    if (CDSRd.Read())
                    {
                        CDSstageCount = CDSRd.GetInt32(0);
                    }
                    CDSRd.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    CDSstageCount = -1;
                    if (!failedList.Contains(countObj))
                    {
                        failedList.Add(countObj);
                    }
                }

                Console.WriteLine(CDSstageCount.ToString());
                Console.WriteLine(CDSCount.ToString());
                Console.WriteLine(CARCount.ToString());

                WriteResult(countObj, batchID, "CAR", CARCount);
                WriteResult(countObj, batchID, "CDS", CDSCount);
                WriteResult(countObj, batchID, "CDSStage", CDSstageCount);

            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
                failedList.Add(countObj);
            }
            finally
            {
                CDSComm.Dispose();
                CARComm.Dispose();
                ResultComm.Dispose();

                CDSConn.Close();
                CARConn.Close();
                resultConn.Close();
            }

        }

        static void Main(string[] args)
        {
            using (SqlConnection PECon = new SqlConnection(PEConnectionString))
            {
                PECon.Open();
                using (SqlCommand cmd = new SqlCommand())
                {
                    cmd.Connection = PECon;
                    cmd.CommandText = @"select * from [dbo].[PE_ProcessList] where ProcessType in (102) and SystemType <> 4";  // Disable IDS for now
                    SqlDataReader objectListReader = cmd.ExecuteReader();

                    //Get ReconBatch
                    int newBatchID = 0;
                    DateTime currentDateTime = DateTime.Now;
                    string currentDateTimeString = currentDateTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
                    using (SqlConnection resultConn = new SqlConnection(ResultDBConnectionString))
                    {
                        resultConn.Open();
                        using (SqlCommand resultComm = new SqlCommand())
                        {
                            resultComm.Connection = resultConn;
                            resultComm.CommandText = @"insert into CARReconBatch(ReconDate) OUTPUT Inserted.ReconBatchID values('" + currentDateTime + "')";
                            SqlDataReader batchIDReader = resultComm.ExecuteReader();
                            if (batchIDReader.Read())
                            {
                                newBatchID = batchIDReader.GetInt32(0);
                            }
                        }
                    }


                    while (objectListReader.Read())
                    {
#if DEBUG
                        Console.WriteLine(objectListReader["Path"].ToString());
#endif
                        //Start counting...
                        string myPath = objectListReader["Path"].ToString();
                        string myObjectName = objectListReader["ProcessName"].ToString();
                        string mySystemID = "";
                        switch(Int32.Parse(objectListReader["SystemType"].ToString()))
                        {
                            case 1:
                                mySystemID = "CCS";
                                break;
                            case 2:
                                mySystemID = "DCS";
                                break;
                            case 3:
                                mySystemID = "ECS";
                                break;
                            case 4:
                                mySystemID = "IDS";
                                break;                         
                        }

       
                            
                       //Start counting CAR
                        CountObj myCountObj = new CountObj();
                        myCountObj.ObjectName = myObjectName;
                        myCountObj.Path = myPath;
                        myCountObj.SystemID = mySystemID;
                        RecordCount(myCountObj,newBatchID);

                        //
                    }

                    //Start retrying objects has issue
                    int currentRrtryRemain = retryTime;
                    while(failedList.Count>0 && currentRrtryRemain>0)
                    {
                        foreach(CountObj obj in failedList)
                        {
                            RecordCount(obj, newBatchID);
                        }
                        currentRrtryRemain--;
                    }
                    

                }
            }
        }
    }
}

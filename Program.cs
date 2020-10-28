using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace ATMMonitor
{
    public class Program
    {
        private static readonly string connectionstr = System.Configuration.ConfigurationManager.AppSettings.Get("Connectionstring");
        private static readonly string SKQuotepath = System.Configuration.ConfigurationManager.AppSettings.Get("SKQuotepath");
        private static readonly string SKOSQuotepath = System.Configuration.ConfigurationManager.AppSettings.Get("SKOSQuotepath");
        private static readonly string StockATMpath = System.Configuration.ConfigurationManager.AppSettings.Get("StockATMpath");
        private static readonly string linepushpath = System.Configuration.ConfigurationManager.AppSettings.Get("LinePushpath");
        private static readonly string pythonpath = System.Configuration.ConfigurationManager.AppSettings.Get("Pythonpath");
        private static Utilties util = new Utilties();
        
        static void Main(string[] args)
        {
            var tCurrent = TimeSpan.Parse(DateTime.Now.ToString("HH:mm"));
            TimeSpan[] timelist = { TimeSpan.Parse("08:46"), TimeSpan.Parse("13:44"),
                                    TimeSpan.Parse("15:01"), TimeSpan.Parse("23:59"),
                                    TimeSpan.Parse("00:00"), TimeSpan.Parse("05:00") };

            util.RecordLog(connectionstr, "ATMMonitor starts ", util.INFO);

            //The following check for SKOS Future
            //Monday 05:00 to Sat 05:00
            if ((int)DateTime.Now.DayOfWeek >=1 && tCurrent > timelist[5] && !((int)DateTime.Now.DayOfWeek == 6 && tCurrent > timelist[5])

                && ((int)DateTime.Now.DayOfWeek != 0) && !CheckSKOSTickRunning())
            {
                RestartSKOSQuote();
            }
            
            //The following check for TW Future
            //If it's on setttlement day, no need to check tick after 13:30
            if(IsSettlementDay())
            {
                timelist[1] = TimeSpan.Parse("13:30");
            }
            //it's a holiday, why not relax a bit
            else if (IsHoliday())
            {
                util.RecordLog(connectionstr, "ATMMonitor rest on holiday ", util.INFO);
            }
            //Check if initial check is passed before trade begins, first trade start 8:50
            //Monitor starts 1 min after trade open
            else if ((DateTime.Now.ToString("HH:mm") == timelist[0].ToString(@"hh\:mm") || DateTime.Now.ToString("HH:mm") == timelist[2].ToString(@"hh\:mm")) && (int)DateTime.Now.DayOfWeek >= 1 && (int)DateTime.Now.DayOfWeek <= 5) 
            {
                InitCheck();
            }
            //Check if tick conversion is successful, if not need to make some action. This is a redundancy check
            else if (DateTime.Now.ToString("HH:mm") == "14:50" && (int)DateTime.Now.DayOfWeek >= 2 && (int)DateTime.Now.DayOfWeek <= 6)
            {
                //Functioncode 0, check if tick is converted properly
                //Functioncode 1, Check if KLine source is matched with tick source
                RestartSKQuoteWithKLine();
                System.Threading.Thread.Sleep(120000);
                CheckTickConsistency(0, DateTime.Now.ToShortDateString(), 0);
                CheckTickConsistency(1, DateTime.Now.ToShortDateString(), 0);
                //CheckTickConsistency(0, DateTime.Now.AddDays(-1).ToShortDateString(), 1);
                //CheckTickConsistency(1, DateTime.Now.AddDays(-1).ToShortDateString(), 1);
            }
            /// After Saturday 5:00 AM, 
            /// Sunday whole day
            /// Monday before 08:46
            else if (((int)DateTime.Now.DayOfWeek>=6 && tCurrent>= timelist[5]) || ((int)DateTime.Now.DayOfWeek == 0 ) || ((int)DateTime.Now.DayOfWeek == 1 && tCurrent < timelist[0]))
            {
                util.RecordLog(connectionstr, "ATMMonitor rest on specified time ", util.INFO);
            }
            //This is normal trade time situation, do SKOrder and SKQuote check then send message
            else if((tCurrent >= timelist[0] && tCurrent <= timelist[1]) || (tCurrent >= timelist[2] && tCurrent <= timelist[3]) || (tCurrent >= timelist[4] && tCurrent <= timelist[5]))
            {
                if (!CheckATMRunning())
                {
                    RestartSKOrder();
                }
                if (!CheckTickRunning())
                {
                    RestartSKQuote();
                }
            }
            //This runs outside trade time, make sure message is delivered at proper time
            PushMessageToLine();
        }

        public static bool IsSettlementDay()
        {
            if(((int)DateTime.Now.DayOfWeek==3 && DateTime.Now.Day>=15 && DateTime.Now.Day<=20))
            {
                return true;
            }
            else
                return false;
        }

        public static void InitCheck()
        {
            using (SqlConnection connection = new SqlConnection(connectionstr))
            {
                SqlCommand sqlcmd = new SqlCommand();
                connection.Open();
                sqlcmd.Connection = connection;
                sqlcmd.CommandText = "EXEC [dbo].[sp_ChkLatest_KLine] @Chktype = 1, @Session = " + util.GetTradeSession() ;

                if ((int)sqlcmd.ExecuteScalar() == 1)
                    util.RecordLog(connectionstr, "990004. Initial Check Minute K is incompleted", util.ALARM);
                else
                    util.RecordLog(connectionstr, "100000. Initial Check minute K is passed", util.ALARM);

                sqlcmd.CommandText = "EXEC [dbo].[sp_ChkLatest_KLine] @Chktype = 0";
                if ((int)sqlcmd.ExecuteScalar() == 1)
                    util.RecordLog(connectionstr, "990005. Initial Check Daily K is incompleted", util.ALARM);
                else
                    util.RecordLog(connectionstr, "100000. Initial Check Daily K is passed", util.ALARM);
            }
        }

        public static void CheckTickConsistency(int functioncode, string dt, int session)
        {
            using (SqlConnection connection = new SqlConnection(connectionstr))
            {
                SqlCommand sqlcmd = new SqlCommand();
                connection.Open();
                sqlcmd.Connection = connection;
                sqlcmd.CommandText = "EXEC [dbo].[sp_ChkQuoteSourceConsistency] @ndate='" + dt  + "' ,@functioncode=" + functioncode+ ", @TSession= " + session;
                sqlcmd.ExecuteNonQuery();
            }
        }

        public static bool IsHoliday()
        {
            using (SqlConnection connection = new SqlConnection(connectionstr))
            {
                SqlCommand sqlcmd = new SqlCommand();
                connection.Open();
                sqlcmd.Connection = connection;
                sqlcmd.CommandText = "IF DATEADD(hour,0,GETDATE()) BETWEEN (SELECT cast(value as datetime) FROM [dbo].[ATM_Enviroment] WHERE Parameter='holidaybegin') " +
                    "AND (SELECT cast(value as datetime) FROM [dbo].[ATM_Enviroment] WHERE Parameter='holidayend' ) SELECT 1";
                if (sqlcmd.ExecuteScalar() != null)
                    return true;
                return false;
            }
        }

        public static bool CheckSKOSTickRunning()
        {
            util.RecordLog(connectionstr, "Checking SKOSQuote if running", util.INFO);
            bool running = true;

            try
            {

                DataTable dataTable = new DataTable();
                string SQL_Text = "SELECT DISTINCT StockNo FROM [dbo].tblSKOS_WatchList";
                SqlCommand sqlcmd1 = new SqlCommand();
                using (SqlConnection connection = new SqlConnection(connectionstr))
                {
                    sqlcmd1.CommandText = SQL_Text;
                    sqlcmd1.CommandType = CommandType.Text;
                    sqlcmd1.Connection = connection;

                    connection.Open();
                    SqlDataAdapter da = new SqlDataAdapter(sqlcmd1);
                    da.Fill(dataTable);
                    da.Dispose();
                }

                foreach (DataRow row in dataTable.Rows)
                {
                    using (SqlConnection connection = new SqlConnection(connectionstr))
                    {
                        SqlCommand sqlcmd = new SqlCommand();
                        SqlParameter stockname = new SqlParameter();
                        stockname.ParameterName = "@stockname";
                        stockname.Value = row["StockNo"].ToString();
                        sqlcmd.Parameters.Add(stockname);

                        connection.Open();
                        sqlcmd.Connection = connection;
                        sqlcmd.CommandText = "EXEC dbo.sp_Chk_SKOSTick_Running @stockNo=@stockname";

                        if (sqlcmd.ExecuteScalar() != null)
                        {
                            util.RecordLog(connectionstr, "990006. SKOS Tick delay than expected", util.ALARM);
                            running = false;
                        }
                        return running;
                    }
                }
            }
            catch (Exception ex)
            {
                util.RecordLog(connectionstr, "CheckSKOSTickRunning " + ex.Message, util.ALARM);
            }
            return running;
        }

        public void CheckSKOS_Productcode()
        {
            util.RecordLog(connectionstr, "Check if any SKOS product code needs update", util.INFO);
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionstr))
                {
                    SqlCommand sqlcmd = new SqlCommand();
                    connection.Open();
                    sqlcmd.Connection = connection;
                    sqlcmd.CommandText = "EXEC dbo.sp_SKOSUpdateProductCode";
                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                util.RecordLog(connectionstr, "CheckSKOS_Productcode " + ex.Message, util.ALARM);
            }
        }

        private static void RestartSKOSQuote()
        {
            try
            {
                util.RecordLog(connectionstr, "Restarting SKOSQuote", util.INFO);
                Process[] proc = Process.GetProcessesByName("SKOSQuote");
                proc[0].Kill();
            }
            catch (Exception ex)
            {
                util.RecordLog(connectionstr, "Restarting SKOSQuote " + ex.Message, util.ALARM);
            }
            finally
            {
                Process p = new Process();
                p.StartInfo = new ProcessStartInfo(SKOSQuotepath)
                {
                    Arguments = "-SKOSTick",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };
                p.Start();
            }
        }
        public static bool CheckTickRunning()
        {
            util.RecordLog(connectionstr, "Checking SKQuote if running", util.INFO);
            bool running = true;
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionstr))
                {
                    SqlCommand sqlcmd = new SqlCommand();
                    connection.Open();
                    sqlcmd.Connection = connection;
                    sqlcmd.CommandText = "EXEC dbo.sp_ChkTickRunning @session="  + util.GetTradeSession() + ",@functioncode=0"  ;
                    
                    if (sqlcmd.ExecuteScalar() != null){
                        util.RecordLog(connectionstr, "990003. Tick delay than expected", util.ALARM);
                        running = false;
                    }

                    sqlcmd.CommandText = "EXEC dbo.sp_ChkTickRunning @session=" + util.GetTradeSession() + ",@functioncode=1";

                    if (sqlcmd.ExecuteScalar() != null){
                        util.RecordLog(connectionstr, "990002. Missing Tick found", util.ALARM);
                        running = false;
                    }

                    sqlcmd.CommandText = "EXEC dbo.sp_ChkTickRunning @session=" + util.GetTradeSession() + ",@functioncode=2";

                    if (sqlcmd.ExecuteScalar() != null){
                        util.RecordLog(connectionstr, "990001. Min date and Day date are not equal ", util.ALARM);
                        running = false;
                    }

                    sqlcmd.CommandText = "EXEC dbo.sp_ChkTickRunning @session=" + util.GetTradeSession() + ",@functioncode=3";
                    sqlcmd.ExecuteScalar();

                    connection.Close();
                    return running;
                }
            }
            catch (Exception ex)
            {
                util.RecordLog(connectionstr, "CheckTickRunning " + ex.Message, util.ALARM);
            }
            return running;
        }

        public static bool CheckATMRunning()
        {
            util.RecordLog(connectionstr, "Checking SKOrder if running", util.INFO);
            bool running = true;
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionstr))
                {
                    SqlCommand sqlcmd = new SqlCommand();
                    SqlParameter interval = new SqlParameter();
                    interval.ParameterName = "@intervalms";
                    interval.Value = 300000;// 5 minutes interval
                    sqlcmd.Parameters.Add(interval);
                    connection.Open();
                    sqlcmd.Connection = connection;
                    sqlcmd.CommandText = "EXEC dbo.sp_ChkSKOorder @intervalms=@intervalms, @functioncode=0";
                    var returnval = sqlcmd.ExecuteScalar();
                    if (returnval != null)  //Return fasle to restart StockATM application
                    {
                        util.RecordLog(connectionstr, "CheckSKOrderRunning: StockATM is delayed, restarting", util.ALARM);
                        running = false;
                    }
                    //Check for script execution, send notification only, no need to restart
                    sqlcmd.CommandText = "EXEC dbo.sp_ChkSKOorder @intervalms=@intervalms, @functioncode=1";
                    sqlcmd.ExecuteScalar();

                    //Check for process cycle time execution, send notification only, no need to restart
                    sqlcmd.CommandText = "EXEC dbo.sp_ChkSKOorder @intervalms=@intervalms, @functioncode=2";
                    sqlcmd.ExecuteScalar();

                    //Check for process cycle time execution, send notification only, no need to restart
                    sqlcmd.CommandText = "EXEC dbo.sp_ChkSKOorder @intervalms=@intervalms, @functioncode=3";
                    sqlcmd.ExecuteScalar();

                    //Check for number of python execution and number of strats, send notification only, no need to restart
                    sqlcmd.CommandText = "EXEC dbo.sp_ChkSKOorder @intervalms=@intervalms, @functioncode=4";
                    sqlcmd.ExecuteScalar();

                    connection.Close();
                    return running;
                }
            }
            catch (Exception ex)
            {
                util.RecordLog(connectionstr, "CheckSKOrderRunning " + ex.Message, util.ALARM);
            }
            return running;
        }

        private static void RestartSKOrder()
        {
            try
            {
                util.RecordLog(connectionstr, "Restarting SKOrder", util.INFO);
                Process[] proc = Process.GetProcessesByName("StockATM");
                proc[0].Kill();
            }
            catch (Exception ex)
            {
                util.RecordLog(connectionstr, "Restarting SKOrder " + ex.Message, util.ALARM);
            }
            finally
            {
                Process p = new Process();
                p.StartInfo = new ProcessStartInfo(StockATMpath)
                {
                    Arguments = "-starttime " + RoundUp(DateTime.Now, TimeSpan.FromMinutes(1)).ToString("HH:mm"),
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };
                p.Start();
            }
        }

        private static void RestartSKQuoteWithKLine()
        {
            try
            {
                util.RecordLog(connectionstr,"Download SKQuote KLine", util.INFO);
                Process[] proc = Process.GetProcessesByName("SKQuote");
                proc[0].Kill();
            }
            catch (Exception ex)
            {
                util.RecordLog(connectionstr, "Download SKQuote " + ex.Message, util.ALARM);
            }
            finally
            {
                Process p = new Process();
                p.StartInfo = new ProcessStartInfo(SKQuotepath)
                {
                    Arguments = "-KLine",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };
                p.Start();
            }
        }

        private static void RestartSKQuote()
        {
            try
            {
                util.RecordLog(connectionstr, "Restarting SKQuote", util.INFO);
                Process[] proc = Process.GetProcessesByName("SKQuote");
                proc[0].Kill();
            }
            catch (Exception ex)
            {
                util.RecordLog(connectionstr,  "RestartSKQuote " + ex.Message, util.ALARM);
            }
            finally
            {
                Process p = new Process();
                p.StartInfo = new ProcessStartInfo(SKQuotepath)
                {
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };
                p.Start();
            }
        }
        /*This round to next 5 minutes
        */
        private static DateTime RoundUp(DateTime dt, TimeSpan d)
        {
            return new DateTime((dt.Ticks + d.Ticks - 1) / d.Ticks * d.Ticks, dt.Kind);
        }

        private static void PushMessageToLine()
        {
            try
            {
                util.RecordLog(connectionstr,  "Check if any line messages", util.INFO);
                string scriptName = linepushpath;
                Process p = new Process();
                p.StartInfo = new ProcessStartInfo(pythonpath, scriptName)
                {
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };
                p.Start();
                string output = p.StandardOutput.ReadToEnd();
                p.WaitForExit();
            }
            catch (Exception ex)
            {
                util.RecordLog(connectionstr, "PushMessageToLine " + ex.Message, util.ALARM);
            }
        }

    }
}

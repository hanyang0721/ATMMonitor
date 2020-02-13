using System;
using System.Data.SqlClient;
using System.Diagnostics;

namespace ATMMonitor
{
    public class Program
    {
        private static readonly string connectionstr = System.Configuration.ConfigurationManager.AppSettings.Get("Connectionstring");
        private static readonly string SKQuotepath = System.Configuration.ConfigurationManager.AppSettings.Get("SKQuotepath");
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
            //Downloading the K Line, and do a source compare check
            //if (DateTime.Now.ToString("HH:mm") == "08:00" && (int)DateTime.Now.DayOfWeek>=1 && (int)DateTime.Now.DayOfWeek<=6)
            //{
            //    RestartSKQuoteWithKLine();
            //    CheckTickConsistency(1, DateTime.Now.ToShortDateString(),1);//Functioncode 1, compares KLine and tick source
            //    CheckTickConsistency(1, DateTime.Now.AddDays(-1).ToShortDateString(),0);
            //}
            //Check if initial check is passed before trade begins, first trade start 8:50
            //Monitor starts 1 min after trade open
            if ((DateTime.Now.ToString("HH:mm") == "08:46" || DateTime.Now.ToString("HH:mm") == "15:01") && (int)DateTime.Now.DayOfWeek >= 1 && (int)DateTime.Now.DayOfWeek <= 5) 
            {
                InitCheck();
            }
            //Check if tick conversion is successful, if not need to make some action. This is a redundancy check
            else if (DateTime.Now.ToString("HH:mm") == "14:50" && (int)DateTime.Now.DayOfWeek >= 2 && (int)DateTime.Now.DayOfWeek <= 6)
            {
                //(((DateTime.Now.ToString("HH:mm") == "14:50") && (int)DateTime.Now.DayOfWeek >= 1 && (int)DateTime.Now.DayOfWeek <= 5) || 
                //Functioncode 0, check if tick is converted properly
                //This is just double check, triple check...etc
                RestartSKQuoteWithKLine();
                System.Threading.Thread.Sleep(120000);
                CheckTickConsistency(0, DateTime.Now.ToShortDateString(), 0);
                CheckTickConsistency(1, DateTime.Now.ToShortDateString(), 0);
                CheckTickConsistency(0, DateTime.Now.AddDays(-1).ToShortDateString(), 1);
                CheckTickConsistency(1, DateTime.Now.AddDays(-1).ToShortDateString(), 1);
            }
            ////Settlement day, Saturday
            else if (IsSettlementDay() || ((int)DateTime.Now.DayOfWeek>=6 && tCurrent>= timelist[5]))
            {
              //chill a bit, do nothing
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
                //PushMessageToLine();
            }
            //This runs outside trade time, make sure message is delivered at proper time
            PushMessageToLine();
        }

        public static bool IsSettlementDay()
        {
            if(((int)DateTime.Now.DayOfWeek==3 && DateTime.Now.Day>=15 && DateTime.Now.Day<=21))
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
                sqlcmd.CommandText = "EXEC	[Stock].[dbo].[sp_ChkLatest_KLine] @Chktype = 1, @Session = " + util.GetTradeSession() ;

                if ((int)sqlcmd.ExecuteScalar() == 1)
                {
                    util.RecordLog(connectionstr, "990004. Initial Check Minute K is incompleted", util.ALARM);
                }
                else
                {
                    util.RecordLog(connectionstr, "100000. Initial Check minute K is passed", util.ALARM);
                }

                sqlcmd.CommandText = "EXEC	[Stock].[dbo].[sp_ChkLatest_KLine] @Chktype = 0";
                if ((int)sqlcmd.ExecuteScalar() == 1)
                {
                    util.RecordLog(connectionstr, "990005. Initial Check Daily K is incompleted", util.ALARM);
                }
                else
                {
                    util.RecordLog(connectionstr, "100000. Initial Check Daily K is passed", util.ALARM);
                }
            }
        }

        public static void CheckTickConsistency(int functioncode, string dt, int session)
        {
            using (SqlConnection connection = new SqlConnection(connectionstr))
            {
                SqlCommand sqlcmd = new SqlCommand();
                connection.Open();
                sqlcmd.Connection = connection;
                sqlcmd.CommandText = "EXEC	[Stock].[dbo].[sp_chkQuoteSourceConsistency] @ndate='" + dt  + "' ,@functioncode=" + functioncode+ ", @TSession= " + session;
                sqlcmd.ExecuteNonQuery();
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
                    
                    if (sqlcmd.ExecuteScalar() != null)
                    {
                        util.RecordLog(connectionstr, "990003. Tick delay than expected", util.ALARM);
                        running = false;
                    }

                    sqlcmd.CommandText = "EXEC dbo.sp_ChkTickRunning @session=" + util.GetTradeSession() + ",@functioncode=1";

                    if (sqlcmd.ExecuteScalar() != null)
                    {
                        util.RecordLog(connectionstr, "990002. Missing Tick found", util.ALARM);
                        running = false;
                    }

                    sqlcmd.CommandText = "EXEC dbo.sp_ChkTickRunning @session=" + util.GetTradeSession() + ",@functioncode=2";

                    if (sqlcmd.ExecuteScalar() != null)
                    {
                        util.RecordLog(connectionstr, "990001. Min date and Day date are not equal ", util.ALARM);
                        running = false;
                    }
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
                    interval.Value = 300000;
                    sqlcmd.Parameters.Add(interval);
                    connection.Open();
                    sqlcmd.Connection = connection;
                    sqlcmd.CommandText = "EXEC dbo.sp_ChkSKOorder @intervalms=@intervalms";

                    if (sqlcmd.ExecuteScalar() != null)  
                    {
                        running = false;
                    }
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
                    Arguments = "-starttime " + RoundUp(DateTime.Now, TimeSpan.FromMinutes(5)).ToString("HH:mm"),
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

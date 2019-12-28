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
            if (!CheckATMRunning())
            {
                RestartSKOrder();
            }

            if (!CheckTickRunning())
            {
                RestartSKQuote();
            }
            PushMessageToLine();
        }

        public static bool CheckTickRunning()
        {
            util.RecordLog(connectionstr, "1. Checking SKQuote if running");
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
                        util.RecordLog(connectionstr, "1. Tick delay than expected");
                        running = false;
                    }

                    sqlcmd.CommandText = "EXEC dbo.sp_ChkTickRunning @session=" + util.GetTradeSession() + ",@functioncode=1";

                    if (sqlcmd.ExecuteScalar() != null)
                    {
                        util.RecordLog(connectionstr, "1. Missing Tick found");
                        running = false;
                    }
                    connection.Close();
                    return running;
                }
            }
            catch (Exception ex)
            {
                util.RecordLog(connectionstr, "CheckTickRunning " + ex.Message);
            }
            return running;
        }

        public static bool CheckATMRunning()
        {
            util.RecordLog(connectionstr, "2. Checking SKOrder if running");
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
                    sqlcmd.CommandText = "EXEC dbo.ChkSKOorder @intervalms=@intervalms";

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
                util.RecordLog(connectionstr, "CheckSKOrderRunning " + ex.Message);
            }
            return running;
        }

        private static void RestartSKOrder()
        {
            try
            {
                util.RecordLog("3.Restarting SKOrder", connectionstr);
                Process[] proc = Process.GetProcessesByName("StockATM");
                proc[0].Kill();
            }
            catch (Exception ex)
            {
                util.RecordLog(connectionstr, "Restarting SKOrder " + ex.Message);
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

        private static void RestartSKQuote()
        {
            try
            {
                util.RecordLog(connectionstr, "3.Restarting SKQuote");
                Process[] proc = Process.GetProcessesByName("SKQuote");
                proc[0].Kill();
            }
            catch (Exception ex)
            {
                util.RecordLog(connectionstr,  "RestartSKQuote " + ex.Message);
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
                util.RecordLog(connectionstr,  "2. Check if any line messages");
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
            catch
            { }
        }

    }
}

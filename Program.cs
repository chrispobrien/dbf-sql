using System;
using System.IO;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace dbf_sql
{
    class Program
    {
        static int Main(string[] args)
        {
            int nRetCode = 0;
            int iThreads = 1;
            List<Thread> workers = new List<Thread>();
            DateTime timeBegin = DateTime.Now;

            // Boilerplate
            if (args.Length==0)
            {
                Console.WriteLine("dbf-sql  DBase Level 7 to Microsoft SQL Server Utility  Version 2.00  03-15-2017.");
                Console.WriteLine("Copyright 2017 Village of Scarsdale, All Rights Reserved.");
                Console.WriteLine("Disclaimer: not warrantied for any purpose.  Use at your own risk.");
                Console.WriteLine("dbf-sql for help screen.");
                Console.WriteLine("Usage:  dbf-sql /s source /h host /d database [/u user] [/p password] [/t threads]");
                Console.WriteLine("Example: dbf-sql /s mytable.dbf /h myserver /d mydatabase /u sa /p mypassword /t 4");
                Console.WriteLine("Warning: ALL tables in the target database will be DROPPED and replaced.");
                Console.WriteLine("Converted files will be named with the file name to the left of the .dbf");
                Console.WriteLine("This only supports dBase Level 7 files, and only certain data types.");
                Console.WriteLine("Multi-threading is supported.  Default is 1 thread (1 table) at a time.");
                Console.WriteLine(".Net Framework 4.x required.");
                Console.WriteLine("Village of Scarsdale");
                Console.WriteLine("1001 Post Road");
                Console.WriteLine("Scarsdale, NY 10583");
                Console.WriteLine("it@scarsdale.com");
                Console.WriteLine("(914) 722-1174");
                return (0);
            }
            else
            {
                Console.WriteLine("dbf-sql  DBase Level 7 to Microsoft SQL Server Utility  Version 2.00  03-15-2017");
                Console.WriteLine("Copyright 2017 Village of Scarsdale, All Rights Reserved.");
                Console.WriteLine("Disclaimer: not warrantied for any purpose.  Use at your own risk.");
                Console.WriteLine("dbf-sql for help screen.");
            }

            // Parse command line args - args[0] is the first argument

            int iSourceTag = -1, iHostTag = -1, iDatabaseTag = -1, iUserTag = -1, iPasswordTag = -1, iThreadTag = -1;
            int c;

            string sHost = "", sDatabase = "";

            for (c=0;c<args.Length;c++)
            {
                Console.WriteLine(args[c]);
                if (args[c].ToLower() == "/s" || args[c].ToLower() == "-s") iSourceTag = c;
                if (args[c].ToLower() == "/h" || args[c].ToLower() == "-h") iHostTag = c;
                if (args[c].ToLower() == "/d" || args[c].ToLower() == "-d") iDatabaseTag = c;
                if (args[c].ToLower() == "/u" || args[c].ToLower() == "-u") iUserTag = c;
                if (args[c].ToLower() == "/p" || args[c].ToLower() == "-p") iPasswordTag = c;
                if (args[c].ToLower() == "/t" || args[c].ToLower() == "-t") iThreadTag = c;
            }

            if (iHostTag > -1 && iHostTag < args.Length)
                sHost = args[iHostTag + 1];

            if (iDatabaseTag > -1 && iDatabaseTag < args.Length)
                sDatabase = args[iDatabaseTag + 1];

            if (iThreadTag > -1 && iThreadTag < args.Length)
                iThreads = Convert.ToInt16(args[iThreadTag + 1]);

            if (iSourceTag == -1)
            {
                Console.WriteLine("Missing source argument /s [filename].  Wildcards allowed.");
                return (1);
            }

            if (iHostTag == -1)
            {
                Console.WriteLine("Missing host argument /h [server].");
                return (1);
            }

            if (iDatabaseTag == -1)
            {
                Console.WriteLine("Missing database argument /d [database].");
                return (1);
            }

            // Get list of files to convert, accepts wildcards
            string[] sSourceFiles = ExpandFilePath(args[iSourceTag + 1]);
            for (c=0;c<sSourceFiles.Length;c++)
            {
                Thread t = null;
                while (t == null)   // Basically sleep until we can add this thread
                {
                    // Prune workers list of stopped threads
                    for (int tc = workers.Count - 1; tc > -1; tc--)
                        if (workers[tc].ThreadState == ThreadState.Stopped)
                        {
                            workers.Remove(workers[tc]);
                        }

                    // If number of active threads is less than maximum, start the next table conversion
                    if (workers.Count < iThreads)
                    {
                        Console.WriteLine(sSourceFiles[c]);
                        string sSourceFile = sSourceFiles[c];
                        t = new Thread(delegate () { ConvertDBFtoSQL(sSourceFile, sHost, sDatabase); });
                        t.Priority = ThreadPriority.Normal;
                        workers.Add(t);
                        t.Start();
                        //nRetCode = ConvertDBFtoSQL(sSourceFiles[c], sHost, sDatabase);

                    }
                    else Thread.Sleep(1000);
                }
            }

            // Delay while last threads are running
            while (workers.Count>0)
            {
                // Prune workers list of stopped threads
                for (int tc = workers.Count - 1; tc > -1; tc--)
                    if (workers[tc].ThreadState == ThreadState.Stopped)
                    {
                        workers.Remove(workers[tc]);
                    }
                Thread.Sleep(1000);
            }

            DateTime timeEnd = DateTime.Now;
            Console.WriteLine("Time Elapsed: " + (timeEnd - timeBegin));

            return (nRetCode);
        }

        // http://stackoverflow.com/questions/381366/is-there-a-wildcard-expansion-option-for-net-apps
        public static string[] ExpandFilePaths(string[] args)
        {
            var fileList = new List<string>();

            foreach (var arg in args)
            {
                var substitutedArg = System.Environment.ExpandEnvironmentVariables(arg);

                var dirPart = Path.GetDirectoryName(substitutedArg);
                if (dirPart.Length == 0)
                    dirPart = ".";

                var filePart = Path.GetFileName(substitutedArg);

                foreach (var filepath in Directory.GetFiles(dirPart, filePart))
                    fileList.Add(filepath);
            }

            return fileList.ToArray();
        }

        // Single path
        public static string[] ExpandFilePath(string arg)
        {
            var fileList = new List<string>();

                var substitutedArg = System.Environment.ExpandEnvironmentVariables(arg);

                var dirPart = Path.GetDirectoryName(substitutedArg);
                if (dirPart.Length == 0)
                    dirPart = ".";

                var filePart = Path.GetFileName(substitutedArg);

                foreach (var filepath in Directory.GetFiles(dirPart, filePart))
                    fileList.Add(filepath);

            return fileList.ToArray();
        }

        // Worker thread
        static int ConvertDBFtoSQL(string sSourcePath, string sHost, string sDatabase)
        {
            string sConnectionString = "SERVER=" + sHost + ";DATABASE=" + sDatabase + ";Trusted_Connection=Yes;";
            string fname = Path.GetFileNameWithoutExtension(sSourcePath);
            string sSchema = "CREATE TABLE " + fname + " (";
            Stream sStream;
            BinaryReader br;  // NOTE: ReadChars is a little buggy, may be related to Unicode, but it often doesn't work - use ReadBytes
            ASCIIEncoding ae = new ASCIIEncoding();     // Instantiate ASCIIEncoding class to convert bytes to chars
            DataTable dt = new DataTable(fname);        // Internal table to load dBase schema and data

            // Open dBase table as stream
            try
            {
                sStream = File.Open(sSourcePath,FileMode.Open,FileAccess.Read,FileShare.ReadWrite);
            }
            catch ( Exception e )
            {
                Console.WriteLine("Error opening source file File.Open: " + e.Message);
                return (1);
            }

            // Create binaryreader from stream
            try
            {
                br = new BinaryReader(sStream);
            }
            catch ( Exception e )
            {
                Console.WriteLine("Error creating binary reader from stream: " + e.Message);
                return (1);
            }

            // Create class to read dbf file and assign binaryreader/io stream/DBF file
            DbfReader drSource = new DbfReader(br);
            
            // Try using Windows authentication
            SqlConnection cnn = new SqlConnection(sConnectionString);
            try
            {
                cnn.Open();
            }
            catch ( SqlException e )
            {
                Console.WriteLine("Error opening SQL Connection: " + e.Message);
                return (1);
            }
            // TODO: Use SQL Server authentication if supplied in command line args

            // Remove file extension and use remainder as table name (DROP TABLE in case schema has changed)
            SqlCommand scDrop = new SqlCommand("DROP TABLE " + fname, cnn);
            try
            {
                scDrop.ExecuteNonQuery();
            }
            catch ( SqlException e )
            {
                Console.WriteLine("Error dropping table [" + fname + "]: " + e.Message);
            }

            DataTable dtTarget = new DataTable();

            DBASEHEADER h = drSource.ReadHeader();
            List<DBASEFIELD> f = new List<DBASEFIELD>();

            // Decimal 13 is used to signal end of field definitions
            while (br.ReadByte() != 13)
                {
                    br.BaseStream.Seek(-1, SeekOrigin.Current);
                    f.Add(drSource.ReadField());
                }
            /*
            foreach(DBASEFIELD t in f)
            {
                Console.WriteLine(new string(t.m_fieldname) + " " + Convert.ToChar(t.m_fieldtype).ToString() + " " + Convert.ToString(t.m_fieldlength));
            }
            */

            // Assign internal datatable data types and assemble SQL CREATE statement
            foreach (DBASEFIELD t in f)
            {
                string sDataType = "System.String";
                int maxlength = 0;

                switch(Convert.ToChar(t.m_fieldtype))
                {
                    case 'C':   // Character
                        sDataType = "System.String";
                        maxlength = t.m_fieldlength;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " varchar(" + maxlength + ") NULL,";
                        break;
                    case 'N':   // Number
                        sDataType = "System.String";
                        maxlength = t.m_fieldlength;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " varchar(" + maxlength + ") NULL,";
                        break;
                    case 'L':   // Logical Byte
                        sDataType = "System.String";
                        maxlength = t.m_fieldlength;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " varchar(" + maxlength + ") NULL,";
                        break;
                    case 'I':   // Integer
                        sDataType = "System.Int32";
                        maxlength = 2;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " int NULL,";
                        break;
                    case '+':   // Auto-Increment Integer
                        sDataType = "System.Int32";
                        maxlength = 2;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " int NULL,";
                        break;
                    case 'D':   // Date YYYMMDD
                        sDataType = "System.String";
                        maxlength = 8;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " varchar(" + maxlength + ") NULL,";
                        break;
                    case 'M':   // Memo
                        sDataType = "System.String";
                        maxlength = 10;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " varchar(" + maxlength + ") NULL,";
                        break;
                    case 'F':   // Floating Point
                        sDataType = "System.Single";
                        maxlength = 8;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " float NULL,";
                        break;
                    case 'B':   // Binary double integer
                        sDataType = "System.UInt32";
                        maxlength = 4;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " int NULL,";
                        break;
                    case 'G':   // General
                        sDataType = "System.String";
                        maxlength = t.m_fieldlength;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " varchar(" + maxlength + ") NULL,";
                        break;
                    case 'P':   // Picture
                        sDataType = "System.String";
                        maxlength = 0;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() +"]";
                        sSchema += " varchar(" + maxlength + ") NULL,";
                        break;
                    case 'Y':   // Currency
                        sDataType = "System.Decimal";
                        maxlength = 8;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " decimal(12,4) NULL,";
                        break;
                    case 'O':   // 8 bytes double
                        sDataType = "System.Double";
                        maxlength = 8;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " float NULL,";
                        break;
                    case '@':   // Timestamp 8 bytes double
                        sDataType = "System.UInt32";
                        maxlength = 8;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " int NULL,";
                        break;
                    default:
                        sDataType = "System.String";
                        maxlength = t.m_fieldlength;
                        sSchema += "[" + new string(t.m_fieldname).Replace("\0", string.Empty).TrimEnd() + "]";
                        sSchema += " varchar(" + maxlength + ") NULL,";
                        break;
                }

                // Add new column to datatable schema, strings have a maxlength, others don't
                if (sDataType == "System.String")
                {
                    dt.Columns.Add(new DataColumn()
                    {
                        ColumnName = new string(t.m_fieldname).Replace("\0",string.Empty).TrimEnd(),
                        DataType = System.Type.GetType(sDataType),
                        AllowDBNull = true,
                        MaxLength = maxlength
                    });
                } else
                {
                    dt.Columns.Add(new DataColumn()
                    {
                        ColumnName = new string(t.m_fieldname).TrimEnd(),
                        DataType = System.Type.GetType(sDataType),
                        AllowDBNull = true
                    });
                }
            }
            sSchema = sSchema.TrimEnd(',');
            sSchema += ")"; // end SQL CREATE statement

            // datatable and sql schema CREATE statement created
            
            // fast-forward to records section
            br.BaseStream.Position = h.m_headerlength;

            // load records into memory datatable
            for (int rc=0;rc<h.m_numrecords;rc++)
            {
                if (br.ReadByte() == '\x20')    // 0x2a=deleted, 0x20-valid
                {
                    DataRow dr = dt.NewRow();

                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        switch (Convert.ToChar(f[i].m_fieldtype))
                        {
                            case 'C':   // Character
                                byte[] b = br.ReadBytes(f[i].m_fieldlength);
                                if (b[0] == 0)
                                    dr.SetField(i, DBNull.Value);
                                else
                                    dr[i] = ae.GetString(b).TrimEnd();  // Not sure if I have to use TrimEnd, strings are null terminated
                                break;
                            case 'N':   // Number 18-20 characters long
                                b = br.ReadBytes(f[i].m_fieldlength);
                                if (b[0] == 0)
                                    dr.SetField(i, DBNull.Value);
                                else
                                    dr[i] = ae.GetString(b).Replace("\0", string.Empty);  // Not sure if I have to use TrimEnd, strings are null terminated
                                break;
                            case 'L':   // Logical Byte
                                b = br.ReadBytes(f[i].m_fieldlength);
                                if (b[0] == 0)
                                    dr.SetField(i, DBNull.Value);
                                else
                                    dr[i] = ae.GetString(b).Replace("\0", string.Empty);  // Not sure if I have to use TrimEnd, strings are null terminated
                                break;
                            case 'I':   // 4-byte little endian integer
                                b = br.ReadBytes(f[i].m_fieldlength);
                                dr[i] = drSource.ConvertByteToInt(b);
                                break;
                            case '+':   // 4-byte little endian integer
                                b = br.ReadBytes(f[i].m_fieldlength);
                                dr[i] = drSource.ConvertByteToInt(b);
                                break;
                            case 'D':   // Date YYYYMMDD - would like to convert this to datetime I suppose
                                b = br.ReadBytes(f[i].m_fieldlength);
                                if (b[0] == 0)
                                    dr.SetField(i, DBNull.Value);
                                else
                                    dr[i] = ae.GetString(b).Replace("\0", string.Empty);  // Not sure if I have to use TrimEnd, strings are null terminated
                                break;
                            case 'M':
                                b = br.ReadBytes(f[i].m_fieldlength);
                                if (b[0] == 0)
                                    dr.SetField(i, DBNull.Value);
                                else
                                    dr[i] = ae.GetString(b).Replace("\0", string.Empty);  // Not sure if I have to use TrimEnd, strings are null terminated
                                break;
                            case 'F':
                                // TODO: Write code to convert
                                break;
                            case 'B':
                                // TODO: Write code to convert
                                break;
                            case 'G':
                                // TODO: Write code to convert
                                break;
                            case 'P':
                                // TODO: Write code to convert
                                break;
                            case 'Y':
                                // TODO: Write code to convert
                                break;
                            case 'O':   // 8 bytes double
                                byte[] d = br.ReadBytes(f[i].m_fieldlength);
                                dr[i] = drSource.ConvertByteToDouble(d);
                                break;

                            /* Timestamp is made up of 2 longs, 1 for date, 1 for time
                                Date is # of days since 1/1/4713 BC
                                Time is hours*3600000L + minutes*60000L + seconds*1000L */

                            case '@':
                                b = br.ReadBytes(f[i].m_fieldlength);
                                // TODO: Write code to convert
                                break;


                            default:
                                // Advance stream
                                //br.ReadChars(f[i].m_fieldlength);
                                // TODO: Probably do text conversion but ignore for now
                                break;
                        }
                    }

                    try
                    {
                        dt.Rows.Add(dr);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error adding row to internal datatable: " + e.Message);
                    }

                }
                else
                {
                    br.ReadBytes(h.m_recordlength - 1); // Skip over deleted record
                }
                    
            }

            // Remove file extension and use remainder as table name (DROP TABLE in case schema has changed)
            SqlCommand scCreate = new SqlCommand(sSchema, cnn);
            try
            {
                scCreate.ExecuteNonQuery();
            }
            catch (SqlException e)
            {
                Console.WriteLine("Error creating table [" + fname + "]: " + e.Message);
                return (1);
            }

            SqlBulkCopy bc = new SqlBulkCopy(cnn,SqlBulkCopyOptions.KeepNulls,null);
            bc.DestinationTableName = fname;
            bc.WriteToServer(dt);

            br.Close();         // Binary Reader
            sStream.Close();    // Stream
            cnn.Close();        // SQL Connection

            return (0);

        }
    }
}

//******************************************************************************************************
//  FileReader.cs - Gbtc
//
//  Copyright © 2021, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
//  file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  10/12/2021 - J. Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************

using GSF;
using GSF.Configuration;
using GSF.Diagnostics;
using GSF.Data;
using GSF.Data.Model;
using GSF.IO;
using GSF.Security;
using GSF.Scheduling;
using GSF.Threading;
using GSF.TimeSeries;
using GSF.TimeSeries.Adapters;
using GSF.Units.EE;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

using static CSVLimitsLoader.Model.GlobalSettings;
using DeviceRecord = GSF.TimeSeries.Model.Device;
using MeasurementRecord = GSF.TimeSeries.Model.Measurement;
using SignalTypeRecord = GSF.TimeSeries.Model.SignalType;
using RuntimeRecord = GSF.TimeSeries.Model.Runtime;
using SignalType = GSF.Units.EE.SignalType;

#pragma warning disable CA1031 // Do not catch general exception types

namespace CSVLimitsLoader
{
    /// <summary>
    /// Represents an input adapter that imports SCADA/EMS limits from a CSV file on a schedule.
    /// </summary>
    [Description("CSV Limits Loader: Reads SCADA/EMS limits from a CSV file on a schedule")]
    public class FileReader : InputAdapterBase
    {
        #region [ Members ]

        // Constants
        private const bool DefaultAutoCreateCSVPath = false;
        private const string DefaultImportSchedule = "*/5 * * * *";
        private const double DefaultImportDelay = 30.0D;
        private const string DefaultIDColumns = "0,1";
        private const string DefaultDataColumns = "10,11,12,13";
        private const string DefaultDataSuffixes = "HighAlert,HighWarning,LowWarning,LowAlert";
        private const bool DefaultImportNaNValues = false;
        private const bool DefaultDeleteCSVAfterImport = false;
        private const double DefaultReadLockTimeout = 5.0D;
        private const int DefaultHeaderRows = 1;
        private const string DefaultParentDeviceAcronymTemplate = "LIMITS!{0}";
        private const double DefaultMeasurementAdder = 0.0D;
        private const double DefaultMeasurementMultiplier = 1000000.0D;
        private const SignalType DefaultMeasurementSignalType = SignalType.ALOG;
        private const bool DefaultEnableImportLog = true;
        private const string DefaultImportLogFilePath = "{0}-ImportLog.txt";
        private const int DefaultImportLogFileSize = LogFile.DefaultFileSize;
        private const LogFileFullOperation DefaultImportLogFileFullOperation = LogFile.DefaultFileFullOperation;

        // Fields
        private readonly LongSynchronizedOperation m_importOperation;
        private readonly LogFile m_importLog;
        private readonly ScheduleManager m_scheduleManager;
        private int m_importDelayInterval;
        private ICancellationToken m_importDelayCancellationToken;

        private int[] m_idColumns;
        private int[] m_dataColumns;
        private string[] m_dataSuffixes;
        private int m_maxColumnMapping;

        private long m_measurementRecordsCreated;
        private long m_totalSuccessfulImports;
        private long m_totalFailedImports;
        private DateTime m_lastSuccessfulImport;
        private DateTime m_lastFailedImport;
        private long m_totalSuccessfulDeletes;
        private long m_totalFailedDeletes;
        private DateTime m_lastSuccessfulDelete;
        private DateTime m_lastFailedDelete;
        private long m_totalNaNValues;
        
        private string m_parentDeviceAcronym;
        private int m_parentDeviceID;
        private int m_parentDeviceRuntimeID;
        private string m_signalName;
        private int m_signalTypeID;
        private SignalKind m_signalKind;
        
        private bool m_disposed;

        #endregion

        #region [ Constructors ]

        /// <summary>
        /// Creates a new instance of the <see cref="FileReader"/> class.
        /// </summary>
        public FileReader()
        {
            m_importOperation = new LongSynchronizedOperation(Import, ex => OnProcessException(MessageLevel.Error, ex));

            m_importLog = new LogFile { TextEncoding = Encoding.UTF8 };
            m_importLog.LogException += (_, e) => OnProcessException(MessageLevel.Error, e.Argument);

            m_scheduleManager = new ScheduleManager();
            m_scheduleManager.ScheduleDue += (_, _) => m_importDelayCancellationToken = 
                new Action(() => m_importOperation.TryRun()).DelayAndExecute(m_importDelayInterval);
        }

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets or sets the path and file name of the CSV file to load.
        /// </summary>
        [ConnectionStringParameter]
        [Description("Defines the path and file name of the CSV file to load")]
        public string CSVFilePath { get; set; }

        /// <summary>
        /// Gets or sets the flag that determines if directory defined in CSVFilePath should be attempted to be created if it does not exist.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultAutoCreateCSVPath)]
        [Description("Defines the flag that determines if directory defined in CSVFilePath should be attempted to be created if it does not exist")]
        public bool AutoCreateCSVPath { get; set; } = DefaultAutoCreateCSVPath;

        /// <summary>
        /// Gets or sets the schedule, defined by cron syntax, to load updated CSV file data.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultImportSchedule)]
        [Description("Defines the schedule, defined by cron syntax, to load updated CSV file data")]
        public string ImportSchedule { get; set; } = DefaultImportSchedule;

        /// <summary>
        /// Gets or sets the delay, in seconds, to postpone top-of-minute CSV imports to reduce read/write contention. Value should be less than 60.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultImportDelay)]
        [Description("Defines the delay, in seconds, to postpone top-of-minute CSV imports to reduce read/write contention; value should be less than 60")]
        public double ImportDelay { get; set; } = DefaultImportDelay;

        /// <summary>
        /// Gets or sets the comma separated, zero-based, column indexes in the CSV that contain import IDs.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultIDColumns)]
        [Description("Defines the comma separated, zero-based, column indexes in the CSV that contain import IDs")]
        public string IDColumns { get; set; } = DefaultIDColumns;

        /// <summary>
        /// Gets or sets the comma separated, zero-based, column indexes in the CSV that contain import data.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultDataColumns)]
        [Description("Defines the comma separated, zero-based, column indexes in the CSV that contain import data; number of values must match DataSuffixes")]
        public string DataColumns { get; set; } = DefaultDataColumns;

        /// <summary>
        /// Gets or sets the comma separated point tag suffixes for each defined data column.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultDataSuffixes)]
        [Description("Defines the comma separated point tag suffixes for each defined data column; number of values must match DataColumns")]
        public string DataSuffixes { get; set; } = DefaultDataSuffixes;

        /// <summary>
        /// Gets or sets the flag that determines if encountered NaN values should be imported.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultImportNaNValues)]
        [Description("Defines the flag that determines if encountered NaN values should be imported")]
        public bool ImportNaNValues { get; set; } = DefaultImportNaNValues;

        /// <summary>
        /// Gets or sets the flag that determines if CSV file should deleted after import.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultDeleteCSVAfterImport)]
        [Description("Defines the flag that determines if CSV file should deleted after import")]
        public bool DeleteCSVAfterImport { get; set; } = DefaultDeleteCSVAfterImport;

        /// <summary>
        /// Gets or sets the timeout, in seconds, to wait before timing out while attempting acquire a read lock on CSV file.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultReadLockTimeout)]
        [Description("Defines the timeout, in seconds, to wait before timing out while attempting acquire a read lock on CSV file")]
        public double ReadLockTimeout { get; set; } = DefaultReadLockTimeout;

        /// <summary>
        /// Gets or sets the number of headers that should be skipped before reading CSV file data.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultHeaderRows)]
        [Description("Defines the number of headers that should be skipped before reading CSV file data")]
        public int HeaderRows { get; set; } = DefaultHeaderRows;

        /// <summary>
        /// Gets or sets template for the parent device acronym used to group associated output measurements.
        /// </summary>
        [ConnectionStringParameter]
        [Description("Defines template for the parent device acronym used to group associated output measurements, typically an expression like \"" + DefaultParentDeviceAcronymTemplate + "\" where \"{0}\" is substituted with this adapter's name")]
        [DefaultValue(DefaultParentDeviceAcronymTemplate)]
        public virtual string ParentDeviceAcronymTemplate { get; set; } = DefaultParentDeviceAcronymTemplate;

        /// <summary>
        /// Gets or sets the additive offset that should be used for newly created output measurements.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultMeasurementAdder)]
        [Description("Defines the additive offset that should be used for newly created output measurements")]
        public double MeasurementAdder { get; set; } = DefaultMeasurementAdder;

        /// <summary>
        /// Gets or sets the multiplicative offset that should be used for newly created output measurements.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultMeasurementMultiplier)]
        [Description("Defines the multiplicative offset that should be used for newly created output measurements")]
        public double MeasurementMultiplier { get; set; } = DefaultMeasurementMultiplier;

        /// <summary>
        /// Gets or sets the signal type that should be used for newly created output measurements.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultMeasurementSignalType)]
        [Description("Defines the signal type that should be used for newly created output measurements")]
        public SignalType MeasurementSignalType { get; set; } = DefaultMeasurementSignalType;

        /// <summary>
        /// Gets or sets the flag that determines if log should be maintained for import operations.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultEnableImportLog)]
        [Description("Defines the flag that determines if log should be maintained for import operations")]
        public bool EnableImportLog { get; set; } = DefaultEnableImportLog;

        /// <summary>
        /// Gets or sets the import log file name and optional path. Exclude path to write to same location as CSVFilePath.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultImportLogFilePath)]
        [Description("Defines the import log file name and optional path, typically an expression like \"" + DefaultImportLogFilePath + "\" where \"{0}\" is substituted with this adapter's name; exclude path to write to same location as CSVFilePath")]
        public string ImportLogFilePath { get; set; } = DefaultImportLogFilePath;

        /// <summary>
        /// Gets or sets the maximum file size of the import log in megabytes. Value must be between 1 and 10.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultImportLogFileSize)]
        [Description("Defines the maximum file size of the import log in megabytes; value must be between 1 and 10")]
        public int ImportLogFileSize { get; set; } = DefaultImportLogFileSize;

        /// <summary>
        /// Gets or sets the type of operation to be performed when the import log file is full.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultImportLogFileFullOperation)]
        [Description("Defines the type of operation to be performed when the import log file is full")]
        public LogFileFullOperation ImportLogFileFullOperation { get; set; } = DefaultImportLogFileFullOperation;

        /// <summary>
        /// Gets or sets output measurements that the action adapter will produce, if any.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override IMeasurement[] OutputMeasurements // Redeclared to hide property from TSL host manager - property is automatically managed by this adapter
        {
            get => base.OutputMeasurements;
            set => base.OutputMeasurements = value;
        }

        /// <summary>
        /// Gets or sets <see cref="DataSet"/> based data source available to this <see cref="FileReader"/>.
        /// </summary>
        public override DataSet DataSource
        {
            get => base.DataSource;
            set
            {
                base.DataSource = value;

                if (!Initialized)
                    return;

                // This adapter dynamically creates its own measurements and informs the TSF host system about availability.
                // The data source property is updated anytime TSF reloads its configuration, so we use this as our local
                // notification that new metadata is available that may contain our new output measurements.
                HashSet<IMeasurement> outputMeasurements = new(GetUpdatedOutputMeasurements());

                if (!outputMeasurements.SetEquals(OutputMeasurements))
                    OutputMeasurements = outputMeasurements.ToArray();
            }
        }

        /// <summary>
        /// Gets the flag indicating if this adapter supports temporal processing.
        /// </summary>
        public override bool SupportsTemporalProcessing => false;

        /// <summary>
        /// Gets flag that determines if the data input connects asynchronously.
        /// </summary>
        protected override bool UseAsyncConnect => false;

        /// <summary>
        /// Returns the detailed status of this <see cref="FileReader"/>.
        /// </summary>
        public override string Status
        {
            get
            {
                StringBuilder status = new();

                status.Append(base.Status);
                
                //                  012345678901234567890123456
                status.AppendLine($"             CSV File Path: {CSVFilePath}");
                status.AppendLine($"      Auto-Create CSV Path: {AutoCreateCSVPath}");
                status.AppendLine($"            Import Logging: {(EnableImportLog ? "Enabled" : "Disabled")}");
                status.AppendLine($"      Import Log File Path: {ImportLogFilePath}");
                status.AppendLine($"       Max Import Log Size: {ImportLogFileSize:N0} MB");
                status.AppendLine($" Import Log Full Operation: {ImportLogFileFullOperation}");
                status.AppendLine($"  Configured CRON Schedule: {ImportSchedule}");
                status.AppendLine($"Top-of-Minute Import Delay: {ImportDelay:N3} seconds");
                status.AppendLine($"            CSV ID Columns: {IDColumns}");
                status.AppendLine($"          CSV Data Columns: {DataColumns}");
                status.AppendLine($"   Point Tag Data Suffixes: {DataSuffixes}");
                status.AppendLine($"         Import NaN Values: {ImportNaNValues}");
                status.AppendLine($"     Total Read NaN Values: {m_totalNaNValues:N0}");
                status.AppendLine($"   Delete CSV After Import: {DeleteCSVAfterImport}");
                status.AppendLine($"         Read Lock Timeout: {ReadLockTimeout:N3} seconds");
                status.AppendLine($"       Header Rows to Skip: {HeaderRows:N0}");
                status.AppendLine($"   Device Acronym Template: {ParentDeviceAcronymTemplate}");
                status.AppendLine($"         Measurement Adder: {MeasurementAdder:N3}");
                status.AppendLine($"    Measurement Multiplier: {MeasurementMultiplier:N3}");
                status.AppendLine($"   Measurement Signal Type: {MeasurementSignalType}");
                status.AppendLine($"   New Measurement Records: {m_measurementRecordsCreated:N0} added over {RunTime.ToString(-1)}");
                status.AppendLine($"  Total Successful Imports: {m_totalSuccessfulImports:N0}");
                status.AppendLine($"    Last Successful Import: {(m_lastSuccessfulImport == default ? "Never" : $"{m_lastSuccessfulImport:yyyy-MM-dd HH:mm:ss.fff}")}");
                status.AppendLine($"      Total Failed Imports: {m_totalFailedImports:N0}");
                status.AppendLine($"        Last Failed Import: {(m_lastFailedImport == default ? "Never" : $"{m_lastFailedImport:yyyy-MM-dd HH:mm:ss.fff}")}");

                if (DeleteCSVAfterImport)
                {
                    //                  012345678901234567890123456
                    status.AppendLine($"  Total Successful Deletes: {m_totalSuccessfulDeletes:N0}");
                    status.AppendLine($"    Last Successful Delete: {(m_lastSuccessfulDelete == default ? "Never" : $"{m_lastSuccessfulDelete:yyyy-MM-dd HH:mm:ss.fff}")}");
                    status.AppendLine($"      Total Failed Deletes: {m_totalFailedDeletes:N0}");
                    status.AppendLine($"        Last Failed Delete: {(m_lastFailedDelete == default ? "Never" : $"{m_lastFailedDelete:yyyy-MM-dd HH:mm:ss.fff}")}");
                }

                status.AppendLine();
                status.AppendLine("    -- Active Schedule Status --");
                status.AppendLine();

                if (m_scheduleManager.Schedules?.Count > 0)
                    status.Append(m_scheduleManager.Schedules[0].Status);
                else
                    status.AppendLine("     >> Schedule not enabled");

                if (EnableImportLog)
                {
                    status.AppendLine();
                    status.AppendLine("       -- Import Log Status --");
                    status.AppendLine();
                    status.Append(m_importLog.Status);
                }

                return status.ToString();
            }
        }

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Initializes <see cref="FileReader"/>.
        /// </summary>
        public override void Initialize()
        {
            ConnectionStringParser<ConnectionStringParameterAttribute> parser = new();
            parser.ParseConnectionString(ConnectionString, this);

            base.Initialize();

            m_idColumns = IDColumns.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(value => ushort.TryParse(value.Trim(), out ushort index) ? (ushort?)index : null)
                .Where(index => index is not null)
                .Select(index => (int)index.Value)
                .ToArray();

            m_dataColumns = DataColumns.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(value => ushort.TryParse(value.Trim(), out ushort index) ? (ushort?)index : null)
                .Where(index => index is not null)
                .Select(index => (int)index.Value)
                .ToArray();

            m_dataSuffixes = DataSuffixes.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(value => value.Trim())
                .ToArray();

            if (m_idColumns.Length == 0)
                throw new InvalidOperationException($"No {nameof(IDColumns)} were configured");

            if (m_dataColumns.Length == 0)
                throw new InvalidOperationException($"No {nameof(DataColumns)} were configured");

            if (m_dataSuffixes.Length == 0)
                throw new InvalidOperationException($"No {nameof(DataSuffixes)} were configured");

            if (m_dataColumns.Length != m_dataSuffixes.Length)
                throw new InvalidOperationException($"Configured {nameof(DataColumns)} and {nameof(DataSuffixes)} must be the same length");

            m_maxColumnMapping = m_idColumns.Concat(m_dataColumns).Max() + 1;

            // Open database connection as defined in configuration file "systemSettings" category
            using AdoDataConnection connection = new("systemSettings");

            // Lookup virtual parent device details, creating device if needed, for output measurement associations
            LookupParentDeviceDetails(connection);

            // Cache queried signal type info for configured MeasurementSignalType
            TableOperations<SignalTypeRecord> signalTypeTable = new(connection);
            SignalTypeRecord signalType = signalTypeTable.QueryRecordWhere("Acronym = {0}", MeasurementSignalType.ToString());
            m_signalName = signalType?.Name ?? "Analog Value";
            m_signalTypeID = signalType?.ID ?? 7;
            m_signalKind = (signalType?.Suffix ?? "AV").ParseSignalKind();

            if (ImportDelay >= 60.0D)
            {
                ImportDelay = 59.999D;
                OnStatusMessage(MessageLevel.Warning, $"{nameof(ImportDelay)} adapter parameter adjusted to 59.999 seconds. Configured value must be less than 60.");
            }

            m_importDelayInterval = (int)TimeSpan.FromSeconds(ImportDelay).TotalMilliseconds;

            if (m_importDelayInterval < 0)
                m_importDelayInterval = 0;

            CSVFilePath = FilePath.GetAbsolutePath(CSVFilePath);
            string csvFileDirectory = FilePath.GetDirectoryName(CSVFilePath);

            if (!Directory.Exists(csvFileDirectory))
            {
                if (AutoCreateCSVPath)
                    Directory.CreateDirectory(csvFileDirectory);
                else
                    OnStatusMessage(MessageLevel.Warning, $"Configured path of {nameof(CSVFilePath)} parameter \"{csvFileDirectory}\" does not exist. Scheduled imports may fail.");
            }

            ImportLogFilePath = string.Format(ImportLogFilePath, Name);

            if (FilePath.GetDirectoryName(ImportLogFilePath).Trim() == Path.DirectorySeparatorChar.ToString())
                ImportLogFilePath = Path.Combine(csvFileDirectory, ImportLogFilePath);

            if (ImportLogFileSize < LogFile.MinFileSize)
            {
                ImportLogFileSize = LogFile.MinFileSize;
                OnStatusMessage(MessageLevel.Warning, $"{nameof(ImportLogFileSize)} adapter parameter adjusted to {LogFile.MinFileSize} MB. Configured value must be greater than or equal {LogFile.MinFileSize} MB.");
            }

            if (ImportLogFileSize > LogFile.MaxFileSize)
            {
                ImportLogFileSize = LogFile.MaxFileSize;
                OnStatusMessage(MessageLevel.Warning, $"{nameof(ImportLogFileSize)} adapter parameter adjusted to {LogFile.MaxFileSize} MB. Configured value must be less than or equal {LogFile.MaxFileSize} MB.");
            }

            m_importLog.FileName = ImportLogFilePath;
            m_importLog.FileSize = ImportLogFileSize;
            m_importLog.FileFullOperation = ImportLogFileFullOperation;

            m_scheduleManager.AddSchedule(nameof(FileReader), ImportSchedule, true);

            if (!EnableImportLog)
                return;

            m_importLog.Open();
            WriteLogMessage($"Starting import operations for {Name}: \"{CSVFilePath}\"");
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="FileReader"/> object and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (m_disposed)
                return;

            try
            {
                if (!disposing)
                    return;

                WriteLogMessage($"Stopping import operations for {Name}");
                m_importLog.Flush();

                m_scheduleManager.Dispose();
                m_importDelayCancellationToken?.Cancel();
                m_importLog.Dispose();
            }
            finally
            {
                m_disposed = true;          // Prevent duplicate dispose.
                base.Dispose(disposing);    // Call base class Dispose().
            }
        }

        private void WriteLogMessage(string message)
        {
            if (!EnableImportLog || !m_importLog.IsOpen || m_disposed)
                return;

            m_importLog.WriteTimestampedLine(message);
        }

        private void LookupParentDeviceDetails(AdoDataConnection connection)
        {
            try
            {
                const string DeviceReferenceTemplate = nameof(CSVLimitsLoader) + "." + nameof(FileReader) + "!{0}";

                // Create a virtual parent device for output measurements association. This will group all output measurements
                // for this custom input adapter with the virtual device and make the measurements transportable via STTP.
                TableOperations<DeviceRecord> deviceTable = new(connection);
                string deviceReference = string.Format(DeviceReferenceTemplate, ID);

                // Using Device.Name field to create a reference to a virtual device using this adapter's runtime ID - this way
                // the acronym of the virtual device can be synchronized to the custom input adapter's acronym while keeping all
                // existing associated measurements even when the custom adapter acronym gets updated.
                DeviceRecord device = deviceTable.QueryRecordWhere("Name = {0}", deviceReference) ?? deviceTable.NewRecord();

                m_parentDeviceAcronym = GetCleanAcronym(string.Format(ParentDeviceAcronymTemplate, Name));

                device.NodeID = AdoSecurityProvider.DefaultNodeID;
                device.Acronym = m_parentDeviceAcronym;
                device.Name = deviceReference;
                device.ProtocolID = connection.ExecuteScalar<int?>("SELECT ID FROM Protocol WHERE Acronym = 'VirtualInput'") ?? 11;
                device.ConnectionString = $"note={{Do not edit name \"{deviceReference}\". Value used to cross-reference custom input adapter \"{Name}\".}}";
                device.Enabled = true;

                deviceTable.AddNewOrUpdateRecord(device);

                m_parentDeviceID = device.ID == 0 ? deviceTable.QueryRecordWhere("Name = {0}", deviceReference).ID : device.ID;
                m_parentDeviceRuntimeID = GetRuntimeID("Device", m_parentDeviceID);

                // Attempt to assign any pre-existing associated output measurements
                OutputMeasurements = GetUpdatedOutputMeasurements();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to get parent device ID: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Gets a short one-line status of this <see cref="FileReader"/>.
        /// </summary>
        public override string GetShortStatus(int maxLength) =>
            $"{ProcessedMeasurements:N0} measurements imported so far...".CenterText(maxLength);

        /// <summary>
        /// Attempts to connect to data input source.
        /// </summary>
        protected override void AttemptConnection() =>
            m_scheduleManager.Start();

        /// <summary>
        /// Attempts to disconnect from data input source.
        /// </summary>
        protected override void AttemptDisconnection() =>
            m_scheduleManager.Stop();

        /// <summary>
        /// Queues the CSV limits file for import.
        /// </summary>
        [AdapterCommand("Queues the CSV limits file for import", "Administrator", "Editor")]
        public void QueueImport() =>
            m_importOperation.RunOnce();

        private void Import()
        {
            if (m_disposed)
                return;

            bool createdNewRecords = false;

            try
            {
                if (!File.Exists(CSVFilePath))
                    throw new FileNotFoundException($"CSV limits file \"{CSVFilePath}\" was not found");

                FilePath.WaitForReadLock(CSVFilePath, ReadLockTimeout);

                // Open database connection as defined in configuration file "systemSettings" category
                using AdoDataConnection connection = new("systemSettings");

                // Open the CSV as a text file
                using TextReader reader = File.OpenText(CSVFilePath);
                
                // Skip header rows
                for (int i = 0; i < HeaderRows; i++)
                    reader.ReadLine();

                string line = reader.ReadLine();
                int row = 1;
                int count = 0;

                while (!string.IsNullOrEmpty(line))
                {
                    (List<IMeasurement> measurements, int newRecords) = ParseCSVRow(connection, line, row);

                    if (newRecords > 0)
                    {
                        m_measurementRecordsCreated += newRecords;
                        createdNewRecords = true;
                    }

                    if (measurements.Count > 0)
                    {
                        OnNewMeasurements(measurements);
                        count += measurements.Count;
                    }

                    line = reader.ReadLine();
                    row++;
                }
                
                m_totalSuccessfulImports++;
                m_lastSuccessfulImport = DateTime.Now;

                WriteLogMessage($"Successful CSV Import of {count:N0} Measurements. Totals: {m_totalSuccessfulImports:N0} successful, {m_totalFailedImports:N0} failed.");
                OnStatusMessage(MessageLevel.Info, $"Successfully imported {count:N0} measurements from \"{CSVFilePath}\".");
            }
            catch (Exception ex)
            {
                m_totalFailedImports++;
                m_lastFailedImport = DateTime.Now;

                WriteLogMessage($"ERROR: Failed CSV Import. Totals: {m_totalSuccessfulImports:N0} successful, {m_totalFailedImports:N0} failed.{Environment.NewLine}    >> {ex.Message}");
                OnProcessException(MessageLevel.Error, ex, "CSV Import");
            }
            finally
            {
                // Notify host system of configuration changes when any new measurements are created
                if (createdNewRecords)
                    OnConfigurationChanged();
            }

            if (!DeleteCSVAfterImport)
                return;

            try
            {
                File.Delete(CSVFilePath);

                m_totalSuccessfulDeletes++;
                m_lastSuccessfulDelete = DateTime.Now;

                WriteLogMessage($"Successful Post Import CSV Delete. Totals: {m_totalSuccessfulDeletes:N0} successful, {m_totalFailedDeletes:N0} failed.");
                OnStatusMessage(MessageLevel.Info, $"Successfully deleted \"{CSVFilePath}\".");
            }
            catch (Exception ex)
            {
                m_totalFailedDeletes++;
                m_lastFailedDelete = DateTime.Now;

                WriteLogMessage($"ERROR: Failed Post Import CSV Delete. Totals: {m_totalSuccessfulDeletes:N0} successful, {m_totalFailedDeletes:N0} failed.{Environment.NewLine}    >> {ex.Message}");
                OnProcessException(MessageLevel.Error, ex, "Post Import CSV Delete");
            }
        }

        // Converts the given row of CSV data into output measurements.
        private (List<IMeasurement>, int) ParseCSVRow(AdoDataConnection connection, string line, int row)
        {
            List<IMeasurement> measurements = new();
            string[] columns = line.Split(',');            
            int newRecords = 0;

            if (columns.Length < m_maxColumnMapping)
                throw new InvalidOperationException($"Not enough columns in CSV row {row:N0} to map configured ID and data columns.");

            // Create base measurement point tag name from ID column mappings
            string baseTagName = string.Join(".", m_idColumns.Select(index => columns[index]));
            int rowIndexFactor = (row - 1) * m_dataColumns.Length;

            // Create a new measurement for each data column mapping
            for (int i = 0; i < m_dataColumns.Length; i++)
            {
                int index = m_dataColumns[i];
                string suffix = m_dataSuffixes[i];
                string value = columns[index].Trim();

                if (value.Length == 0)
                    continue;

                // Lookup measurement signal ID, creating measurement record if needed
                (Guid signalID, bool newRecord) = GetMeasurementSignalID(connection, $"{baseTagName}.{suffix}", rowIndexFactor + i + 1);

                if (newRecord)
                    newRecords++;

                if (double.TryParse(value, out double limit))
                {
                    if (double.IsNaN(limit))
                    {
                        m_totalNaNValues++;

                        if (!ImportNaNValues)
                            continue;
                    }

                    measurements.Add(new Measurement
                    {
                        Metadata = MeasurementKey.LookUpBySignalID(signalID).Metadata,
                        Timestamp = DateTime.UtcNow,
                        Value = limit
                    });
                }
                else
                {
                    OnStatusMessage(MessageLevel.Error, $"Failed to parse CSV row {row:N0}, column {index:N0} [{suffix}] value of \"{value}\" as a double-precision floating-point number");
                }
            }

            return (measurements, newRecords);
        }

        // Gets measurement signal ID identified by specified pointTag, creating the measurement record it if needed.
        private (Guid, bool) GetMeasurementSignalID(AdoDataConnection connection, string pointTag, int index)
        {
            TableOperations<MeasurementRecord> measurementTable = new(connection);
            string cleanPointTag = GetCleanAcronym(pointTag);

            // Lookup measurement record by point tag, creating a new record if one does not exist
            MeasurementRecord measurement = measurementTable.QueryRecordWhere("PointTag = {0}", cleanPointTag) ?? measurementTable.NewRecord();

            // Update record fields
            measurement.DeviceID = m_parentDeviceID;
            measurement.PointTag = cleanPointTag;
            measurement.AlternateTag = pointTag;
            measurement.SignalReference = SignalReference.ToString(m_parentDeviceAcronym, m_signalKind, index);
            measurement.SignalTypeID = m_signalTypeID;
            measurement.Adder = MeasurementAdder;
            measurement.Multiplier = MeasurementMultiplier;
            measurement.Description = $"{m_parentDeviceAcronym} {m_signalName} #{index} [{pointTag}]";

            // Save record updates
            measurementTable.AddNewOrUpdateRecord(measurement);

            // Re-query new records to get any database assigned information, e.g., PointID field or possibly updated SignalID
            if (measurement.PointID == 0)
            {
                measurement = measurementTable.QueryRecordWhere("PointTag = {0}", cleanPointTag);
                return (measurement.SignalID, true);
            }

            return (measurement.SignalID, false);
        }

        private int GetRuntimeID(string source, int id)
        {
            using AdoDataConnection connection = new("systemSettings");
            TableOperations<RuntimeRecord> runtimeTable = new(connection);
            RuntimeRecord runtime = runtimeTable.QueryRecordWhere("SourceTable = {0} AND SourceID = {1}", source, id);
            return runtime?.ID ?? 0;
        }

        private IMeasurement[] GetUpdatedOutputMeasurements() =>
            ParseOutputMeasurements(DataSource, false, $"FILTER ActiveMeasurements WHERE DeviceID = {m_parentDeviceRuntimeID}");

        #endregion

        #region [ Static ]

        // Static Constructor
        static FileReader() => 
            ValidateModelDependencies();

        // Static Methods
        private static string GetCleanAcronym(string acronym) =>
            Regex.Replace(acronym.ToUpperInvariant().Replace(" ", "_"), @"[^A-Z0-9\-!_\.@#\$]", "", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        #endregion
    }
}

# CSVLimitsLoader

### Grid Solutions Framework (GSF) Time-series Library (TSL) Adapter for Loading EMS Limits from a CSV File.

This adapter can be deployed with any application that is based on the [Grid Solutions Framework](https://github.com/GridProtectionAlliance/gsf) - [Time-Series Library](https://www.gridprotectionalliance.org/technology.asp#TSL). This includes products like the [openPDC](https://github.com/GridProtectionAlliance/openPDC), [SIEGate](https://github.com/GridProtectionAlliance/SIEGate) and the [openHistorian](https://github.com/GridProtectionAlliance/openHistorian).

To use the adapter, download the [release](https://github.com/GridProtectionAlliance/CSVLimitsLoader/releases/latest) associated with your product version and the unzip the assembly (`CSVLimitsLoader.dll`) into the product installation folder, e.g., `C:\Program Files\openPDC\`. Note that it may be necessary to [unblock](https://docs.microsoft.com/en-us/powershell/module/microsoft.powershell.utility/unblock-file?view=powershell-7.2) the DLL downloaded from the Internet before the TSL host application can use the adapter. Restarting the host product service after unblocking the assembly is recommended.

To configure a new instance of the adapter, follow these steps:

1) In the TSL host "Manager" application (e.g., the openPDC Manager UI), access the `Custom Inputs Adapter` page, commonly `Inputs > Manage Custom Inputs` from the main menu.
2) Click the `Add New` button located on the right side of the screen right above the table of current adapters.
3) Type in a new `Name` for the adapter, e.g., `VOLTAGELOADER`.
4) Select the `CSV Limits Loader` adapter from the `Type` drop-down list.
5) Assign desired values to the adapter `Connection String Parameters` - see table below.
6) Check the `Enabled` check-box located above the `Delete`, `Add New` and `Save` buttons.
7) Click the `Save` button
8) Click the `Initialize` button
9) Monitor adapter activity in the TSL host "Console"` application (e.g., the openPDC Console)

| Connection String Parameter | Description | Required? | Default |
|:---------------------------:|:------------|:---------:|:-------:|
| `CSVFilePath` | Defines the path and file name of the CSV file to load | Yes | N/A |
| `AutoCreateCSVPath` | Defines the flag that determines if directory defined in `CSVFilePath` should be attempted to be created if it does not exist | No | False |
| `ImportSchedule` | Defines the schedule, defined by cron syntax, to load updated CSV file data | No | */5 * * * * |
| `ImportDelay` | Defines the delay, in seconds, to postpone top-of-minute CSV imports to reduce read/write contention; value should be less than 60 | No | 30.0 |
| `IDColumns` | Defines the comma separated, zero-based, column indexes in the CSV that contain import IDs | No | 0, 1 |
| `DataColumns` | Defines the comma separated, zero-based, column indexes in the CSV that contain import data; number of values must match `DataSuffixes` | No | 10, 11, 12, 13 |
| `DataSuffixes` | Defines the comma separated point tag suffixes for each defined data column; number of values must match `DataColumns` | No | HighAlert, HighWarning, LowWarning, LowAlert |
| `ImportNaNValues` | Defines the flag that determines if encountered `NaN` values should be imported | No | false |
| `DeleteCSVAfterImport` | Defines the flag that determines if CSV file should deleted after import | No | false |
| `ReadLockTimeout` | Defines the timeout, in seconds, to wait before timing out while attempting acquire a read lock on CSV file | No | 5.0 |
| `HeaderRows` | Defines the number of headers that should be skipped before reading CSV file data | No | 1 |
| `ParentDeviceAcronymTemplate` | Defines template for the parent device acronym used to group associated output measurements, typically an expression like "LIMITS!{0}" where "{0}" is substituted with this adapter's name | No | LIMITS!{0} |
| `MeasurementAdder` | Defines the additive offset that should be used for newly created output measurements | No | 0.0 |
| `MeasurementMultiplier` | Defines the multiplicative offset that should be used for newly created output measurements | No | 1000000.0 |
| `MeasurementSignalType` | Defines the signal type that should be used for newly created output measurements | No | ALOG |
| `EnableImportLog` | Defines the flag that determines if log should be maintained for import operations | No | true |
| `ImportLogFilePath` | Defines the import log file name and optional path; exclude path to write to same location as `CSVFilePath` | No | ImportLog.txt |
| `ImportLogFileSize` | Defines the maximum file size of the import log in megabytes; value must be between 1 and 10 | No | 3 |
| `ImportLogFileFullOperation` | Defines the type of operation to be performed when the import log file is full | No | Truncate |

This adapter can work with importing multiple CSV values per row, here is an example based on adapter defaults:

| MeasurementPoint (0) | Quantity (1) | Units (2) | HighAlertEL (3) | HighWarningEL (4) | LowWarningEL (5) | LowAlertEL (6) | TimeGroupID (7) | ExportY/N (8) | PointNumber (9) | HighAlert (10) | HighWarning (11) | LowWarning (12)| LowAlert (13) | UNITS (14) |
|:----------------:|----------|:-----:|:-----------:|:-------------:|:------------:|:----------:|:-----------:|:----------:|:-----------:|:---------:|:----------:|:---------:|:--------:|:-----:|
| FF.TL77008 | ThreePhase.Power.Apparent | VA | 650000000 | 600000000 | -600000000 | -650000000 | Apparent | Y  |766201  |NaN | 912 | -912 | NaN | MVAC |
| GG.TL88016 | ThreePhase.Power.Reactive | Var | 650000000 | 600000000 | -600000000 | -650000000 | Reactive | Y | 766002 | NaN | 365 | -365 | NaN | MVAR |
| HH.TL99032 | ThreePhase.Power.Real  | W | 650000000 | 590000000 | -600000000 | -650000000 | Real | Y | 766003 | NaN | 912 | -912 | NaN | MW |


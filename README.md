# CSVLimitsLoader

### Grid Solutions Framework (GSF) Time-series Library (TSL) Adapter for Loading EMS Limits from a CSV File.

This adapter can be deployed with any application that is based on the [Grid Solutions Framework](https://github.com/GridProtectionAlliance/gsf) - [Time-Series Library](https://www.gridprotectionalliance.org/technology.asp#TSL). This includes products like the [openPDC](https://github.com/GridProtectionAlliance/openPDC), [SIEGate](https://github.com/GridProtectionAlliance/SIEGate) and the [openHistorian](https://github.com/GridProtectionAlliance/openHistorian).

To use the adapter, download the [release](releases/latest) associated with your product version and the unzip the assembly (`CSVLimitsLoader.dll`) into the product installation folder, e.g., `C:\Program Files\openPDC\`. Note that it may be necessary to [unblock](https://docs.microsoft.com/en-us/powershell/module/microsoft.powershell.utility/unblock-file?view=powershell-7.2) the DLL downloaded from the Internet before the TSL host application can use the adapter. It may be necessary to restart the host product after unblocking the assembly.

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
| CSVFilePath | Defines the path and file name of the CSV file to load | Yes | N/A |
| AutoCreateCSVPath | Defines the flag that determines if directory defined in CSVFilePath should be attempted to be created if it does not exist | No | False |

TODO: Add remaining properties...
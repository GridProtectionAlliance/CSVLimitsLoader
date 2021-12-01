//******************************************************************************************************
//  GlobalSettings.cs - Gbtc
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
//  11/30/2021 - J. Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************

using ExpressionEvaluator;
using GSF.ComponentModel;
using GSF.Configuration;
using GSF.Diagnostics;
using System;

namespace CSVLimitsLoader.Model
{
    // TODO: Remove class when reference to GSF is > 2.4.7 - code added for Device model to handle its own proxy settings
    internal class GlobalSettings
    {
        public string CompanyAcronym { get; } = s_companyAcronym;

        private readonly static string s_companyAcronym;

        static GlobalSettings()
        {
            try
            {
                CategorizedSettingsElementCollection systemSettings = ConfigurationFile.Current.Settings["systemSettings"];
                s_companyAcronym = systemSettings["CompanyAcronym"]?.Value;
            }
            catch (Exception ex)
            {
                Logger.SwallowException(ex, "Failed to initialize default company acronym");
            }

            if (string.IsNullOrWhiteSpace(s_companyAcronym))
                s_companyAcronym = "GPA";
        }

        public static void ValidateModelDependencies()
        {
            TypeRegistry registry = ValueExpressionParser.DefaultTypeRegistry;

            if (!registry.ContainsKey("Global"))
                registry.RegisterSymbol("Global", new GlobalSettings()); // Needed by modeled Device records
        }
    }
}

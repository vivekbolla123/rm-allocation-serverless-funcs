/**
 qp_dw_rmalloc.config_column_names
 */
[
  {
    "tableName": "config_sector_demand_multiplier",
    "columns": "DepartureDate,Sector,FlightNumber,Demand_Multiplier"
  },
  {
    "tableName": "config_quarter_wtp",
    "columns": "Sector,Month,Year,WTP_Factor"
  }
]

/**
 *qp_dw_rmalloc.config_inputs
 */
[
  {
    "tableName": "config_sector_demand_multiplier",
    "value": "config_sector_demand_multiplier",
    "title": "Additive Model Demand Multiplier",
    "view_permission": "view_demand_multiplier",
    "upload_permission": "update_demand_multiplier",
    "message": "Will drop and update full table.",
    "route": null,
    "order": 41
  },
  {
    "tableName": "config_quarter_wtp",
    "value": "config_quarter_wtp",
    "title": "Additive Model WTP",
    "view_permission": "view_wtp",
    "upload_permission": "update_wtp",
    "message": "Will drop and update full table.",
    "route": null,
    "order": 42
  }
]

/**
 * qp_rm_master.config_permissions
 */
[
  {
    "PermissionID": 167,
    "Permission": "view_demand_multiplier"
  },
  {
    "PermissionID": 168,
    "Permission": "update_demand_multiplier"
  },
  {
    "PermissionID": 169,
    "Permission": "view_wtp"
  },
  {
    "PermissionID": 170,
    "Permission": "update_wtp"
  }
]

[{
  "Role": "ADMIN",
  "PermissionID": "{\"role\": \"ADMIN\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170]}",
  "priority": 1
},
{
  "Role": "MANAGER",
  "PermissionID": "{\"role\": \"MANAGER\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 98, 100, 101, 102, 103, 104, 105, 107, 108, 109, 110, 106, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 131, 132, 133, 130, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170]}",
  "priority": 2
}]

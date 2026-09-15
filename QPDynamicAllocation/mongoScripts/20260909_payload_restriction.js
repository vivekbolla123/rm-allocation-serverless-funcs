[
  {
    "PermissionID": 174,
    "Permission": "view_payload_restriction"
  },
  {
    "PermissionID": 175,
    "Permission": "update_payload_restriction"
  },
  {
    "PermissionID": 176,
    "Permission": "trigger_payload_restriction"
  }
]

[{
  "tableName": "config_payload_restriction",
  "columns": "flightNumber,origin,destination,depStart,depEnd,lid,adjustedCapacity"
}]

[{
  "tableName": "config_payload_restriction",
  "value": "config_payload_restriction",
  "title": "Payload Restriction Config",
  "view_permission": "view_payload_restriction",
  "upload_permission": "update_payload_restriction",
  "message": "Will drop and update full table.",
  "route": null,
  "order": 44
}]
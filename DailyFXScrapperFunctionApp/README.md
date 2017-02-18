Configuration variables and parameters can be passed by creating a local appsettings.json in the same directory as this README.md

The appsettings.json should not be added to source control

Eg:

appsettings.json:

{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true;",
    "AzureWebJobsDashboard": "",
    "Monitoring:ClientId": "",
    "Monitoring:AppKey": "",
    "Monitoring:BackendAddress": "",
    "Monitoring:BackendAppIdUri": "",
    "InfluxDB:Host": "",
    "InfluxDB:Name": "",
    "InfluxDB:User": "",
    "InfluxDB:Password": ""
  }
}

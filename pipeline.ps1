$tenantId="8ca31555-269b-415f-9114-d4ff2f57659f"
$appId = "25250bc0-1bb5-460d-b158-339f080d6841"
$clientSecrets = "2Zs.J4z2v2vP-G7nr3-bsnRbQ6qBD-iJ2V"
$subscriptionId = "504fa83b-fd78-4bd4-ab0e-9b084d7e4982"
$resourceGroupName = "proyecto-opi-analytics-group"
$factoryName = "tortasTamal-df"
$apiVersion = "2018-06-01"

$AuthContext = [Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationContext]"https://login.microsoftonline.com/${tenantId}"
$cred = New-Object -TypeName Microsoft.IdentityModel.Clients.ActiveDirectory.ClientCredential -ArgumentList ($appId, $clientSecrets)
$result = $AuthContext.AcquireTokenAsync("https://management.core.windows.net/", $cred).GetAwaiter().GetResult()
$authHeader = @{
'Content-Type'='application/json'
'Accept'='application/json'
'Authorization'=$result.CreateAuthorizationHeader()
}

$body = '{
  "country":mx
  ],
  "period":20200801
  ]
}'
$request = "https://management.azure.com/subscriptions/${subscriptionId}/resourceGroups/${resourceGroupName}/providers/Microsoft.DataFactory/factories/${factoryName}/pipelines/pipeline-last/createRun?api-version=${apiVersion}"
$response = Invoke-RestMethod -Method POST -Uri $request -Header $authHeader -Body $body
$response | ConvertTo-Json
$runId = $response.runId

$request = "https://management.azure.com/subscriptions/${subscriptionId}/resourceGroups/${resourceGroupName}/providers/Microsoft.DataFactory/factories/${factoryName}/pipelineruns/${runId}?api-version=${apiVersion}"
while ($True) {
    $response = Invoke-RestMethod -Method GET -Uri $request -Header $authHeader
    Write-Host  "Pipeline run status: " $response.Status -foregroundcolor "Yellow"

    if ($response.Status -eq "InProgress") {
        Start-Sleep -Seconds 15
    }
    else {
        $response | ConvertTo-Json
        break
    }
}
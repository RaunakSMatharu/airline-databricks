Param(
[Parameter(Mandatory=$False,Position=1)]
[object] $WEBHOOKDATA
)

$input=ConvertFrom-JSON -InputObject $WEBHOOKDATA.RequestBody
Write-Output $input


$azcontext=(Connect-AzAccount -Identity).context
$azcontext1=Set-AzContext -SubscriptionName $azcontext.Subscription -DefaultProfile  $azcontext
$status=Get-AzSQLDatabase -ResourceGroupName 'raunak_source' -ServerName 'raunaksdev' -DatabaseName 'raunaktsdw'

if($input.param -eq'Start' -and $status.Status -eq 'Online'  )
{

   Write-Output "DW is already online" 
}
elseif($input.param -eq'Start' -and $status.Status -eq 'Paused')
{
 
  Resume-AzSQLDatabase -ResourceGroupName 'raunak_source' -ServerName 'raunakdev' -DatabaseName 'raunaktsdw'

}
elseif($input.param -eq 'Stop' -and $status.Status -eq 'Online')
{
 
  Suspend-AzSQLDatabase -ResourceGroupName 'raunak_source' -ServerName 'raunakdev' -DatabaseName 'raunaktsdw'

}



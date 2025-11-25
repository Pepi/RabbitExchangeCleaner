# Imposta le directory
$dirA = "C:\Users\p.taliani\Sviluppo\Programmi\RabbitExchangeCleaner"
$dirB = "C:\Users\p.taliani\Sviluppo\Works\Varie\c#\RabbitMQ\RabbitExchangeCleaner\bin\Release\net9.0\win-x64"

# Ottieni i file da entrambe le directory
$filesA = Get-ChildItem -Path $dirA -File
$filesB = Get-ChildItem -Path $dirB -File

foreach ($fileA in $filesA) {
    # Cerca il file corrispondente in B
    $fileB = $filesB | Where-Object { $_.Name -eq $fileA.Name }
    
    if ($fileB) {
        # Confronta le date di modifica
        if ($fileB.LastWriteTime -gt $fileA.LastWriteTime) {
            Write-Host "Copio $($fileB.Name) da B ad A"
            Copy-Item -Path $fileB.FullName -Destination $fileA.FullName -Force
        }
    }
}

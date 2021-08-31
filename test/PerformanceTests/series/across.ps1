﻿#!/usr/bin/pwsh
param (
	$ResultsFile="./results.csv",
	$PrintColumnNames=$true,
	$tu=20
)

# read the settings that are common to all scripts
. ./settings.ps1

if ($PrintColumnNames)
{
	Add-Content -path $ResultsFile -value "plan,nodes,tag,test,tu,starttime,iteration,size,duration"
}

./series/runsingle -Tag neth     -HubName AC0a -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration FileHash -Data 5000 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runsingle -Tag neth-loc -HubName AC0b -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration FileHash -Data 5000 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runsingle -Tag azst-12  -HubName AC0c -Plan EP2 -NumNodes 6 -WaitForDeploy 50 -Orchestration FileHash -Data 5000 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits 1

./series/runmany -Tag neth-loc   -HubName AC2b -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 80 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runmany -Tag neth       -HubName AC2a -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 80 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runmany -Tag azst-12    -HubName AC2c -Plan EP2 -NumNodes 6 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 2000 -PortionSize 0 -DelayAfterRun 120 -ResultsFile $ResultsFile -ThroughputUnits 1

./series/runmany -Tag neth-loc   -HubName AC7b -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 200 -PortionSize 0 -DelayAfterRun 120 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runmany -Tag neth       -HubName AC7a -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 200 -PortionSize 0 -DelayAfterRun 120 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runmany -Tag azst-12    -HubName AC7c -Plan EP2 -NumNodes 6 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 30 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1

./series/runmany -Tag neth-loc   -HubName AC4b -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration HelloSequence5 -NumOrchestrations 10000 -PortionSize 200 -DelayAfterRun 80 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runmany -Tag neth       -HubName AC4a -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration HelloSequence5 -NumOrchestrations 10000 -PortionSize 200 -DelayAfterRun 80 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runmany -Tag azst-12    -HubName AC4c -Plan EP2 -NumNodes 6 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 200 -DelayAfterRun 180 -ResultsFile $ResultsFile -ThroughputUnits 1

./series/runsingle -Tag neth     -HubName AC5a -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration "WordCount?shape=10x30" -Data 10 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runsingle -Tag neth-loc -HubName AC5b -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration "WordCount?shape=10x30" -Data 10 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runsingle -Tag azst-12  -HubName AC5c -Plan EP2 -NumNodes 6 -WaitForDeploy 50 -Orchestration "WordCount?shape=10x30" -Data 10 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu

./series/runsingle -Tag neth-loc -HubName AC3b -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 400 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runsingle -Tag neth     -HubName AC3a -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 400 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runsingle -Tag azst-12  -HubName AC3c -Plan EP2 -NumNodes 6 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 400 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits 1

./series/runsingle -Tag neth-loc -HubName AC6b -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration CollisionSearch/flat-parallel -Data 400 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runsingle -Tag neth     -HubName AC6a -Plan EP2 -NumNodes 6 -WaitForDeploy 80 -Orchestration CollisionSearch/flat-parallel -Data 400 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runsingle -Tag azst-12  -HubName AC6c -Plan EP2 -NumNodes 6 -WaitForDeploy 50 -Orchestration CollisionSearch/flat-parallel -Data 400 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits 1 -DeleteAfterTests $true


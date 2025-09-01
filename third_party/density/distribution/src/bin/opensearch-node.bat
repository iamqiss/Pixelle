@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set DENSITY_MAIN_CLASS=org.density.cluster.coordination.NodeToolCli
call "%~dp0density-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%

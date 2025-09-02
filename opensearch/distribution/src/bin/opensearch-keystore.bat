@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set DENSITY_MAIN_CLASS=org.density.tools.cli.keystore.KeyStoreCli
set DENSITY_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/keystore-cli
call "%~dp0density-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%

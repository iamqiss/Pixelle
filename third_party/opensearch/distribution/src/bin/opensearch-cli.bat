call "%~dp0density-env.bat" || exit /b 1

if defined DENSITY_ADDITIONAL_SOURCES (
  for %%a in ("%DENSITY_ADDITIONAL_SOURCES:;=","%") do (
    call "%~dp0%%a"
  )
)

if defined DENSITY_ADDITIONAL_CLASSPATH_DIRECTORIES (
  for %%a in ("%DENSITY_ADDITIONAL_CLASSPATH_DIRECTORIES:;=","%") do (
    set DENSITY_CLASSPATH=!DENSITY_CLASSPATH!;!DENSITY_HOME!/%%a/*
  )
)

rem use a small heap size for the CLI tools, and thus the serial collector to
rem avoid stealing many CPU cycles; a user can override by setting DENSITY_JAVA_OPTS
set DENSITY_JAVA_OPTS=-Xms4m -Xmx64m -XX:+UseSerialGC %DENSITY_JAVA_OPTS%

"%JAVA%" ^
  %DENSITY_JAVA_OPTS% ^
  -Ddensity.path.home="%DENSITY_HOME%" ^
  -Ddensity.path.conf="%DENSITY_PATH_CONF%" ^
  -Ddensity.distribution.type="%DENSITY_DISTRIBUTION_TYPE%" ^
  -cp "%DENSITY_CLASSPATH%" ^
  "%DENSITY_MAIN_CLASS%" ^
  %*

exit /b %ERRORLEVEL%

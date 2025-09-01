set SCRIPT=%0

rem determine Density home; to do this, we strip from the path until we
rem find bin, and then strip bin (there is an assumption here that there is no
rem nested directory under bin also named bin)
if not defined DENSITY_HOME goto density_home_start_setup
goto density_home_done_setup

:density_home_start_setup
for %%I in (%SCRIPT%) do set DENSITY_HOME=%%~dpI

:density_home_loop
for %%I in ("%DENSITY_HOME:~1,-1%") do set DIRNAME=%%~nxI
if not "%DIRNAME%" == "bin" (
  for %%I in ("%DENSITY_HOME%..") do set DENSITY_HOME=%%~dpfI
  goto density_home_loop
)
for %%I in ("%DENSITY_HOME%..") do set DENSITY_HOME=%%~dpfI

:density_home_done_setup
rem now set the classpath
set DENSITY_CLASSPATH=!DENSITY_HOME!\lib\*

set HOSTNAME=%COMPUTERNAME%

if not defined DENSITY_PATH_CONF (
  set DENSITY_PATH_CONF=!DENSITY_HOME!\config
)

rem now make DENSITY_PATH_CONF absolute
for %%I in ("%DENSITY_PATH_CONF%..") do set DENSITY_PATH_CONF=%%~dpfI

rem Check if any bc-fips jar exists on classpath
rem run in FIPS JVM if jar is found
set "FOUND_BC_FIPS="
if exist "%DENSITY_HOME%\lib\bc-fips*.jar" (
    echo BouncyCastle FIPS library found, setting FIPS JVM options.
    set DENSITY_JAVA_OPTS=-Dorg.bouncycastle.fips.approved_only=true -Djava.security.properties="%DENSITY_PATH_CONF%\fips_java.security" %DENSITY_JAVA_OPTS%
)

set DENSITY_DISTRIBUTION_TYPE=${density.distribution.type}
set DENSITY_BUNDLED_JDK=${density.bundled_jdk}

if "%DENSITY_BUNDLED_JDK%" == "false" (
  echo "warning: no-jdk distributions that do not bundle a JDK are deprecated and will be removed in a future release" >&2
)

cd /d "%DENSITY_HOME%"

rem now set the path to java, pass "nojava" arg to skip setting JAVA_HOME and JAVA
if "%1" == "nojava" (
   exit /b
)

rem comparing to empty string makes this equivalent to bash -v check on env var
rem and allows to effectively force use of the bundled jdk when launching Density
rem by setting DENSITY_JAVA_HOME= and JAVA_HOME= 
if not "%DENSITY_JAVA_HOME%" == "" (
  set "JAVA=%DENSITY_JAVA_HOME%\bin\java.exe"
  set JAVA_TYPE=DENSITY_JAVA_HOME 
) else if not "%JAVA_HOME%" == "" (
  set "JAVA=%JAVA_HOME%\bin\java.exe"
  set JAVA_TYPE=JAVA_HOME
) else (
  set "JAVA=%DENSITY_HOME%\jdk\bin\java.exe"
  set "JAVA_HOME=%DENSITY_HOME%\jdk"
  set JAVA_TYPE=bundled jdk
)

if not exist !JAVA! (
  echo "could not find java in !JAVA_TYPE! at !JAVA!" >&2
  exit /b 1
)

rem do not let JAVA_TOOL_OPTIONS slip in (as the JVM does by default)
if defined JAVA_TOOL_OPTIONS (
  echo warning: ignoring JAVA_TOOL_OPTIONS=%JAVA_TOOL_OPTIONS%
  set JAVA_TOOL_OPTIONS=
)

rem JAVA_OPTS is not a built-in JVM mechanism but some people think it is so we
rem warn them that we are not observing the value of %JAVA_OPTS%
if defined JAVA_OPTS (
  (echo|set /p=warning: ignoring JAVA_OPTS=%JAVA_OPTS%; )
  echo pass JVM parameters via DENSITY_JAVA_OPTS
)

rem check the Java version
"%JAVA%" -cp "%DENSITY_CLASSPATH%" "org.density.tools.java_version_checker.JavaVersionChecker" || exit /b 1

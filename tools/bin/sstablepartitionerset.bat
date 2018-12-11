@REM  (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
@REM
@REM  Licensed under the Apache License, Version 2.0 (the "License");
@REM  you may not use this file except in compliance with the License.
@REM  You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.

@echo off

if "%OS%" == "Windows_NT" setlocal

pushd "%~dp0"
call cassandra.in.bat

if NOT DEFINED JAVA_HOME goto :err
set TOOL_MAIN=org.apache.cassandra.tools.SSTablePartitionerSetter

REM ***** JAVA options *****
set JAVA_OPTS=^
 -Dlogback.configurationFile=logback-tools.xml

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %CASSANDRA_PARAMS% -cp %CLASSPATH% %TOOL_MAIN% %*
goto finally

:err
echo JAVA_HOME environment variable must be set!
set ERRORLEVEL=1
pause

:finally
ENDLOCAL & set RC=%ERRORLEVEL%
goto :exit_with_code

:returncode
exit /B %RC%

:exit_with_code
call :returncode %RC%

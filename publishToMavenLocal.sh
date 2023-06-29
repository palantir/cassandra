./gradlew --stop
git clean -fdx
ant clean
./gradlew clean
ant
ant generate-idea-files
ant maven-ant-tasks-retrieve-build
./gradlew --no-daemon
ant build
./gradlew --no-daemon ant-artifacts
./gradlew ant-sources-jar
./gradlew publishToMavenLocal

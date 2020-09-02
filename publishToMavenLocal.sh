./gradlew --stop
git clean -fdx
ant clean
./gradlew clean
ant
ant generate-idea-files
ant maven-ant-tasks-retrieve-build
./gradlew --no-daemon
ant build build-test
./gradlew --no-daemon ant-artifacts
./gradlew publishToMavenLocal

1. To set up IntelliJ environment. Download source code (not the ones with Hadoop), extract and build with the following command:
build/mvn -DskipTests clean package

Once built. In the Project Structure -> Libraries
Click `+`
Choose Java
Import Spark 2.1.0/assembly/target/scala-2.11/jars
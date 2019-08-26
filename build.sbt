name := "STreamingWithHbase"

version := "1.6.0"

scalaVersion in ThisBuild := "2.10.5"

val sparkVersion = "1.6.0"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"

dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.6.7.1"

libraryDependencies ++= Seq(
"org.elasticsearch.client" % "transport" % "5.5.2",
"org.elasticsearch" % "elasticsearch" % "5.5.2"
)

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.24"

unmanagedJars in Compile += file("lib/shc-core-1.1.1-1.6-s_2.10.jar")

libraryDependencies ++= Seq(
   "org.apache.spark" %% "spark-core" % "1.6.0",
   "org.apache.spark" %% "spark-streaming" % "1.6.0",
   //"org.apache.kafka" % "kafka-streams" % "1.0.2",
   "org.apache.kafka" %% "kafka" % "0.8.0",
   "org.apache.kafka" % "kafka-clients" % "0.8.2.0",
   "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0",
   "org.apache.spark" %% "spark-sql" % "1.6.0",
   "org.apache.spark" %% "spark-hive" % "1.6.0",
   "org.elasticsearch" %% "elasticsearch-spark-13" % "5.5.2",
   "ch.qos.logback" % "logback-classic" % "1.0.9",
   "org.apache.hadoop" % "hadoop-common" % "2.6.0",
   "org.apache.hbase" % "hbase-server" % "1.2.0",
   "org.apache.hbase" % "hbase-client" % "1.2.0",
   "org.apache.hbase" % "hbase-common" % "1.2.0"
 )
 
 resolvers ++= Seq(
  "Typesafe" at "http://repo.hortonworks.com/content/groups/public/"
//  "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
)

fork in run := true

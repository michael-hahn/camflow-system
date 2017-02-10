spark:
	mkdir -p dependencies
	cd ./dependencies	&& git clone git://github.com/apache/spark.git -b branch-2.1
	cd ./dependencies/spark && build/mvn -DskipTests clean package

bahir:
	mkdir -p dependencies
	cd ./dependencies && wget http://www-us.apache.org/dist/bahir/2.0.2/apache-bahir-2.0.2-src.tar.gz
	cd ./dependencies && tar -xvzf apache-bahir-2.0.2-src.tar.gz
	cd ./dependencies/apache-bahir-2.0.2-src && mvn -DskipTests clean package

json:
	mkdir -p dependencies
	cd ./dependencies && git clone https://github.com/tdunning/open-json
	cd ./dependencies/open-json && mvn -DskipTests clean package

mqtt:
	mkdir -p dependencies
	cd ./dependencies && git clone https://github.com/eclipse/paho.mqtt.java.git
	cd ./dependencies/paho.mqtt.java && git checkout -b tags/v1.1.0
	cd ./dependencies/paho.mqtt.java && mvn -DskipTests clean package

dependencies: spark bahir json mqtt

clean:
	rm -rf dependencies

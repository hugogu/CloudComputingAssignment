echo "Building $1.java"
rm -rvf ./build/* ./$1.jar
mkdir -vp build
export HADOOP_CLASS=$JAVA_HOME/lib/tools.jar
hadoop com.sun.tools.javac.Main $1.java -d build
jar -cvf $1.jar -C build/ ./
hadoop fs -rm -r -f /mp2/$2-output

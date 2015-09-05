echo "Building and Running $1.java"
rm -rvf ./build/* ./$1.jar
mkdir -vp build
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
hadoop com.sun.tools.javac.Main $1.java -d build
jar -cvf $1.jar -C build/ ./
hadoop fs -rm -r -f /mp2/$2-output
hadoop jar $1.jar $1 -D stopwords=/mp2/misc/stopwords.txt -D delimiters=/mp2/misc/delimiters.txt /mp2/titles /mp2/$2-output

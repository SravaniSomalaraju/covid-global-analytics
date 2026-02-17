#!/bin/bash

echo "===== COVID Analytics Environment Setup ====="

echo "============================================="

echo "===== Checking Java... ====="

if command -v java &> /dev/null
then
    echo "Java already installed"
    java -version

    # Detect JAVA_HOME automatically
    JAVA_PATH=$(readlink -f $(which java) | sed "s:/bin/java::")

    echo "Detected JAVA_HOME=$JAVA_PATH"

    if ! grep -q "JAVA_HOME" ~/.bashrc; then
        echo "export JAVA_HOME=$JAVA_PATH" >> ~/.bashrc
        echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
    fi

else
    echo "Java not found. Installing OpenJDK 17..."
    sudo apt install openjdk-17-jdk -y

    JAVA_PATH=$(readlink -f $(which java) | sed "s:/bin/java::")

    echo "export JAVA_HOME=$JAVA_PATH" >> ~/.bashrc
    echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
fi

echo " =============================================="
echo "===== Checking Hadoop... ====="

if command -v hadoop &> /dev/null
then
    echo "Hadoop already installed"
    hadoop version
else
    echo "Installing Hadoop..."

    cd ~
    wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
    tar -xzf hadoop-3.3.6.tar.gz
    mv hadoop-3.3.6 hadoop

    if ! grep -q "HADOOP_HOME" ~/.bashrc; then
        echo 'export HADOOP_HOME=$HOME/hadoop' >> ~/.bashrc
        echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
    fi
fi


echo " ============================================="
echo "Checking Spark..."

if command -v spark-submit &> /dev/null
then
    echo "Spark already installed"
    spark-submit --version

    # Detect SPARK_HOME automatically
    SPARK_PATH=$(dirname $(dirname $(readlink -f $(which spark-submit))))

    echo "Detected SPARK_HOME=$SPARK_PATH"

    if ! grep -q "SPARK_HOME" ~/.bashrc; then
        echo "export SPARK_HOME=$SPARK_PATH" >> ~/.bashrc
        echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
    fi

else
    echo "Spark not found. Installing Spark..."

    cd ~
    wget https://downloads.apache.org/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3.tgz
    tar -xzf spark-4.1.1-bin-hadoop3.tgz
    mv spark-4.1.1-bin-hadoop3 spark

    echo 'export SPARK_HOME=$HOME/spark' >> ~/.bashrc
    echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
fi

echo " ============================================"
echo "=====Checking PySpark...====="

if command -v pyspark &> /dev/null
then
    echo "PySpark already installed"
else
    echo "Installing PySpark..."
    pip3 install pyspark
fi

echo "============================================="
echo "=====Applying environment variables... ====="
source ~/.bashrc

echo "============================================"
echo "===== VERIFYING ====="

echo "Java:"
java -version

echo ""
echo "Hadoop:"
hadoop version

echo ""
echo "Spark:"
spark-submit --version

echo ""
echo "PySpark:"
pyspark --version

echo ""
echo "===== SETUP COMPLETED SUCCESSFULLY ====="
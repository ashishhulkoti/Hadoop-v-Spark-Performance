sudo lxc launch ubuntu:22.04 namenode --vm -c limits.cpu=4 -c limits.memory=4GiB -d root,size=10GiB

sudo lxc launch ubuntu:22.04 datanode1 --vm -c limits.cpu=24 -c limits.memory=24GiB -d root,size=260GiB

sudo lxc exec namenode -- ssh-keygen -t rsa -P ''

sudo lxc file pull namenode/root/.ssh/id_rsa.pub ./id_rsa.pub
sudo lxc file push ./id_rsa.pub datanode1/root/.ssh/authorized_keys
sudo lxc file pull namenode/root/.ssh/id_rsa.pub ./id_rsa.pub
sudo lxc file push ./id_rsa.pub namenode/root/.ssh/authorized_keys

sudo lxc exec namenode -- sudo apt update
sudo lxc exec namenode -- sudo apt install -y openjdk-11-jdk
sudo lxc exec namenode -- wget https://downloads.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz
sudo lxc exec namenode -- tar -xzf hadoop-3.4.0.tar.gz
sudo lxc exec namenode -- sudo mv hadoop-3.4.0 /usr/local/hadoop
 


for i in 1
do
    sudo lxc exec datanode$i -- sudo apt update
    sudo lxc exec datanode$i -- sudo apt install -y openjdk-11-jdk
    sudo lxc exec datanode$i -- wget https://downloads.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz
    sudo lxc exec datanode$i -- tar -xzf hadoop-3.4.0.tar.gz
    sudo lxc exec datanode$i -- sudo mv hadoop-3.4.0 /usr/local/hadoop
done

for vmname in namenode datanode1
do
    sudo lxc exec $vmname -- bash -c "echo 'export HADOOP_HOME=/usr/local/hadoop' >> ~/.bashrc"
    sudo lxc exec $vmname -- bash -c "echo 'export JAVA_HOME=\$(readlink -f /usr/bin/java | sed "s:/bin/java::")' >> ~/.bashrc"
    sudo lxc exec $vmname -- bash -c "echo 'export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$JAVA_HOME/bin' >> ~/.bashrc"
    sudo lxc exec $vmname -- bash -c "echo 'export HADOOP_MAPRED_HOME=\$HADOOP_HOME' >> ~/.bashrc"
    sudo lxc exec $vmname -- bash -c "echo 'export HADOOP_COMMON_HOME=\$HADOOP_HOME' >> ~/.bashrc"
    sudo lxc exec $vmname -- bash -c "echo 'export HADOOP_HDFS_HOME=\$HADOOP_HOME' >> ~/.bashrc"
    sudo lxc exec $vmname -- bash -c "echo 'export YARN_HOME=\$HADOOP_HOME' >> ~/.bashrc"
    sudo lxc exec $vmname -- bash -c "echo 'export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop' >> ~/.bashrc"
    sudo lxc exec $vmname -- bash -c "source .bashrc"
done



sudo lxc exec namenode -- bash -c ' echo "export JAVA_HOME=/usr" >> /usr/local/hadoop/etc/hadoop/hadoop-env.sh '

for i in 1
do
    sudo lxc exec datanode$i -- bash -c ' echo "export JAVA_HOME=/usr" >> /usr/local/hadoop/etc/hadoop/hadoop-env.sh '
done


sudo lxc exec namenode -- bash -c ' echo -e "HDFS_NAMENODE_USER="root"\nHDFS_DATANODE_USER="root"\nHDFS_SECONDARYNAMENODE_USER="root"\nYARN_RESOURCEMANAGER_USER="root"\nYARN_NODEMANAGER_USER=root" >> /etc/environment '


for i in 1
do
    sudo lxc exec datanode$i -- bash -c ' echo -e "HDFS_NAMENODE_USER="root"\nHDFS_DATANODE_USER="root"\nHDFS_SECONDARYNAMENODE_USER="root"\nYARN_RESOURCEMANAGER_USER="root"\nYARN_NODEMANAGER_USER=root" >> /etc/environment '
done



sudo lxc exec namenode -- sed -i '/<configuration>/,/<\/configuration>/c\
<configuration>\
  <property>\
    <name>fs.defaultFS</name>\
    <value>hdfs:\/\/namenode:9000<\/value>\
  <\/property>\
  <property>\
    <name>hadoop.tmp.dir</name>\
    <value>\/var\/hadoop\/tmp<\/value>\
  <\/property>\
<\/configuration>' "/usr/local/hadoop/etc/hadoop/core-site.xml"

for i in 1
do
    sudo lxc exec datanode$i -- sed -i '/<configuration>/,/<\/configuration>/c\
    <configuration>\
    <property>\
        <name>fs.defaultFS</name>\
        <value>hdfs:\/\/namenode:9000<\/value>\
    <\/property>\
    <property>\
    <name>hadoop.tmp.dir</name>\
    <value>\/var\/hadoop\/tmp<\/value>\
  <\/property>\
    <\/configuration>' "/usr/local/hadoop/etc/hadoop/core-site.xml"
done


sudo lxc exec namenode -- sed -i '/<configuration>/,/<\/configuration>/c\
  <configuration>\
    <property>\
		<name>dfs.blocksize</name>\
		<value>134217728<\/value>\
	<\/property>\
    <property>\
		<name>dfs.namenode.handler.count</name>\
		<value>100<\/value>\
	<\/property>\
    <property>\
        <name>dfs.replication</name>\
        <value>1<\/value>\
    <\/property>\
    <property>\
        <name>dfs.namenode.name.dir</name>\
        <value>file:\/\/\/usr\/local\/hadoop\/data\/namenode<\/value>\
    <\/property>\
  <\/configuration>' "/usr/local/hadoop/etc/hadoop/hdfs-site.xml"

for i in 1
do
    sudo lxc exec datanode$i -- sed -i '/<configuration>/,/<\/configuration>/c\
    <configuration>\
    <property>\
		<name>dfs.blocksize</name>\
		<value>134217728<\/value>\
	<\/property>\
    <property>\
        <name>dfs.replication</name>\
        <value>1<\/value>\
    <\/property>\
    <property>\
        <name>dfs.datanode.data.dir</name>\
        <value>file:\/\/\/usr\/local\/hadoop\/data\/datanode<\/value>\
    <\/property>\
    <\/configuration>' "/usr/local/hadoop/etc/hadoop/hdfs-site.xml"
done


    sudo lxc exec namenode -- sed -i '/<configuration>/,/<\/configuration>/c\
    <configuration>\
    <property>\
        <name>mapreduce.framework.name</name>\
        <value>yarn<\/value>\
    <\/property>\
    <property>\
        <name>yarn.app.mapreduce.am.env</name>\
        <value>HADOOP_MAPRED_HOME=\/usr\/local\/hadoop<\/value>\
    <\/property>\
    <property>\
        <name>mapreduce.map.env</name>\
        <value>HADOOP_MAPRED_HOME=\/usr\/local\/hadoop<\/value>\
    <\/property>\
    <property>\
        <name>mapreduce.reduce.env</name>\
        <value>HADOOP_MAPRED_HOME=\/usr\/local\/hadoop<\/value>\
    <\/property>\
        <property>\
        <name>mapreduce.map.speculative</name>\
        <value>true<\/value>\
        <\/property>\
    <property>\
            <name>mapreduce.map.memory.mb</name>\
            <value>768<\/value>\
        <\/property>\
        <property>\
            <name>mapreduce.map.java.opts</name>\
            <value>-Xmx614m<\/value>\
        <\/property>\
        <property>\
            <name>mapreduce.reduce.memory.mb</name>\
            <value>1536<\/value>\
        <\/property>\
        <property>\
            <name>mapreduce.reduce.java.opts</name>\
            <value>-Xmx1229m<\/value>\
        <\/property>\
        <property>\
            <name>mapreduce.job.reduce.slowstart.completedmaps</name>\
            <value>0.6<\/value>\
        <\/property>\
    <\/configuration>' "/usr/local/hadoop/etc/hadoop/mapred-site.xml"



    for i in 1
    do
        sudo lxc exec datanode$i -- sed -i '/<configuration>/,/<\/configuration>/c\
        <configuration>\
        <property>\
            <name>mapreduce.framework.name</name>\
            <value>yarn<\/value>\
        <\/property>\
        <property>\
            <name>yarn.app.mapreduce.am.env</name>\
            <value>HADOOP_MAPRED_HOME=\/usr\/local\/hadoop<\/value>\
        <\/property>\
        <property>\
            <name>mapreduce.map.env</name>\
            <value>HADOOP_MAPRED_HOME=\/usr\/local\/hadoop<\/value>\
        <\/property>\
        <property>\
            <name>mapreduce.reduce.env</name>\
            <value>HADOOP_MAPRED_HOME=\/usr\/local\/hadoop<\/value>\
        <\/property>\
        <property>\
        <name>mapreduce.map.speculative</name>\
        <value>true<\/value>\
        <\/property>\
        <property>\
            <name>mapreduce.map.memory.mb</name>\
            <value>768<\/value>\
        <\/property>\
        <property>\
            <name>mapreduce.map.java.opts</name>\
            <value>-Xmx614m<\/value>\
        <\/property>\
        <property>\
            <name>mapreduce.reduce.memory.mb</name>\
            <value>1536<\/value>\
        <\/property>\
        <property>\
            <name>mapreduce.reduce.java.opts</name>\
            <value>-Xmx1229m<\/value>\
        <\/property>\
        <property>\
            <name>mapreduce.job.reduce.slowstart.completedmaps</name>\
            <value>0.6<\/value>\
        <\/property>\
        <\/configuration>' "/usr/local/hadoop/etc/hadoop/mapred-site.xml"
    done


sudo lxc exec namenode -- sed -i '/<configuration>/,/<\/configuration>/c\
<configuration>\
  <property>\
    <name>yarn.nodemanager.aux-services</name>\
    <value>mapreduce_shuffle<\/value>\
  <\/property>\
  <property>\
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>\
    <value>org.apache.hadoop.mapred.ShuffleHandler<\/value>\
  <\/property>\
  <property>\
    <name>yarn.resourcemanager.hostname</name>\
    <value>namenode<\/value>\
  <\/property>\
  <property>\
    <name>yarn.nodemanager.resource.memory-mb</name>\
    <value>2048<\/value>\
    <\/property>\
    <property>\
    <name>yarn.scheduler.minimum-allocation-mb</name>\
    <value>256<\/value>\
    <\/property>\
    <property>\
    <name>yarn.scheduler.maximum-allocation-mb</name>\
    <value>1536<\/value>\
<\/property>\
<\/configuration>' "/usr/local/hadoop/etc/hadoop/yarn-site.xml"

for i in 1
do
    sudo lxc exec datanode$i -- sed -i '/<configuration>/,/<\/configuration>/c\
    <configuration>\
    <property>\
        <name>yarn.nodemanager.aux-services</name>\
        <value>mapreduce_shuffle<\/value>\
    <\/property>\
    <property>\
        <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>\
        <value>org.apache.hadoop.mapred.ShuffleHandler<\/value>\
    <\/property>\
    <property>\
        <name>yarn.resourcemanager.hostname</name>\
        <value>namenode<\/value>\
    <\/property>\
    <property>\
        <name>yarn.nodemanager.resource.memory-mb</name>\
        <value>2048<\/value>\
        <\/property>\
        <property>\
        <name>yarn.scheduler.minimum-allocation-mb</name>\
        <value>256<\/value>\
        <\/property>\
        <property>\
        <name>yarn.scheduler.maximum-allocation-mb</name>\
        <value>1536<\/value>\
    <\/property>\
    <\/configuration>' "/usr/local/hadoop/etc/hadoop/yarn-site.xml"

done



for vmname in namenode datanode1
do
    sudo lxc exec namenode -- ssh -o StrictHostKeyChecking=no $vmname hostname
done

for vmname in namenode datanode1
do
    sudo lxc exec $vmname -- bash -c 'echo -e "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64\nexport HADOOP_HOME=/usr/local/hadoop\nexport PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin" >> .bashrc'
    sudo lxc exec $vmname -- bash -c 'source .bashrc'
done

sudo lxc exec namenode -- sudo reboot
for i in 1
do
    sudo lxc exec datanode$i -- sudo reboot
done


sudo /usr/local/hadoop/bin/hdfs namenode -format



hdfs --daemon start namenode && hdfs --daemon start secondarynamenode && yarn --daemon start resourcemanager

hdfs --daemon start datanode && yarn --daemon start nodemanager



hdfs --daemon stop namenode && hdfs --daemon stop secondarynamenode && yarn --daemon stop resourcemanager

hdfs --daemon stop datanode && yarn --daemon stop nodemanager









sudo lxc file push /home/cc/a5/6cluster/HadoopSort.jar namenode/root/
sudo lxc file push /home/cc/a5/6cluster/build namenode/root/
sudo lxc file push /home/cc/a5/6cluster/src namenode/root/



sudo lxc exec namenode -- hadoop jar HadoopSort.jar HadoopSort /user/root/input /user/root/output 268435456 16





javac -classpath "$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/common/lib/*" -d build/ src/*.java
jar -cvf HadoopSort.jar -C build/ .
hadoop jar HadoopSort.jar HadoopSort /user/root/input /user/root/output 2 8











sudo lxc exec namenode -- sed -i '/<configuration>/,/<\/configuration>/c\
<configuration>\
  <property>\
    <name>fs.defaultFS</name>\
    <value>hdfs:\/\/namenode:9000<\/value>\
  <\/property>\
  <property>\
    <name>hadoop.tmp.dir</name>\
    <value>\/var\/hadoop\/tmp<\/value>\
  <\/property>\
<\/configuration>' "/usr/local/hadoop/etc/hadoop/core-site.xml"

for i in 1
do
    sudo lxc exec datanode$i -- sed -i '/<configuration>/,/<\/configuration>/c\
    <configuration>\
    <property>\
        <name>fs.defaultFS</name>\
        <value>hdfs:\/\/namenode:9000<\/value>\
    <\/property>\
    <property>\
    <name>hadoop.tmp.dir</name>\
    <value>\/var\/hadoop\/tmp<\/value>\
  <\/property>\
    <\/configuration>' "/usr/local/hadoop/etc/hadoop/core-site.xml"
done




##hdfs-site.xml

sudo lxc exec namenode -- sed -i '/<configuration>/,/<\/configuration>/c\
  <configuration>\
    <property>\
		<name>dfs.namenode.handler.count</name>\
		<value>100<\/value>\
	<\/property>\
    <property>\
        <name>dfs.replication</name>\
        <value>1<\/value>\
    <\/property>\
    <property>\
        <name>dfs.namenode.name.dir</name>\
        <value>file:\/\/\/usr\/local\/hadoop\/data\/namenode<\/value>\
    <\/property>\
  <\/configuration>' "/usr/local/hadoop/etc/hadoop/hdfs-site.xml"

for i in 1
do
    sudo lxc exec datanode$i -- sed -i '/<configuration>/,/<\/configuration>/c\
    <configuration>\
    <property>\
		<name>dfs.blocksize</name>\
		<value>134217728<\/value>\
	<\/property>\
    <property>\
        <name>dfs.replication</name>\
        <value>1<\/value>\
    <\/property>\
    <property>\
        <name>dfs.datanode.data.dir</name>\
        <value>file:\/\/\/usr\/local\/hadoop\/data\/datanode<\/value>\
    <\/property>\
    <\/configuration>' "/usr/local/hadoop/etc/hadoop/hdfs-site.xml"
done




##mapred-site.xml
sudo lxc exec namenode -- sed -i '/<configuration>/,/<\/configuration>/c\
<configuration>\
  <property>\
    <name>mapreduce.framework.name</name>\
    <value>yarn<\/value>\
  <\/property>\
  <property>\
    <name>yarn.app.mapreduce.am.env</name>\
    <value>HADOOP_MAPRED_HOME=\/usr\/local\/hadoop<\/value>\
  <\/property>\
  <property>\
    <name>mapreduce.map.env</name>\
    <value>HADOOP_MAPRED_HOME=\/usr\/local\/hadoop<\/value>\
  <\/property>\
  <property>\
    <name>mapreduce.reduce.env</name>\
    <value>HADOOP_MAPRED_HOME=\/usr\/local\/hadoop<\/value>\
  <\/property>\
<\/configuration>' "/usr/local/hadoop/etc/hadoop/mapred-site.xml"



for i in 1
do
    sudo lxc exec datanode$i -- sed -i '/<configuration>/,/<\/configuration>/c\
    <configuration>\
    <property>\
        <name>mapreduce.framework.name</name>\
        <value>yarn<\/value>\
    <\/property>\
    <property>\
        <name>yarn.app.mapreduce.am.env</name>\
        <value>HADOOP_MAPRED_HOME=\/usr\/local\/hadoop<\/value>\
    <\/property>\
    <property>\
        <name>mapreduce.map.env</name>\
        <value>HADOOP_MAPRED_HOME=\/usr\/local\/hadoop<\/value>\
    <\/property>\
    <property>\
        <name>mapreduce.reduce.env</name>\
        <value>HADOOP_MAPRED_HOME=\/usr\/local\/hadoop<\/value>\
    <\/property>\
    <\/configuration>' "/usr/local/hadoop/etc/hadoop/mapred-site.xml"
done

##yarn-site.xml
sudo lxc exec namenode -- sed -i '/<configuration>/,/<\/configuration>/c\
<configuration>\
  <property>\
    <name>yarn.nodemanager.aux-services</name>\
    <value>mapreduce_shuffle<\/value>\
  <\/property>\
  <property>\
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>\
    <value>org.apache.hadoop.mapred.ShuffleHandler<\/value>\
  <\/property>\
  <property>\
    <name>yarn.resourcemanager.hostname</name>\
    <value>namenode<\/value>\
  <\/property>\
<\/configuration>' "/usr/local/hadoop/etc/hadoop/yarn-site.xml"


for i in 1
do
    sudo lxc exec datanode$i -- sed -i '/<configuration>/,/<\/configuration>/c\
    <configuration>\
    <property>\
        <name>yarn.nodemanager.aux-services</name>\
        <value>mapreduce_shuffle<\/value>\
    <\/property>\
    <property>\
        <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>\
        <value>org.apache.hadoop.mapred.ShuffleHandler<\/value>\
    <\/property>\
    <property>\
        <name>yarn.resourcemanager.hostname</name>\
        <value>namenode<\/value>\
    <\/property>\
    <\/configuration>' "/usr/local/hadoop/etc/hadoop/yarn-site.xml"

done



wget https://downloads.apache.org/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3.tgz


tar -xzf spark-3.4.3-bin-hadoop3.tgz
sudo mv spark-3.4.3-bin-hadoop3 /usr/local/spark

#inside .bashrc
echo 'export SPARK_HOME=/usr/local/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
source ~/.bashrc

#inside spark-config.env
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::")

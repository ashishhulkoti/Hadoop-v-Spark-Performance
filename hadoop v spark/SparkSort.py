from pyspark import SparkContext
import os
import struct
import blake3

def generate_data(num_records):
    """Generate records with a BLAKE3 hash and a nonce."""
    for _ in range(num_records):
        nonce = os.urandom(6)  # Generate a 6-byte nonce
        hash_value = blake3.blake3(nonce).digest()[:10]  # Generate a 10-byte BLAKE3 hash from the nonce
        yield (hash_value, nonce)  # Return the hash and nonce as a tuple

def sort_records(record):
    """Function to return the hash part of the record for sorting."""
    return record[0]

def to_binary_format(record):
    """Convert the record tuple to binary format."""
    hash_part, nonce_part = record
    return struct.pack('10s6s', hash_part, nonce_part)

def main():
    sc = SparkContext(appName="GenerateAndSortBlake3Hashes")
    num_records_per_partition = 1  # Adjust the number of records per partition based on your cluster size and memory
    num_partitions = 16  # Adjust the number of partitions based on your cluster configuration

    # Create an RDD by parallelizing the data generation task across multiple partitions
    data = sc.parallelize(range(num_partitions), num_partitions).flatMap(lambda _: generate_data(num_records_per_partition))

    # Sort the data based on the hash values
    sorted_data = data.sortBy(sort_records)

    # Coalesce all data into one partition
    coalesced_data = sorted_data.coalesce(1)

    # Map the sorted data to binary format
    binary_data = coalesced_data.map(to_binary_format)

    # Save the binary data to HDFS as a binary file
    # Save as Hadoop file
    binary_data.saveAsNewAPIHadoopFile(
        "hdfs:///user/root/spark_output",
        'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat',
        keyClass='org.apache.hadoop.io.NullWritable',
        valueClass='org.apache.hadoop.io.BytesWritable'
    )
    sc.stop()

if __name__ == "__main__":
    main()

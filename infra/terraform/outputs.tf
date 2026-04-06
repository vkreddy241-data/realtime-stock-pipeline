output "kafka_bootstrap_brokers" {
  description = "MSK bootstrap broker string (TLS)"
  value       = aws_msk_cluster.stock_kafka.bootstrap_brokers_tls
}

output "delta_lake_bucket" {
  description = "S3 bucket name for Delta Lake"
  value       = aws_s3_bucket.stock_lake.bucket
}

output "emr_cluster_id" {
  description = "EMR cluster ID for Spark Streaming"
  value       = aws_emr_cluster.spark_streaming.id
}

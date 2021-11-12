kafka-topics \
   --create \
   --bootstrap-server broker01:2181,broker02:2181,broker03:2181 \
   --replication-factor 3 \
   --partitions 3 \
   --topic pos-invoices

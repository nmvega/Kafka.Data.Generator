kafka-console-consumer \
   --bootstrap-server broker01:9092,broker02:9092,broker03:9092 \
   --topic pos-invoices \
   --from-beginning

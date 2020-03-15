#!/bin/bash

echo "Publishing temperature every second 20 times!"

for i in {1..20}
do
	mosquitto_pub -h localhost -p 1883 -t "sensors/temperature" -m "160 C"
	sleep 1
done

echo "Done publishing temperature!"
#!/bin/bash

echo "Publishing pressure every second 20 times!"

for i in {1..20}
do
	mosquitto_pub -h localhost -p 1883 -t "sensors/pressure" -m "300 bar"
	sleep 1
done

echo "Done publishing Pressure!"
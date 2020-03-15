#!/bin/bash

echo "Subscribing to temperature!"

mosquitto_sub -h localhost -p 1883 -t "sensors/temperature" -v

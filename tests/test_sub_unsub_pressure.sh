#!/bin/bash

echo "Subscribing and unsuncribing to pressure!"

mosquitto_sub -h localhost -p 1883 -t "sensors/pressure" -U "sensors/pressure" -v
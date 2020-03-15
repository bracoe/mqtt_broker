#!/bin/bash

gnome-terminal -e ./test_sub_temp.sh

gnome-terminal -e ./test_pub_temp.sh

gnome-terminal -e ./test_sub_unsub_pressure.sh

gnome-terminal -e ./test_pub_pressure.sh


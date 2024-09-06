# Automated ZFS Resilver Testing Script
Automates testing of ZFS pool resilvers.

Uses FIO to place various simulated CPU and/or disk loads on the system to see how these loads impact resilver times (and how the resilver impacts those loads).

Automates pool creation and fills pool to a specified percent. Can fill the pool with no fragmentation, moderate, or high fragmentation (~0%, ~25%, ~50%). Script can also automatically format disks to a smaller size to speed up testing.

Generates a summary CSV file with statistics from each resilver. General stats gathered on each run:
* Pool used, available
* Actual fill percent
* Actual fragmentation percent
* Resilver time
* Amount of data scanned during the resilver
* Scan speed (when applicable)
* Amount of data issued during the resilver
* Issue speed
* Speed at which the pool filled

This CSV file also notes the test conditions for each resilver (i.e., CPU stress test level, disk stress test level, and target pool fragmentation level).

Also generates one CSV per resilver containing CPU and disk utilization stats sampled on 5 second intervals. Collection of these stats start ~30 second before the resilver is started to gather a rough baseline. CPU and disk stats gathered during each run:
* Write IOPS from an fio monitor function
* Write bandwidth from an fio monitor function
* Write latency from an fio monitor function
* Write IOPS from disk stress function (if running)
* Write bandwidth from disk stress function (if running)
* Write latency from disk stress function (if running)
* Read IOPS from an fio monitor function
* Read bandwidth from an fio monitor function
* Read latency from an fio monitor function
* CPU % (User)
* CPU % (System)
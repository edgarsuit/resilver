#!/bin/python3

# resilver.py
# Test resilvering times on different ZFS layouts, varies recordsize, fill percent, and fragmentation levels
# Runs resilver with and without CPU and disk stress
# Outputs results to a CSV file
# Uses pool name "tank" and dataset name "test"
# Uses FIO for CPU and disk stress as well as pool fill

import subprocess, shlex, math, time, os, random, csv, signal, sys, logging, psutil, shutil

fill_percent = 70             # Target pool fill percent for all tests
physical_disk_size = "7.3T"   # Size of physical disks
format_disks = True           # Format disks before creating pool
format_size = "100G"          # Size to format disks to
target_disk = "sdaa"          # Disk to offline/online during testing
results_file = "output.csv"   # Output file name
log_file = "resilver.log"     # Log file name
append_results = True        # Append results to existing output file instead of creating a new one

# starting_run can be used to resume testing from a specific run number
# First value is the layout, second is the fragmentation level, third is the recordsize, and fourth is the test schedule
# [0,0,0,0] starts from the beginning
starting_test = [14, 0, 0, 0]

# Total number of disks in the pool
TOTAL_NUM_DISKS = 82

# ZFS layouts to test
# layout: ZFS layout
# width: vdev width
# minspares: Minimum number of spares in the vdev
layouts = [
   # SECOND RUN LAYOUTS
   {"layout": "draid1:32d:82c:2s", "width": 82, "minspares": 0},    # 0
   {"layout": "draid1:16d:82c:2s", "width": 82, "minspares": 0},    # 1
   {"layout": "draid1:8d:82c:2s",  "width": 82, "minspares": 0},    # 2
   {"layout": "draid1:4d:82c:2s",  "width": 82, "minspares": 0},    # 3
   {"layout": "draid3:32d:82c:2s", "width": 82, "minspares": 0},    # 4
   {"layout": "draid3:16d:82c:2s", "width": 82, "minspares": 0},    # 5
   {"layout": "draid3:8d:82c:2s",  "width": 82, "minspares": 0},    # 6
   {"layout": "draid3:4d:82c:2s",  "width": 82, "minspares": 0},    # 7
   {"layout": "raidz2",            "width": 15, "minspares": 0},    # 8
   {"layout": "raidz2",            "width": 25, "minspares": 0},    # 9
   {"layout": "raidz2",            "width": 30, "minspares": 0},    # 10
   {"layout": "raidz2",            "width": 35, "minspares": 0},    # 11
   {"layout": "raidz2",            "width": 45, "minspares": 0},    # 12
   {"layout": "raidz2",            "width": 50, "minspares": 0},    # 13
   {"layout": "raidz1",            "width": 5,  "minspares": 0},    # 14
   {"layout": "raidz1",            "width": 20, "minspares": 0},    # 15
   {"layout": "raidz1",            "width": 30, "minspares": 0},    # 16
   {"layout": "raidz1",            "width": 40, "minspares": 0},    # 17
   {"layout": "raidz1",            "width": 50, "minspares": 0},    # 18
   {"layout": "raidz3",            "width": 5,  "minspares": 0},    # 19
   {"layout": "raidz3",            "width": 20, "minspares": 0},    # 20
   {"layout": "raidz3",            "width": 30, "minspares": 0},    # 21
   {"layout": "raidz3",            "width": 40, "minspares": 0},    # 22
   {"layout": "raidz3",            "width": 50, "minspares": 0},    # 23
   {"layout": "draid1:32d:41c:1s", "width": 41, "minspares": 0},    # 24
   {"layout": "draid1:16d:41c:1s", "width": 41, "minspares": 0},    # 25
   {"layout": "draid1:8d:41c:1s",  "width": 41, "minspares": 0},    # 26
   {"layout": "draid1:4d:41c:1s",  "width": 41, "minspares": 0},    # 27
   {"layout": "draid3:32d:41c:1s", "width": 41, "minspares": 0},    # 28
   {"layout": "draid3:16d:41c:1s", "width": 41, "minspares": 0},    # 29
   {"layout": "draid3:8d:41c:1s",  "width": 41, "minspares": 0},    # 30
   {"layout": "draid3:4d:41c:1s",  "width": 41, "minspares": 0},    # 31
   {"layout": "raidz1",            "width": 8,  "minspares": 0},    # 32
   {"layout": "raidz1",            "width": 16, "minspares": 0},    # 33
   {"layout": "raidz2",            "width": 8,  "minspares": 0},    # 34
   {"layout": "raidz2",            "width": 16, "minspares": 0},    # 35
   {"layout": "raidz3",            "width": 8,  "minspares": 0},    # 36
   {"layout": "raidz3",            "width": 16, "minspares": 0},    # 37
   {"layout": "draid1:64d:82c:2s", "width": 82, "minspares": 0},    # 38
   {"layout": "draid2:64d:82c:2s", "width": 82, "minspares": 0},    # 39
   {"layout": "draid3:64d:82c:2s", "width": 82, "minspares": 0},    # 40
   

   # ORIGINAL LAYOUTS
   {"layout": "draid2:32d:82c:2s", "width": 82, "minspares": 0},    # 41
   {"layout": "draid2:16d:82c:2s", "width": 82, "minspares": 0},    # 42
   {"layout": "draid2:8d:82c:2s",  "width": 82, "minspares": 0},    # 43
   {"layout": "draid2:4d:82c:2s",  "width": 82, "minspares": 0},    # 44
   {"layout": "draid2:32d:41c:1s", "width": 41, "minspares": 0},    # 45
   {"layout": "draid2:16d:41c:1s", "width": 41, "minspares": 0},    # 46
   {"layout": "draid2:8d:41c:1s",  "width": 41, "minspares": 0},    # 47
   {"layout": "draid2:4d:41c:1s",  "width": 41, "minspares": 0},    # 48
   {"layout": "raidz2",            "width": 40, "minspares": 0},    # 49
   {"layout": "raidz2",            "width": 20, "minspares": 0},    # 50
   {"layout": "raidz2",            "width": 10, "minspares": 0},    # 51
   {"layout": "raidz2",            "width": 5,  "minspares": 0},    # 52
   {"layout": "raidz3",            "width": 10, "minspares": 0},    # 53
   {"layout": "raidz1",            "width": 10, "minspares": 0},    # 54
   {"layout": "mirror",            "width": 2,  "minspares": 1},    # 55
   {"layout": "mirror",            "width": 3,  "minspares": 1}     # 56  
]

# Fragmentation levels to test on each configuration
frag_schedule = [
   "none",     # 0
   "med",      # 1
   "high"      # 2
]

# Recordsize values to test on each configuration
recordsize_schedule = [
   "1M"        # 0
]

# Tests to run on each configuration
# cpu: CPU stress level
# disk: Disk stress level
test_schedule = [
   {"cpu": "none", "disk": "none"},    # 0
   {"cpu": "med",  "disk": "none"},    # 1
   {"cpu": "high", "disk": "none"},    # 2
   {"cpu": "none", "disk": "med" },    # 3
   {"cpu": "none", "disk": "high"},    # 4
   {"cpu": "med",  "disk": "med" },    # 5
   {"cpu": "high", "disk": "high"}     # 6
]

# Names of the values returned by fio terse output
fio_key = ("terse_version_3;fio_version;jobname;groupid;error;read_kb;read_bandwidth_kb;read_iops;read_runtime_ms;"
   "read_slat_min_us;read_slat_max_us;read_slat_mean_us;read_slat_dev_us;read_clat_min_us;read_clat_max_us;"
   "read_clat_mean_us;read_clat_dev_us;read_clat_pct01;read_clat_pct02;read_clat_pct03;read_clat_pct04;"
   "read_clat_pct05;read_clat_pct06;read_clat_pct07;read_clat_pct08;read_clat_pct09;read_clat_pct10;read_clat_pct11;"
   "read_clat_pct12;read_clat_pct13;read_clat_pct14;read_clat_pct15;read_clat_pct16;read_clat_pct17;read_clat_pct18;"
   "read_clat_pct19;read_clat_pct20;read_tlat_min_us;read_lat_max_us;read_lat_mean_us;read_lat_dev_us;read_bw_min_kb;"
   "read_bw_max_kb;read_bw_agg_pct;read_bw_mean_kb;read_bw_dev_kb;write_kb;write_bandwidth_kb;write_iops;"
   "write_runtime_ms;write_slat_min_us;write_slat_max_us;write_slat_mean_us;write_slat_dev_us;write_clat_min_us;"
   "write_clat_max_us;write_clat_mean_us;write_clat_dev_us;write_clat_pct01;write_clat_pct02;write_clat_pct03;"
   "write_clat_pct04;write_clat_pct05;write_clat_pct06;write_clat_pct07;write_clat_pct08;write_clat_pct09;"
   "write_clat_pct10;write_clat_pct11;write_clat_pct12;write_clat_pct13;write_clat_pct14;write_clat_pct15;"
   "write_clat_pct16;write_clat_pct17;write_clat_pct18;write_clat_pct19;write_clat_pct20;write_tlat_min_us;"
   "write_lat_max_us;write_lat_mean_us;write_lat_dev_us;write_bw_min_kb;write_bw_max_kb;write_bw_agg_pct;"
   "write_bw_mean_kb;write_bw_dev_kb;cpu_user;cpu_sys;cpu_csw;cpu_mjf;cpu_minf;iodepth_1;iodepth_2;iodepth_4;"
   "iodepth_8;iodepth_16;iodepth_32;iodepth_64;lat_2us;lat_4us;lat_10us;lat_20us;lat_50us;lat_100us;lat_250us;"
   "lat_500us;lat_750us;lat_1000us;lat_2ms;lat_4ms;lat_10ms;lat_20ms;lat_50ms;lat_100ms;lat_250ms;lat_500ms;lat_750ms;"
   "lat_1000ms;lat_2000ms;lat_over_2000ms;disk_name;disk_read_iops;disk_write_iops;disk_read_merges;disk_write_merges;"
   "disk_read_ticks;write_ticks;disk_queue_time;disk_util").split(";")

# Main function
# Formats disks if needed, iterates through layouts, fragmentation levels, and tests
# Writes results to a CSV file
def main():
   global log
   global test_index
   global starting_test
   global f

   overall_start_time = time.time()

   # Rotate out previous log file (if it exists)
   try:
      os.rename(log_file,log_file + ".old")
   except:
      pass

   # Set up logging
   logging.basicConfig(filename=log_file,encoding="utf-8",format="%(asctime)s %(message)s",datefmt="%Y-%m-%d %H:%M:%S",level=logging.INFO)
   log = logging.getLogger()
   log.addHandler(logging.StreamHandler(sys.stdout))

   # Setup main output file
   # Check if output file exists and has a header row
   if os.path.isfile(results_file) and append_results == True:
      f = open(results_file,"r")
      header = f.readline().strip()
      if "TestIndex" in header:
         # Results file exists and has a header row, open for appending
         f.close()
         f = open(results_file,"a")
         results = csv.writer(f)
      else:
         # Results file exists but does not have a header row; make a copy of it and create a new one
         f.close()
         os.rename(results_file,results_file + ".old")
         f = open(results_file,"w")
         results = set_up_csv(f)
   else:
      # Create a new results file, rename old one if it exists
      try:
         os.rename(results_file,results_file + ".old")
      except:
         pass
      f = open(results_file,"w")
      results = set_up_csv(f)

   # Check if fio stats directory exists; if not, create it. If it exists, rename it
   try:
      os.mkdir("fio_stats")
   except:
      if append_results == False:
         try:
            os.rename("fio_stats","fio_stats_old")
         except:
            shutil.rmtree("fio_stats_old")
            os.rename("fio_stats","fio_stats_old")
         os.mkdir("fio_stats")
      else:
         pass

   # Display total number of tests to run and starting test number
   total_tests = len(layouts) * len(frag_schedule) * len(recordsize_schedule) * len(test_schedule)
   starting_test_number = starting_test[0]*len(frag_schedule)*len(recordsize_schedule)*len(test_schedule) + \
      starting_test[1]*len(recordsize_schedule)*len(test_schedule) + \
      starting_test[2]*len(test_schedule) + starting_test[3]
   log.info("Total tests to run: " + str(total_tests) + " | Starting test number: " + str(starting_test_number))

   # Format disks if needed; destroy pool (if exists) before formatting
   if format_disks == True:
      destroy_pool()
      format(format_size)

   # Kill old instances of fio
   try:
      subprocess.check_output("pkill -9  fio",shell=True)
   except:
      pass

   # Iterate through layouts
   for layout in layouts[starting_test[0]:]:
      log.info("Starting layout: " + layout["layout"])
      
      # Iterate through fragmentation levels
      for frag in frag_schedule[starting_test[1]:]:
         log.info("Starting fragmentation level: " + frag)

         # Iterate through recordsize values
         for recordsize in recordsize_schedule[starting_test[2]:]:
            log.info("Starting recordsize: " + recordsize)
            
            # Destroy, recreate, and refill pool
            destroy_pool()
            create_pool(layout["layout"],layout["width"],recordsize,layout["minspares"])
            
            # Initialize test index to diplay during pool fill
            test_index = "[" + str(layouts.index(layout)) + ", " + str(frag_schedule.index(frag)) + ", " + str(recordsize_schedule.index(recordsize)) + ", -]"
            
            # fill_pool() returns the speed at which the pool was filled
            fill_speed = fill_pool(fill_percent,frag)

            # Gather pool status for results CSV
            zfs_status = subprocess.check_output("zfs list -Hpo used,available tank/test",shell=True).decode("utf-8")
            used = int(zfs_status.split()[0])
            used_tib = round(used/1024**4,2)
            avail = int(zfs_status.split()[1])
            avail_tib = round(avail/1024**4,2)
            used_percent = round(used/(used+avail)*100,2)
            pool_size = used + avail
            pool_size_tib = round(pool_size/1024**4,2)
            frag_percent = subprocess.check_output("zpool list -Hpo frag tank",shell=True).decode("utf-8").strip()

            log.info("Sleeping 10 seconds to let fio clear...")
            time.sleep(10)

            # Once pool is filled with appropriate fragmentation level, iterate through tests
            for test in test_schedule[starting_test[3]:]:

               # Before starting the tests, export and import the pool to clear ARC data
               log.info("Exporting and importing pool to clear ARC...")
               subprocess.check_output("zpool export tank",shell=True)
               subprocess.check_output("zpool import tank",shell=True)

               test_number = layouts.index(layout)*len(frag_schedule)*len(recordsize_schedule)*len(test_schedule) + \
                  frag_schedule.index(frag)*len(recordsize_schedule)*len(test_schedule) + \
                  recordsize_schedule.index(recordsize)*len(test_schedule) + test_schedule.index(test)
               test_index = "[" + str(layouts.index(layout)) + ", " + str(frag_schedule.index(frag)) + ", " + \
                  str(recordsize_schedule.index(recordsize)) + ", " + str(test_schedule.index(test)) + "]"
               # Log test index
               elapsed_time = sec_to_dhms(time.time() - overall_start_time)
               log.info("Starting test index " + test_index + " (" + str(test_number) + "/" + str(total_tests) + ") | Total runtime: " + elapsed_time)

               # Set up latency tracking variables
               num_write_latency_samples_mon = 1
               write_lat_mean_mon = 0
               num_write_latency_samples_stress = 1
               write_lat_mean_stress = 0
               num_read_latency_samples = 1
               read_lat_mean = 0

               # Set up FIO stats CSV file
               fio_stats_file = "fio_stats/" + test_index.replace("[","").replace("]","").replace(", ","-") + ".csv"   
               fio_file = open(fio_stats_file,"w")

               fio_stats = csv.writer(fio_file)
               fio_stats.writerow([
                  "Write IOPS (monitor)",
                  "Write Bandwidth (MiB/s, monitor)",
                  "Write Latency (mSec, monitor)",
                  "Write IOPS (stress)",
                  "Write Bandwidth (MiB/s, stress)",
                  "Write Latency (mSec, stress)",
                  "Read IOPS",
                  "Read Bandwidth (MiB/s)",
                  "Read Latency (mSec)",
                  "CPU % User",
                  "CPU % System"
               ])

               # Start CPU and disk stress. If stress is set to "none", these functions will return 0
               disk_stress_handle = disk_stress(test["disk"])
               cpu_stress_handle = cpu_stress(test["cpu"])
               read_monitor_handle = read_latency_monitor()
               write_monitor_handle = write_latency_monitor()

               # Gather CPU and disk stats before resilver starts
               log.info("Gathering pre-resilver system stats for 60 seconds...")
               for i in range(12):
                  get_fio_stats(
                     disk_stress_handle,
                     read_monitor_handle,
                     write_monitor_handle,
                     fio_stats,
                     5,
                     num_read_latency_samples,
                     read_lat_mean,
                     num_write_latency_samples_mon,
                     write_lat_mean_mon,
                     num_write_latency_samples_stress,
                     write_lat_mean_stress)
               
               # Offline target disk to start the resilver, log event in fio stat file, wait for 5 seconds before checking resilver status
               offline_disk(target_disk)
               fio_stats.writerow(["Resilver Start"])
               fio_file.flush()
               time.sleep(5)

               # Set up average speed tracking variables
               scan_speed_avg = 0
               scan_sample_count = 1
               issue_speed_avg = 0
               issue_sample_count = 1

               # Wait for resilver to complete, checking status every 5 seconds
               resilver_status = get_resilver_status()
               while resilver_status[0] == "resilvering":

                  # Calculate average scan speed
                  scan_speed = resilver_status[1]
                  try:
                     if "T/s" in scan_speed:
                        scan_speed_float = float(scan_speed.split("T/s")[0]) * 1000 * 1000
                     elif "G/s" in scan_speed:
                        scan_speed_float = float(scan_speed.split("G/s")[0]) * 1000
                     elif "M/s" in scan_speed:
                        scan_speed_float = float(scan_speed.split("M/s")[0])
                     scan_speed_avg = (scan_speed_avg * (scan_sample_count - 1) + scan_speed_float)/scan_sample_count
                     scan_sample_count += 1
                  
                  except:
                     pass

                  # Calculate average issue speed
                  issue_speed = resilver_status[2]
                  try:
                     if "T/s" in issue_speed:
                        issue_speed_float = float(issue_speed.split("T/s")[0]) * 1000 * 1000
                     elif "G/s" in issue_speed:
                        issue_speed_float = float(issue_speed.split("G/s")[0]) * 1000
                     elif "M/s" in issue_speed:
                        issue_speed_float = float(issue_speed.split("M/s")[0])
                     issue_speed_avg = (issue_speed_avg * (issue_sample_count - 1) + issue_speed_float)/issue_sample_count
                     issue_sample_count += 1
                  
                  except:
                     pass
                  
                  # Log resilver status
                  percent_done = resilver_status[3]
                  time_left = resilver_status[4]
                  log.info(test_index + " Resilvering: " + percent_done + " (" + issue_speed + ", ETA " + time_left + ")")
                  
                  # Gather FIO stats
                  get_fio_stats(
                     disk_stress_handle,
                     read_monitor_handle,
                     write_monitor_handle,
                     fio_stats,
                     0.1,
                     num_read_latency_samples,
                     read_lat_mean,
                     num_write_latency_samples_mon,
                     write_lat_mean_mon,
                     num_write_latency_samples_stress,
                     write_lat_mean_stress)
                  fio_file.flush()

                  # Call to psutil above blocks for 0.1 seconds, so we sleep for 4.9 seconds to keep the 5 second interval
                  time.sleep(4.9)

                  # When resilver is at 100%, zpool status output can cause parse issues.
                  # If we fail to parse, wait 5 seconds and try again
                  try:
                     resilver_status = get_resilver_status()
                  except:
                     time.sleep(5)
                     resilver_status = get_resilver_status()
               
               # Log resilver results
               log.info("Resilver complete in " + resilver_status[1] + " | " + resilver_status[3] + " resilvered")

               # Calculate resilver time in seconds and minutes
               resilver_time_seconds = int(resilver_status[1].split(":")[0])*60*60 + int(resilver_status[1].split(":")[1])*60 + int(resilver_status[1].split(":")[2])
               resilver_time_minutes = resilver_time_seconds/60

               # Terminate stress tests
               if disk_stress_handle != 0:
                  disk_stress_handle.terminate()
                  log.info("Disk stress terminated")
               if cpu_stress_handle != 0:
                  cpu_stress_handle.terminate()
                  log.info("CPU stress terminated")
               read_monitor_handle.terminate()
               write_monitor_handle.terminate()
               time.sleep(5)
               log.info("Read and write latency monitoring terminated")

               # Clean up scan and issue speed values if needed
               if scan_speed_avg == 0: scan_speed_avg = "-"
               if issue_speed_avg == 0: issue_speed_avg = "-"

               # Get some extra data for the output sheet for the pool
               vdev_type = layout["layout"].split(":")[0]
               parity_level = get_parity_level(vdev_type, layout["width"])
               num_vdevs = get_num_vdevs(vdev_type, layout["width"])
               size_per_vdev = pool_size / num_vdevs

               # Generate a concise layout description
               if "draid" in layout["layout"]:
                  layout_description = layout["layout"]
                  draid_data_disks = layout["layout"].split(":")[1].split("d")[0]
                  draid_spare_disks = layout["layout"].split(":")[3].split("s")[0]
                  num_hot_spares = 0
               elif "raidz" in layout["layout"]:
                  layout_description = str(layout["width"]) + "-wide " + layout["layout"]
                  draid_data_disks = "-"
                  draid_spare_disks = "-"
                  num_hot_spares = TOTAL_NUM_DISKS - (num_vdevs * layout["width"])
               elif "mirror" in layout["layout"]:
                  layout_description = str(layout["width"]) + "-way mirror"
                  draid_data_disks = "-"
                  draid_spare_disks = "-"
                  num_hot_spares = TOTAL_NUM_DISKS - (num_vdevs * layout["width"])

               # Calculate pool AFR
               afr_array_1x = []
               for disk_afr in range(1,11):
                  afr = disk_afr / 100
                  pool_afr = get_pool_afr(layout["width"], parity_level, num_vdevs, afr, resilver_time_seconds)
                  afr_array_1x.append(pool_afr)

               afr_array_100x = []
               for disk_afr in range(1,11):
                  afr = disk_afr / 100
                  pool_afr = get_pool_afr(layout["width"], parity_level, num_vdevs, afr, resilver_time_seconds * 100)
                  afr_array_100x.append(pool_afr)
               
               # Write results from this run to CSV
               results.writerow([
                  test_index,                # Test Index
                  layout["layout"],          # Layout
                  vdev_type,                 # Vdev Type
                  layout["width"],           # Vdev Width
                  parity_level,              # Parity Level
                  num_vdevs,                 # Number of Vdevs
                  num_hot_spares,            # Number of Hot Spares
                  size_per_vdev,             # Size per Vdev
                  layout_description,        # Layout Description
                  draid_data_disks,          # dRAID Data Disks
                  draid_spare_disks,         # dRAID Spare Disks
                  recordsize,                # Recordsize
                  str(fill_percent) + "%",   # Target Fill Percent
                  used,                      # Used (bytes)
                  used_tib,                  # Used (TiB)
                  avail,                     # Available (bytes)
                  avail_tib,                 # Available (TiB)
                  str(used_percent) + "%",   # Used Percent
                  pool_size,                 # Pool Size (bytes)
                  pool_size_tib,             # Pool Size (TiB)
                  str(frag_percent) + "%",   # Fragmentation Percent
                  format_size,               # Disk Size
                  frag,                      # Fragmentation Level
                  test["cpu"],               # CPU Stress
                  test["disk"],              # Disk Stress
                  resilver_status[1],        # Resilver Time
                  resilver_time_seconds,     # Resilver Time (seconds)
                  resilver_time_minutes,     # Resilver Time (minutes)
                  afr_array_1x[0],           # Pool AFR at 1% disk AFR
                  afr_array_1x[1],           # Pool AFR at 2% disk AFR
                  afr_array_1x[2],           # Pool AFR at 3% disk AFR
                  afr_array_1x[3],           # Pool AFR at 4% disk AFR
                  afr_array_1x[4],           # Pool AFR at 5% disk AFR
                  afr_array_1x[5],           # Pool AFR at 6% disk AFR
                  afr_array_1x[6],           # Pool AFR at 7% disk AFR
                  afr_array_1x[7],           # Pool AFR at 8% disk AFR
                  afr_array_1x[8],           # Pool AFR at 9% disk AFR
                  afr_array_1x[9],           # Pool AFR at 10% disk AFR
                  afr_array_100x[0],         # Pool AFR at 1% disk AFR (100x resilver time)
                  afr_array_100x[1],         # Pool AFR at 2% disk AFR (100x resilver time)
                  afr_array_100x[2],         # Pool AFR at 3% disk AFR (100x resilver time)
                  afr_array_100x[3],         # Pool AFR at 4% disk AFR (100x resilver time)
                  afr_array_100x[4],         # Pool AFR at 5% disk AFR (100x resilver time)
                  afr_array_100x[5],         # Pool AFR at 6% disk AFR (100x resilver time)
                  afr_array_100x[6],         # Pool AFR at 7% disk AFR (100x resilver time)
                  afr_array_100x[7],         # Pool AFR at 8% disk AFR (100x resilver time)
                  afr_array_100x[8],         # Pool AFR at 9% disk AFR (100x resilver time)
                  afr_array_100x[9],         # Pool AFR at 10% disk AFR (100x resilver time)
                  resilver_status[2],        # Scanned
                  scan_speed_avg,            # Scan Speed (M/s)
                  resilver_status[3],        # Issued
                  issue_speed_avg,           # Issue Speed (M/s)
                  fill_speed                 # Fill Speed
               ])
               f.flush()

               # Online target disk; data hasn't changed to resilvering should happen in <1 second
               online_disk(target_disk)

               # Sleep for 30 seconds between tests
               log.info("Sleeping 30 seconds to let pool recover...")
               time.sleep(30)

            # Reset starting_test test schedule to 0 otherwise those tests will be skipped on the next run
            starting_test[3] = 0
         
         # Reset starting_test recordsize schedule to 0 otherwise those tests will be skipped on the next run
         starting_test[2] = 0

      # Reset starting_test frag schedule 0 otherwise those tests will be skipped on the next run
      starting_test[1] = 0

   # Log completion time
   log.info("All tests completed in " + sec_to_dhms(time.time() - overall_start_time))

   # Close output file after all tests completed
   f.close()

# Set up the CSV file with headers
def set_up_csv(f):
   results = csv.writer(f)
   results.writerow([
      "TestIndex",
      "Layout",
      "VdevType",
      "VdevWidth",
      "ParityLevel",
      "NumVdevs",
      "NumHotSpares",
      "SizePerVdev",
      "LayoutDescription",
      "dRAIDDataDisks",
      "dRAIDSpareDisks",
      "RecordSize",
      "TargetFillPercent",
      "UsedBytes",
      "UsedTiB",
      "AvailableBytes",
      "AvailableTiB",
      "UsedPercent",
      "PoolSizeBytes",
      "PoolSizeTiB",
      "FragPercent",
      "DiskSize",
      "FragLevel",
      "CPUStress",
      "DiskStress",
      "ResilverTime",
      "ResilverTimeSeconds",
      "ResilverTimeMinutes",
      "PoolAFR1percent",
      "PoolAFR2percent",
      "PoolAFR3percent",
      "PoolAFR4percent",
      "PoolAFR5percent",
      "PoolAFR6percent",
      "PoolAFR7percent",
      "PoolAFR8percent",
      "PoolAFR9percent",
      "PoolAFR10percent",
      "PoolAFR1percent100x",
      "PoolAFR2percent100x",
      "PoolAFR3percent100x",
      "PoolAFR4percent100x",
      "PoolAFR5percent100x",
      "PoolAFR6percent100x",
      "PoolAFR7percent100x",
      "PoolAFR8percent100x",
      "PoolAFR9percent100x",
      "PoolAFR10percent100x",
      "Scanned",
      "ScanSpeedMBps",
      "Issued",
      "IssueSpeedMBps",
      "FillSpeedGiBps"])
   return results

def get_fio_stats(
      disk_stress_handle,
      read_monitor_handle,
      write_monitor_handle,
      fio_stats,
      psutil_interval,
      num_read_latency_samples,
      read_lat_mean,
      num_write_latency_samples_mon,
      write_lat_mean_mon,
      num_write_latency_samples_stress,
      write_lat_mean_stress):
   global fio_key

   # Gather CPU stats
   cpu_info_before_resilver = psutil.cpu_times_percent(interval=psutil_interval)
   cpu_user = cpu_info_before_resilver.user
   cpu_system = cpu_info_before_resilver.system

   # Gather write latency stats from the write_monitor process
   # Latency presented by FIO is an average over the full run. We need to calculate the average latency per sample
   out = write_monitor_handle.stdout.readline().strip().split(";")
   write_iops_mon = out[fio_key.index("write_iops")]
   write_bw_mon = out[fio_key.index("write_bw_mean_kb")]
   write_lat_mean_old_mon = write_lat_mean_mon
   write_lat_mean_mon = float(out[fio_key.index("write_lat_mean_us")])/1000
   write_lat_mon = (num_write_latency_samples_mon * write_lat_mean_mon) - (write_lat_mean_old_mon * (num_write_latency_samples_mon - 1))

   # Gather read latency stats from the read_monitor process
   out = read_monitor_handle.stdout.readline().strip().split(";")
   if "fio: opendir added" not in out[0]:
      read_iops = out[fio_key.index("read_iops")]
      read_bw = out[fio_key.index("read_bw_mean_kb")]
      read_lat_mean_old = read_lat_mean
      read_lat_mean = float(out[fio_key.index("read_lat_mean_us")])/1000
      read_lat = (num_read_latency_samples * read_lat_mean) - (read_lat_mean_old * (num_read_latency_samples - 1))
      num_read_latency_samples += 1
   else:
      read_iops = "0"
      read_bw = "0"
      read_lat = "0"

   # If a disk stress test is running, gather write disk stats from it
   if disk_stress_handle != 0:
      out = disk_stress_handle.stdout.readline().strip().split(";")
      write_iops_stress = out[fio_key.index("write_iops")]
      write_bw_stress = float(out[fio_key.index("write_bw_mean_kb")])/1024
      write_lat_mean_old_stress = write_lat_mean_stress
      write_lat_mean_stress = float(out[fio_key.index("write_lat_mean_us")])/1000
      write_lat_stress = (num_write_latency_samples_stress * write_lat_mean_stress) - (write_lat_mean_old_stress * (num_write_latency_samples_stress - 1))
      num_write_latency_samples_stress += 1
   else:
      write_iops_stress = "0"
      write_bw_stress = "0"
      write_lat_stress = "0"
   
   fio_stats.writerow([write_iops_mon,write_bw_mon,write_lat_mon,write_iops_stress,write_bw_stress,write_lat_stress,read_iops,read_bw,read_lat,cpu_user,cpu_system])

# Convert seconds to a string with days, hours, minutes, and seconds
def sec_to_dhms(seconds):
   [d,h,m,s] = [0,0,0,0]
   m, s = divmod(seconds, 60)
   h, m = divmod(m, 60)
   d, h = divmod(h, 24)
   if d != 0:
      t = str(round(d)) + "d " + "{:02}".format(round(h)) + "h " + "{:02}".format(round(m)) + "m " + "{:02}".format(round(s)) + "s"
   elif h != 0:
      t = str(round(h)) + "h " + "{:02}".format(round(m)) + "m " + "{:02}".format(round(s)) + "s"
   elif m != 0:
      t = str(round(m)) + "m " + "{:02}".format(round(s)) + "s"
   else:
      t = "{:02}".format(round(s)) + "s"
   return t

# Get a list of the device nodes for all disks with the specified size (i.e., don't use boot, cache, etc. drives)
def get_disk_list():
   global log

   disk_list = []
   disk_list_raw = subprocess.check_output("/usr/bin/lsblk -d -n --output NAME,SIZE",shell=True)
   disk_list_raw = disk_list_raw.decode("utf-8").splitlines()
   
   for disk in disk_list_raw:
      dev_node = disk.split()[0]
      disk_size = disk.split()[1]
      if disk_size == physical_disk_size:
         disk_list.append("/dev/" + dev_node)

   return(disk_list)

# Format disks to a specified size
def format(format_size):
   global log

   start = time.time()
   disk_list = get_disk_list()
   num_formatted = 0
   num_skipped = 0

   for disk in disk_list:
      # Check if disk is already formatted to the specified size. If disk is not partitioned, lsblk will error and disk needs to be formatted
      try:
         partition_info = subprocess.check_output("lsblk -n --output size " + disk + "1",shell=True).decode("utf-8").strip()
      except:
         partition_info = 0

      # If disk is not formatted to the specified size, format it. If it has already been formatted to the correct size, skip it
      if partition_info != format_size:
         log.info("Formatting " + disk + "...")
         subprocess.check_output("sgdisk -Z " + disk,shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
         subprocess.check_output("sgdisk -n 0:0:+" + format_size + " " + disk,shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
         num_formatted += 1
      else:
         num_skipped += 1

   time_taken = time.time() - start
   log.info("Formatted " + str(num_formatted) + " and skipped " + str(num_skipped) + " disks in " + sec_to_dhms(time_taken))

# CPU stress function
def cpu_stress(cpu_load):
   global log

   # If CPU stress is set to "none", return 0 and don't start stress
   if cpu_load == "none":
      return 0
   elif cpu_load == "med":
      log.info("Starting medium CPU stress...")
      cpu_load = 50
   elif cpu_load == "high":
      cpu_load = 80
      log.info("Starting high CPU stress...")
   
   # Get number of CPU cores to scale stress level
   cpu_count = os.cpu_count()

   # Set up FIO command for CPU stress
   cmd = """
         fio \
         --name=cpuburn \
         --ioengine=cpuio \
         --group_reporting \
      """
   cmd += "--numjobs=" + str(cpu_count) + " \\"
   cmd += "--cpuload=" + str(cpu_load)

   # Start CPU stress and return process handle
   proc = subprocess.Popen(shlex.split(cmd),stdout=subprocess.DEVNULL)
   return proc

# Disk stress function
def disk_stress(disk_load):
   global log

   # If disk stress is set to "none", return 0 and don't start stress
   if disk_load == "none":
      return 0
   elif disk_load == "med":
      log.info("Starting medium disk stress...")
   elif disk_load == "high":
      log.info("Starting high disk stress...")

   # Set up FIO command for disk stress
   cmd = """
      fio \
      --rw=write \
      --ioengine=io_uring \
      --runtime=100D \
      --time_based=1 \
      --direct=1 \
      --directory=/mnt/tank/test/diskstress/ \
      --group_reporting \
      --unified_rw_reporting=both \
      --output-format=terse \
      --status-interval=5 \
      --name=diskstress \
   """
   # We high disk utilization with more jobs and smaller block sizes
   if disk_load == "high":
      cmd += "--numjobs=256 \\"
      cmd += "--bs=4Ki \\"
      cmd += "--filesize=1Mi"
   # We get moderate utilization with fewer jobs and larger block sizes
   elif disk_load == "med":
      cmd += "--numjobs=16 \\"
      cmd += "--bs=256Ki \\"
      cmd += "--filesize=1Mi"
      
   # Set sync=always on the dataset so I/O is not buffered in memory
   subprocess.run("zfs set sync=always tank/test",shell=True)

   # Remove any previous disk stress files and create a new directory for the stress test files
   subprocess.run("rm -rf /mnt/tank/test/diskstress",shell=True)
   subprocess.run("mkdir /mnt/tank/test/diskstress",shell=True)

   # Start disk stress and return process handle
   proc = subprocess.Popen(shlex.split(cmd),stdout=subprocess.PIPE,text=True)
   return proc

# Performs random reads with iodpeth of 1 to monitor read latency.
# Disables primarycache on dataset to ensure reads are from disk rather than ARC
def read_latency_monitor():
   # Set up FIO command to perform small queue random reads
   cmd = """
      fio \
      --rw=randread \
      --ioengine=io_uring \
      --runtime=100D \
      --time_based=1 \
      --direct=1 \
      --opendir=/mnt/tank/test/fill/fill0/0 \
      --group_reporting \
      --unified_rw_reporting=both \
      --name=readlatmon \
      --output-format=terse \
      --allow_file_create=0 \
      --status-interval=5 \
      --numjobs=1 \
      --nrfiles=1 \
      --file_service_type=random \
      --bs=4Ki
   """
   subprocess.run("zfs set primarycache=none tank/test",shell=True)
   proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, text=True)
   return proc

# Performs random writes with iodpeth of 1 to monitor read latency. Performs one I/O every 100ms
def write_latency_monitor():
   # Set up FIO command to perform small queue random reads
   cmd = """
      fio \
      --rw=randwrite \
      --ioengine=io_uring \
      --runtime=100D \
      --time_based=1 \
      --direct=1 \
      --thinktime=100ms \
      --directory=/mnt/tank/test/write_latency \
      --group_reporting \
      --unified_rw_reporting=both \
      --name=writelatmon \
      --output-format=terse \
      --status-interval=5 \
      --numjobs=1 \
      --nrfiles=1 \
      --file_service_type=random \
      --bs=4Ki \
      --filesize=4Ki
   """
   # Set sync=always on the dataset so I/O is not buffered in memory
   subprocess.run("zfs set sync=always tank/test",shell=True)

   # Remove any previous disk stress files and create a new directory for the stress test files
   subprocess.run("rm -rf /mnt/tank/test/write_latency",shell=True)
   subprocess.run("mkdir /mnt/tank/test/write_latency",shell=True)
   proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, text=True)
   return proc

# Create a ZFS pool with the specified layout, vdev width, recordsize, and minimum spare count
def create_pool(layout,vdev_width,recordsize,minspares):
   global log

   start = time.time()

   # Get a list of disks to use for the pool
   disk_list = get_disk_list()

   # If the minimum spare count is set, pop the first disk(s) off the list and add them to the spares list
   spares = ""
   if minspares != 0:
      for spare in range(minspares):
         if format_disks:
            spares += disk_list[0] + "1 "
         else:
            spares += disk_list[0] + " "
         disk_list.pop(0)

   # Use the remaining disks to form the vdevs
   num_disks = len(disk_list)
   num_vdevs = math.floor(num_disks/vdev_width)
   vdev_lists = [""] * num_vdevs

   # Add each disk to a vdev list
   for vdev in range(num_vdevs):
      for disk in range(vdev_width):
         if format_disks:
            vdev_lists[vdev] += disk_list[0] + "1 "
         else:
            vdev_lists[vdev] += disk_list[0] + " "
         disk_list.pop(0)
      vdev_lists[vdev] = vdev_lists[vdev].strip()

   # If there are any disks left in the disk list, add them to the spares
   if disk_list != []:
      for disk in disk_list:
         if format_disks:
            spares += disk + "1 "
         else:
            spares += disk + " "
   spares = spares.strip()

   # Format the zpool create string; set ashift=12 and autoreplace=on
   zpool_create = "zpool create -f tank -o ashift=12 -o autoreplace=on -m /mnt/tank "

   # Add each vdev to the zpool create string
   for vdev in vdev_lists:
      zpool_create += layout + " " + vdev + " "

   # If there are any spares, add them to the zpool create string
   # If we're using draid, we don't need traditional hot spares
   if spares != "" or "draid" not in layout:
      zpool_create += "spare " + spares
   else:
      zpool_create = zpool_create.strip()
   
   # Create the pool
   log.info("Creating pool/dataset with layout: " + layout + " and recordsize: " + recordsize + "...")
   subprocess.check_output(shlex.split(zpool_create))
   
   # Create a dataset with the specified recordsize and disable compression
   subprocess.check_output("zfs create -o compression=off -o recordsize=" + recordsize + " tank/test",shell=True)
   time_taken = time.time() - start
   log.info("Created pool in " + sec_to_dhms(time_taken))

# Destroy zpool if it exists
def destroy_pool():
   global log
   
   start = time.time()
   log.info("Destroying pool...")
   
   # Kill any running instances of fio, otherwise pool destroy can fail
   try:
      subprocess.check_output("pkill -9 fio",shell=True)
      log.info("Killed lingering fio processes.")
      time.sleep(1)
   except:
      pass
   
   pool_status = "online"
   while pool_status == "online":
      # Check if pool exists and is online
      try:
         pool_status = subprocess.check_output("zpool list -Ho name tank",shell=True,stderr=subprocess.DEVNULL).decode("utf-8").strip()
         if pool_status == "tank": pool_status = "online"
      except:
         pool_status = "offline"
         break

      # If pool is online, destroy it
      try:
         subprocess.check_output("zpool destroy tank",shell=True,stderr=subprocess.DEVNULL)
      except:
         try:
            subprocess.check_output("pkill -9 fio",shell=True)
            log.info("Killed lingering fio processes.")
            time.sleep(1)
         except:
            log.info("Could not destroy pool or kill fio processes.")
            time.sleep(5)
      
   time_taken = time.time() - start
   log.info("Destroyed pool in " + sec_to_dhms(time_taken))

# Fill pool to a specified percentage with a specified fragmentation level
# Moderate (~30%) and high fragmentation (~50%) levels are achieved by writing small, unaligned blocks to fill up the pool
# to 100% and then randomly deleting a certain percentage of those files to get back to the specified fill percentage.
# If fragmentation is "none", sequentially fill the pool
def fill_pool(fill_percent,frag_level):
   global log
   global test_index

   start = time.time()

   # Set sync=disabled on the dataset for faster fill
   subprocess.run("zfs set sync=disabled tank/test",shell=True)
   
   # Create a directory for the fill files
   subprocess.run("mkdir /mnt/tank/test/fill",shell=True)
   
   # We will track the pool percent used as well as the run number during the fill loop
   percent_used = 0
   run_number = 0

   # Check pool size and calculate the fill size required
   zfs_status = subprocess.check_output("zfs list tank/test -Hpo used,available",shell=True).decode("utf-8")
   pool_size = int(zfs_status.split()[0]) + int(zfs_status.split()[1])

   if frag_level == "none":
      # If no fragmentation is specified, sequentially fill the pool with large blocks to the specified percentage
      fill_size = pool_size * fill_percent/100
      log.info("Filling pool to " + str(fill_percent) + "%...")
   else:
      # If fragmentation is specified, fill the pool to 100% and then prune the specified percentage of files
      fill_size = pool_size
      prune_percent = 100 - fill_percent
      fill_percent = 100
      log.info("Filling pool to 100% and pruning " + str(prune_percent) + "%...")
   
   # Fill the pool with the specified fragmentation level
   while percent_used < fill_percent:
      # Set up FIO command for pool fill
      cmd = """
         fio \
         --rw=write \
         --ioengine=io_uring \
         --directory=/mnt/tank/test/fill/ \
         --filename_format='$jobname/$jobnum/$filenum' \
         --group_reporting \
         --unified_rw_reporting=both \
         --nrfiles=2000 \
         --openfiles=100 \
         --file_service_type=sequential \
         --fallocate=none \
      """
      # We get higher fragmentation with small, unaligned blocks
      if frag_level == "high":
         cmd += "--numjobs=128 \\"
         cmd += "--bs_unaligned \\"
         cmd += "--bsrange=4Ki-128Ki \\"
         cmd += "--filesize=4Ki-128Ki \\"
      # We get moderate fragmentation with medium, unaligned blocks
      elif frag_level == "med":
         cmd += "--numjobs=128 \\"
         cmd += "--bs_unaligned \\"
         cmd += "--bsrange=128Ki-1Mi \\"
         cmd += "--filesize=128Ki-1Mi \\"
      # If no fragmentation is needed, use large, aligned blocks and fewer jobs
      elif frag_level == "none":
         cmd += "--numjobs=8 \\"
         cmd += "--bs=1Mi \\"
         cmd += "--filesize=100Mi \\"
      cmd += "--name=fill" + str(run_number)

      # Start the fill process
      proc = subprocess.Popen(shlex.split(cmd),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

      # Wait for the fill run to complete (multiple runs required to fill to the specified percentage)
      # Check pool status every 5 seconds and calculate fill rate and used percentage
      # Once pool is filled to the specified percentage, terminate the fill process
      while proc.poll() == None:
         # Check pool fill status
         zfs_status = subprocess.check_output("zfs list tank/test -Hpo used,available",shell=True).decode("utf-8")
         used = int(zfs_status.split()[0])
         avail = int(zfs_status.split()[1])
         percent_used = round(used/(used+avail)*100,2)

         # Calculate elapsed time and use it to calculate fill rate and time left
         time_taken = time.time() - start
         rate_bps = used/time_taken
         rate_gibps = round(rate_bps/1024**3,2)
         if rate_bps == 0:
            rate_bps = 1
         size_left = fill_size - used
         time_left_sec = size_left/rate_bps
         if time_left_sec < 0: time_left_sec = 0

         # Calculate pool fill percentage and log status
         used_tib_str =  "{:.2f}".format(round(used/1024**4,2))
         total_tib_str =  "{:.2f}".format(round((used+avail)/1024**4,2))
         percent_used_str = "{:.2f}".format(percent_used)
         rate_gibps_str = "{:.2f}".format(rate_gibps)
         log.info(test_index + " Filling pool to " + str(fill_percent) + "% (frag @ " + frag_level + "): " + used_tib_str + \
            "TiB/" + total_tib_str + "TiB >> " + percent_used_str + "% (" + rate_gibps_str + "Gi/s | ETA " + sec_to_dhms(time_left_sec) + ")")
         
         # If pool is filled to the specified percentage, terminate the fill process and break the loop
         if percent_used >= fill_percent:
            proc.terminate()
            break
         time.sleep(5)
      
      # Increment the run number and start the next fill run
      run_number += 1
   
   # Calculate and log the time taken to fill the pool
   time_taken = time.time() - start
   log.info("Filled pool in " + sec_to_dhms(time_taken))

   # If fragmentation is specified, prune the specified percentage of files
   if frag_level == "high" or frag_level == "med":
      start = time.time()
      log.info("Pruning " + str(prune_percent) + "% of files")

      # Prune random files until the specified size is reached
      for root, dirs, files in os.walk("/mnt/tank/test/fill/"):
         for file in files:
            if random.random() < prune_percent/100:
               os.remove(os.path.join(root,file))

      # Get the current pool status and calculate the size to prune to achieve the specified fill percentage
      zfs_status = subprocess.check_output("zfs list tank/test -Hpo used,available",shell=True).decode("utf-8")
      used = int(zfs_status.split()[0])
      avail = int(zfs_status.split()[1])
      total = used + avail
      percent_used = round(used/total*100,2)

      # Calculate and log the time taken to prune the files
      time_taken = time.time() - start
      log.info("Files pruned in " + sec_to_dhms(time_taken) + ", pool at " + str(percent_used) + "% full")

   # Return the average fill speed
   return str(rate_gibps)

# Offline specified disk from the pool
def offline_disk(disk):
   global log
   disk = "/dev/" + disk
   if format_disks: disk += "1"
   log.info("Taking " + disk + " offline...")
   subprocess.check_output("zpool offline tank -f " + disk,shell=True,stderr=subprocess.DEVNULL)

# Online specified disk on the pool
def online_disk(disk):
   global log
   disk = "/dev/" + disk
   if format_disks: disk += "1"
   log.info("Bringing " + disk + " online...")
   subprocess.check_output("zpool online tank " + disk,shell=True,stderr=subprocess.DEVNULL)
   subprocess.check_output("zpool clear tank",shell=True,stderr=subprocess.DEVNULL)

# Check resilver status
def get_resilver_status():
   global log

   # Get resilver status from zpool status output
   resilver_status = subprocess.check_output("zpool status tank",shell=True).decode("utf-8")

   # The output of the zpool status command is slightly different for different ZFS layouts
   if "draid" in resilver_status:
      raid_type = "draid"
   elif "raidz" in resilver_status:
      raid_type = "raidz"
   elif "mirror" in resilver_status:
      raid_type = "mirror"

   # Check if resilver is in progress or if it has completed
   if "currently being resilvered" in resilver_status:
      resilver_status = resilver_status.splitlines()
      for line in resilver_status:
         if "scanned" in line:
            # Extract scan speed from the zpool status output
            scan_info = line.split(",")[0].split()
            scan_speed = scan_info[len(scan_info)-1]
            # On RAIDZ and mirrored vdevs, the scan speed is not listed
            if scan_speed == "scanned": scan_speed = "-"

            # Extract issue speed from the zpool status output
            issue_info = line.split(",")[1].split()
            issue_speed = issue_info[len(issue_info)-1]
            # At the very start of a resilver, issue speed is not listed
            if issue_speed == "issued": issue_speed = "-"
         
         if "resilvered" in line:
            # Extract percent done and time left from the zpool status output
            percent_done = line.split()[2]
            time_left = line.split()[4]
            # If resilver is at 100%, time left will not be listed in the zpool status output
            if time_left == "no":
               time_left = "00:00:00" 
      # Return all the resilver progress information
      return ("resilvering",scan_speed,issue_speed,percent_done,time_left)
   
   # If resilver has completed (but we still have a faulted disk), get the resilver rate(s) and time
   elif "persistent errors" in resilver_status:
      resilver_status = resilver_status.splitlines()
      for line in resilver_status:
         # For draid vdevs, get the resilver time, amount of data scanned, and amount of data issued
         if raid_type == "draid":
            if "resilvered" in line:
               resilver_time = line.split()[5]
            if "scanned," in line:
               scanned = line.split()[0]
               issued = line.split()[4]
            if "scanned at" in line:
               scanned = line.split()[2]
               issued = line.split()[8]

         # For RAIDZ and mirrored vdevs, get the resilver time and amount of data issued (scanned amount is not listed)
         elif raid_type == "raidz" or raid_type == "mirror":
            if "resilvered" in line:
               resilver_time = line.split()[4]
               scanned = "-"
               issued = line.split()[2]
      
      # Return resilver completion information
      return ("complete",resilver_time,scanned,issued)
   
   # If the pool is healthy, return "healthy"
   else:
      return "healthy"

# Returns the parity level of the vdev configuration
def get_parity_level(vdev_type, vdev_width):
   if vdev_type == "draid1": parity_level = 1
   if vdev_type == "draid2": parity_level = 2
   if vdev_type == "draid3": parity_level = 3
   if vdev_type == "raidz1": parity_level = 1
   if vdev_type == "raidz2": parity_level = 2
   if vdev_type == "raidz3": parity_level = 3
   if vdev_type == "mirror":
      parity_level = vdev_width - 1
   return parity_level

def get_num_vdevs(vdev_type, vdev_width):
   num_vdevs = math.floor(TOTAL_NUM_DISKS / vdev_width)
   
   if vdev_type == "mirror" and vdev_width == 2:
      num_vdevs -= 1
   
   return num_vdevs

# Returns the AFR of the pool based on the vdev configuration and AFR of the individual disks
def get_pool_afr(vdev_width, parity_level, num_vdevs, disk_AFR, resilver_time_sec):
   # Generate probability matrix
   diskP = []
   for i in range(0, parity_level + 1):
      if i == 0:
         diskP.append(disk_AFR)
      else:
         diskP.append(disk_AFR * resilver_time_sec * i / (365 * 24 * 60 * 60))

   # Calculate the probability of 1, then 2, then ..., then p+1 disk failures (just enough to kill the vdev)
   pool_P = 1

   for num_disk_failures in range(1,parity_level + 2):
      p = diskP[num_disk_failures - 1]
      p_fail = (vdev_width - (num_disk_failures - 1)) * p
      pool_P *= p_fail

   return pool_P * num_vdevs

# SIGINT and SIGTERM handler
def kill(signum, frame):
   global log
   global f

   log.info("Exiting...")

   # Kill any running instances of fio
   try:
      subprocess.check_output("pkill fio",shell=True)
   except:
      pass

   # Close file
   f.close()
   
   # Exit
   sys.exit(0)

if __name__ == '__main__':
   # SIGINT and SIGTERM handlers
   signal.signal(signal.SIGTERM,kill)
   signal.signal(signal.SIGINT,kill)

   main()

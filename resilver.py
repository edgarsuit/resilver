#!/bin/python3

# resilver.py
# Test resilvering times on different ZFS layouts, varies recordsize, fill percent, and fragmentation levels
# Runs resilver with and without CPU and disk stress
# Outputs results to a CSV file
# Uses pool name "tank" and dataset name "test"
# Uses FIO for CPU and disk stress as well as pool fill

import subprocess, shlex, math, time, os, random, csv, signal, sys, logging

fill_percent = 70				# Target fill percent
physical_disk_size = "7.3T" 	# Size of physical disks
format_disks = True				# Format disks before creating pool
format_size = "100G"			# Size to format disks to
target_disk = "sdbz"			# Disk to offline/online dring testing
results_file = "output.csv" 	# Output file
log_file = "resilver.log"		# Log file
skip_pool_setup = False			# Skip pool setup; assume it's already created and filled for first run

# starting_run can be used to resume testing from a specific run number
# First value is the layout, second is the fragmentation level, and third is the test schedule
# [0,0,0] starts from the beginning
starting_test = [0, 0, 0]

# ZFS layouts to test
# layout: ZFS layout
# width: vdev width
# recordsize: ZFS recordsize
# minspares: Minimum number of spares in the vdev
layouts = [
	{"layout": "draid2:32d:82c:2s", "width": 82, "recordsize": "1M",   "minspares": 0}, 	# 0
	{"layout": "draid2:16d:82c:2s", "width": 82, "recordsize": "1M",   "minspares": 0}, 	# 1
	{"layout": "draid2:8d:82c:2s",  "width": 82, "recordsize": "1M",   "minspares": 0}, 	# 2
	{"layout": "draid2:4d:82c:2s",  "width": 82, "recordsize": "1M",   "minspares": 0}, 	# 3
	{"layout": "draid2:32d:41c:1s", "width": 41, "recordsize": "1M",   "minspares": 0}, 	# 4
	{"layout": "draid2:16d:41c:1s", "width": 41, "recordsize": "1M",   "minspares": 0}, 	# 5
	{"layout": "draid2:8d:41c:1s",  "width": 41, "recordsize": "1M",   "minspares": 0}, 	# 6
	{"layout": "draid2:4d:41c:1s",  "width": 41, "recordsize": "1M",   "minspares": 0}, 	# 7
	{"layout": "raidz2",            "width": 40, "recordsize": "1M",   "minspares": 0}, 	# 8
	{"layout": "raidz2",            "width": 20, "recordsize": "1M",   "minspares": 0}, 	# 9
	{"layout": "raidz2",            "width": 10, "recordsize": "1M",   "minspares": 0}, 	# 10
	{"layout": "raidz2",            "width": 5,  "recordsize": "1M",   "minspares": 0}, 	# 11
	{"layout": "raidz3",            "width": 10, "recordsize": "1M",   "minspares": 0}, 	# 12
	{"layout": "raidz1",            "width": 10, "recordsize": "1M",   "minspares": 0}, 	# 13
	{"layout": "mirror",            "width": 2,  "recordsize": "1M",   "minspares": 1}, 	# 14
	{"layout": "mirror",            "width": 3,  "recordsize": "1M",   "minspares": 1}, 	# 15
	{"layout": "draid2:32d:82c:2s", "width": 82, "recordsize": "128k", "minspares": 0}, 	# 16
	{"layout": "draid2:16d:82c:2s", "width": 82, "recordsize": "128k", "minspares": 0}, 	# 17
	{"layout": "draid2:8d:82c:2s",  "width": 82, "recordsize": "128k", "minspares": 0}, 	# 18
	{"layout": "draid2:4d:82c:2s",  "width": 82, "recordsize": "128k", "minspares": 0}, 	# 19
	{"layout": "draid2:32d:41c:1s", "width": 41, "recordsize": "128k", "minspares": 0}, 	# 20
	{"layout": "draid2:16d:41c:1s", "width": 41, "recordsize": "128k", "minspares": 0}, 	# 21
	{"layout": "draid2:8d:41c:1s",  "width": 41, "recordsize": "128k", "minspares": 0}, 	# 22
	{"layout": "draid2:4d:41c:1s",  "width": 41, "recordsize": "128k", "minspares": 0}, 	# 23
	{"layout": "raidz2",            "width": 40, "recordsize": "128k", "minspares": 0}, 	# 24
	{"layout": "raidz2",            "width": 20, "recordsize": "128k", "minspares": 0}, 	# 25
	{"layout": "raidz2",            "width": 10, "recordsize": "128k", "minspares": 0}, 	# 26
	{"layout": "raidz2",            "width": 5,  "recordsize": "128k", "minspares": 0}, 	# 27
	{"layout": "raidz3",            "width": 10, "recordsize": "128k", "minspares": 0}, 	# 28
	{"layout": "raidz1",            "width": 10, "recordsize": "128k", "minspares": 0}, 	# 29
	{"layout": "mirror",            "width": 2,  "recordsize": "128k", "minspares": 1}, 	# 30
	{"layout": "mirror",            "width": 3,  "recordsize": "128k", "minspares": 1} 		# 31
]

# Fragmentation levels to test on each configuration
frag_schedule = [
	"none",		# 0
	"med",		# 1
	"high"		# 2
]

# Tests to run on each configuration
# cpu: CPU stress level
# disk: Disk stress level
test_schedule = [
	{"cpu": "none", "disk": "none"},	# 0
	{"cpu": "med",  "disk": "none"},	# 1
	{"cpu": "high", "disk": "none"},	# 2
	{"cpu": "none", "disk": "med" },	# 3
	{"cpu": "none", "disk": "high"},	# 4
	{"cpu": "med",  "disk": "med" },	# 5
	{"cpu": "high", "disk": "high"}		# 6
]

# Main function
# Formats disks if needed, itterates through layouts, fragmentation levels, and tests
# Writes results to a CSV file
def main():
	global log
	global skip_pool_setup
	global test_index

	# Rotate out previous log file (if it exists)
	try:
		os.rename(log_file,log_file + ".old")
	except:
		pass

	# Set up logging
	logging.basicConfig(filename=log_file,encoding="utf-8",level=logging.INFO)
	log = logging.getLogger()
	log_format = logging.Formatter("%(asctime)s %(message)s",datefmt="%Y-%m-%d %H:%M:%S")
	log.setFormatter(log_format)
	log.addHandler(logging.StreamHandler(sys.stdout))

	# Setup output file
	global f
	# Check if output file exists and has a header row
	if os.path.isfile(results_file):
		f = open(results_file,"r")
		header = f.readline().strip()
		if "Test Index" in header:
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
		# Results file does not exist, create new one
		f = open(results_file,"w")
		results = set_up_csv(f)

	# Format disks if needed; destroy pool (if exists) before formatting
	if format_disks == True and skip_pool_setup == False:
		destroy_pool()
		format(format_size)

	# Kill old instances of fio
	try:
		subprocess.check_output("pkill fio",shell=True)
	except:
		pass

	# Calculate total number of tests to run
	total_tests = len(layouts)*len(frag_schedule)*len(test_schedule)

	# Itterate through layouts
	for layout in layouts[starting_test[0]:]:
		log.info("Starting layout: " + layout["layout"])
		
		# Itterate through fragmentation levels
		for frag in frag_schedule[starting_test[1]:]:

			if skip_pool_setup == True:
				# If we are resuming from a test, skip pool setup
				# Fill speed returned by fill_pool() is not available if we skip pool setup
				fill_speed = "N/A"

				# Check if pool is healthy; if not, attempt to recover
				if get_resilver_status() != "healthy":
					log.info("Pool is not healthy; attempting to recover")
					online_disk(target_disk)
					log.info("Sleeping 30 seconds...")
					time.sleep(30)
					while get_resilver_status() != "healthy":
						# If pool is resilvering, sleep until resilver is complete
						try:
							eta = get_resilver_status()[4]
							log.info("Pool is still not healthy; waiting for resilver to complete, ETA " + eta)
						except:
							pass
						log.info("Sleeping 30 seconds...")
						time.sleep(30)

				# We don't want to skip pool setup on the next fragmentation itteration; set skip_pool_setup to False
				skip_pool_setup = False
			else:
				# If we are starting from the beginning (or moving to the next frag level), recreate and refill pool
				destroy_pool()
				create_pool(layout["layout"],layout["width"],layout["recordsize"],layout["minspares"])
				# fill_pool() returns the speed at which the pool was filled
				fill_speed = fill_pool(fill_percent,frag) + "G/s"

			# Gather pool status for results CSV
			zfs_status = subprocess.check_output("zfs list tank/test -Hpo used,available",shell=True).decode("utf-8")
			used = int(zfs_status.split()[0])
			used_tib = round(used/1024**4,2)
			avail = int(zfs_status.split()[1])
			avail_tib = round(avail/1024**4,2)
			used_percent = round(used/(used+avail)*100,2)
			pool_size = used + avail
			pool_size_tib = round(pool_size/1024**4,2)
			frag_percent = subprocess.check_output("zpool list -Hpo frag tank",shell=True).decode("utf-8").strip()

			# Before starting the tests, export and import the pool to clear ARC data
			log.info("Exporting and importing pool to clear ARC data...")
			subprocess.check_output("zpool export tank",shell=True)
			subprocess.check_output("zpool import tank",shell=True)

			# Once pool is filled with appropriate fragmentation level, itterate through tests
			for test in test_schedule[starting_test[2]:]:
				test_number = layouts.index(layout)*len(frag_schedule)*len(test_schedule) + frag_schedule.index(frag)*len(test_schedule) + test_schedule.index(test)
				test_index = "[" + str(layouts.index(layout)) + ", " + str(frag_schedule.index(frag)) + ", " + str(test_schedule.index(test)) + "]"
				# Print test index
				log.info("Starting test index " + test_index + " (" + str(test_number) + "/" + str(total_tests) + ")")

				# Start CPU and disk stress. If stress is set to "none", these functions will return 0
				disk_stress_handle = disk_stress(test["disk"])
				cpu_stress_handle = cpu_stress(test["cpu"])

				# Sleep for 65 seconds and get 1m system load average
				print("Monitoring system load average...")
				time.sleep(65)
				load_avg_before_resilver = subprocess.check_output("cat /proc/loadavg",shell=True).decode("utf-8").strip().split()[0]

				# Offline target disk, wait for 5 seconds before checking resilver status
				offline_disk(target_disk)
				time.sleep(5)

				# Set up average speed tracking variables
				scan_speed_avg = 0
				scan_sample_count = 1
				issue_speed_avg = 0
				issue_sample_count = 1

				# Set up for load average collection ~60 seconds into resilver
				run_start = time.time()
				load_avg_collected = False

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

					# Check system load average 120 seconds into resilver
					if load_avg_collected == False and time.time() - run_start > 120:
						load_avg_during_resilver = subprocess.check_output("cat /proc/loadavg",shell=True).decode("utf-8").strip().split()[0]
						load_avg_collected = True

					percent_done = resilver_status[3]
					time_left = resilver_status[4]
					log.info(test_index + " Resilvering: " + percent_done + " (" + issue_speed + ", ETA " + time_left + ")")
					time.sleep(5)

					# When resilver is at 100%, zpool status output can cause parse issues.
					# If we fail to parse, wait 5 seconds and try again
					try:
						resilver_status = get_resilver_status()
					except:
						time.sleep(5)
						resilver_status = get_resilver_status()
				
				# Print resilver results
				log.info("Resilver complete in " + resilver_status[1] + " | " + resilver_status[3] + " resilvered")

				# If load average was not collected during the resilver, collect it now
				if load_avg_collected == False:
					load_avg_during_resilver = subprocess.check_output("cat /proc/loadavg",shell=True).decode("utf-8").strip().split()[0]

				# Calculate resilver time in seconds
				resilver_time_seconds = int(resilver_status[1].split(":")[0])*60*60 + int(resilver_status[1].split(":")[1])*60 + int(resilver_status[1].split(":")[2])

				# Terminate stress tests
				if disk_stress_handle != 0:
					disk_stress_handle.terminate()
					log.info("Disk stress terminated")
				if cpu_stress_handle != 0:
					cpu_stress_handle.terminate()
					log.info("CPU stress terminated")

				# Clean up scan and issue speed values if needed
				if scan_speed_avg == 0: scan_speed_avg = "-"
				if issue_speed_avg == 0: issue_speed_avg = "-"
				
				# Write results from this run to CSV
				results.writerow([
					test_index,					# Test Index
					layout["layout"],			# Layout
					layout["width"],			# Width
					layout["recordsize"],		# Recordsize
					fill_percent + "%",			# Target Fill Percent
					used,						# Used (bytes)
					used_tib,					# Used (TiB)
					avail,						# Available (bytes)
					avail_tib,					# Available (TiB)
					used_percent + "%",			# Used Percent
					pool_size,					# Pool Size (bytes)
					pool_size_tib,				# Pool Size (TiB)
					frag_percent + "%",			# Fragmentation Percent
					format_size,				# Disk Size
					frag,						# Fragmentation Level
					test["cpu"],				# CPU Stress
					test["disk"],				# Disk Stress
					resilver_status[1],			# Resilver Time
					resilver_time_seconds,		# Resilver Time (seconds)
					resilver_status[2],			# Scanned
					scan_speed_avg,				# Scan Speed (M/s)
					resilver_status[3],			# Issued
					issue_speed_avg,			# Issue Speed (M/s)
					fill_speed,					# Fill Speed
					load_avg_before_resilver,	# 1m Load Avg. Baseline
					load_avg_during_resilver	# 1m Load Avg. During Resilver
				])
				f.flush()

				# Online target disk; data hasn't changed to resilvering should happen in <1 second
				online_disk(target_disk)

				# Sleep for 30 seconds between tests
				log.info("Sleeping 30 seconds...")
				time.sleep(30)
	
	# Close output file after all tests completed
	f.close()

# Set up the CSV file with headers
def set_up_csv(f):
	results = csv.writer(f)
	results.writerow([
		"Test Index",
		"Layout",
		"Width",
		"Recordsize",
		"Target Fill %",
		"Used (bytes)",
		"Used (TiB)",
		"Available (bytes)",
		"Available (TiB)",
		"Used Percent",
		"Pool Size (bytes)",
		"Pool Size (TiB)",
		"Frag. Percent",
		"Disk Size",
		"Frag. Level",
		"CPU Stress",
		"Disk Stress",
		"Resilver Time",
		"Resilver Time (sec.)",
		"Scanned",
		"Scan Speed (M/s)",
		"Issued",
		"Issue Speed (M/s)",
		"Fill Speed",
		"1m Load Avg. Baseline",
		"1m Load Avg. During"])
	return results

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
		--directory=/mnt/tank/test/diskstress/ \
		--group_reporting \
		--unified_rw_reporting=both \
		--name=diskstress \
	"""
	# We high disk utilization with more jobs and smaller block sizes
	if disk_load == "high":
		cmd += "--numjobs=256 \\"
		cmd += "--bs=4Ki \\"
		cmd += "--filesize=1Mi"
	# We get moderate utilization with fewer jobs and larger block sizes
	elif disk_load == "med":
		cmd += "--numjobs=1 \\"
		cmd += "--bs=1Mi \\"
		cmd += "--filesize=1Mi"
		
	# Set sync=always on the dataset so I/O is not buffered in memory
	subprocess.run("zfs set sync=always tank/test",shell=True)

	# Remove any previous disk stress files and create a new directory for the stress test files
	subprocess.run("rm -rf /mnt/tank/test/diskstress",shell=True)
	subprocess.run("mkdir /mnt/tank/test/diskstress",shell=True)

	# Start disk stress and return process handle
	proc = subprocess.Popen(shlex.split(cmd),stdout=subprocess.DEVNULL)
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
	if spares != "":
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
	try:
		start = time.time()
		log.info("Destroying pool...")
		subprocess.check_output("zpool destroy tank",shell=True,stderr=subprocess.DEVNULL)
		time_taken = time.time() - start
		log.info("Destroyed pool in " + sec_to_dhms(time_taken))
	except:
		pass

# Fill pool to a specified percentage with a specified fragmentation level
# Moderate (~30%) and high fragmentation (~50%) levels are achieved by writing small, unaligned blocks to fill up the pool
# to 100% and then randomly deleting a certain percentage of those files to get back to the specified fill percentage.
# If fragmentation is "none", sequentially fill the pool
def fill_pool(fill_percent,frag_level):
	global log
	start = time.time()

	# Set sync=disabled on the dataset for faster fill
	subprocess.run("zfs set sync=disabled tank/test",shell=True)
	
	# Create a directory for the fill files
	subprocess.run("mkdir /mnt/tank/test/fill",shell=True)
	
	# We will track the pool percent used as well as the run number duing the fill loop
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
		prune_percent = 100-fill_percent
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
			rate_gbps = round(rate_bps/1000**3,2)
			if rate_bps == 0:
				rate_bps = 1
			size_left = fill_size - used
			time_left_sec = size_left/rate_bps
			if time_left_sec < 0: time_left_sec = 0

			# Calculate pool fill percentage and print status
			used_tib_str =  "{:.2f}".format(round(used/1024**4,2))
			total_tib_str =  "{:.2f}".format(round((used+avail)/1024**4,2))
			percent_used_str = "{:.2f}".format(percent_used)
			rate_gbps_str = "{:.2f}".format(rate_gbps)
			log.info(test_index + " Filling (frag @ " + frag_level + "): " + used_tib_str + "TiB/" + total_tib_str + "TiB >> " + percent_used_str + "% (" + rate_gbps_str + "G/s | ETA " + sec_to_dhms(time_left_sec) + ")")
			
			# If pool is filled to the specified percentage, terminate the fill process and break the loop
			
			if percent_used >= fill_percent:
				proc.terminate()
				break
			time.sleep(5)
		
		# Increment the run number and start the next fill run
		run_number += 1
	
	# Calculate and print the time taken to fill the pool
	time_taken = time.time() - start
	log.info("Filled pool in " + sec_to_dhms(time_taken))

	# If fragmentation is specified, prune the specified percentage of files
	if frag_level == "high" or frag_level == "med":
		start = time.time()
		log.info("Pruning " + str(prune_percent) + "% of files")

		# Get the current pool status and calculate the size to prune to achieve the specified fill percentage
		zfs_status = subprocess.check_output("zfs list tank/test -Hpo used,available",shell=True).decode("utf-8")
		used = int(zfs_status.split()[0])
		avail = int(zfs_status.split()[1])
		total = used + avail
		prune_size = round(total * prune_percent/100)

		# Adjust the target prune percentage to overshoot and terminate early (otherwise we may prune slightly too few or too many files)
		prune_percent += 5

		# Get file count
		file_count = int(subprocess.check_output("find /mnt/tank/test/fill/ -type f | wc -l",shell=True).decode("utf-8"))

		# Prune random files until the specified size is reached
		pruned_size_so_far = 0
		pruned_count_so_far = 0
		for root, dirs, files in os.walk("/mnt/tank/test/fill/"):
			for file in files:
				if random.random() < prune_percent/100:
					pruned_size_so_far += os.path.getsize(os.path.join(root,file))
					# Once we have pruned enough files to get down to the specified fill percentage, break the loop
					if pruned_size_so_far >= prune_size:
						break
					else:
						os.remove(os.path.join(root,file))
						pruned_count_so_far += 1
						pruned_percent = round(pruned_count_so_far/file_count*100,2)
						if pruned_percent % 1 == 0:
							log.info(test_index + " Pruned " + str(pruned_percent) + "% of files: " + str(pruned_count_so_far) + "/" + str(file_count))

		# Calculate and print the time taken to prune the files
		time_taken = time.time() - start
		log.info("Files pruned in " + sec_to_dhms(time_taken))

	# Return the average fill speed
	return str(rate_gbps)

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
	
	# Parsing the zpool status output often fails, so write the output to a file for debugging
#	with open("zpool_status_debug","w") as f:
#		f.write(resilver_status)

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

# SIGINT and SIGTERM handler
def kill(signum, frame):
	global log

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
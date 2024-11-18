source("analysis_functions.r")

red <- "#E06666"
yellow <- "#FFD966"
green <- "#93C47D"

resilver_results_old <- read.csv("firstrun.csv")
resilver_results_new <- read.csv("output.csv")
names(resilver_results_old) <- names(resilver_results_new)
resilver_results <- rbind(resilver_results_old, resilver_results_new)

raidz2 <- resilver_results %>% filter(VdevType == "raidz2" & NumHotSpares == 2)
raidz <- resilver_results %>% filter((VdevType == "raidz1" | VdevType == "raidz2" | VdevType == "raidz3") & NumHotSpares == 2)

plot_3group_set(
   raidz2,
   "RAIDZ2",
   "ResilverTimeMinutes",
   "VdevWidth",
   "RecordSize",
   "128k",
   "1M"
)

plot_2group(
   raidz2,
   "RAIDZ2",
   "ResilverTimeMinutes",
   "VdevWidth",
   "RecordSize"
)

plot_2group(
   raidz,
   "RAIDZ",
   "ResilverTimeMinutes",
   "VdevWidth",
   "VdevType"
)

plot_2group(
   raidz,
   "RAIDZ",
   "ResilverTimeMinutes",
   "PoolSizeTiB",
   "VdevType"
)

for (i in 1:10) {
   plot_2group(
      raidz2,
      "RAIDZ2",
      paste("PoolAFR",as.character(i),"percent100x", sep=""),
      "VdevWidth",
      "RecordSize"
   )
}

draid <- resilver_results %>% filter(VdevWidth == 82 & RecordSize == "1M" & (VdevType == "draid1" | VdevType == "draid2" | VdevType == "draid3"))

plot_2group(
   draid,
   "dRAID",
   "ResilverTimeMinutes",
   "dRAIDDataDisks",
   "VdevType"
)

draid2 <- resilver_results %>% filter(VdevType == "draid2")

plot_3group_set(
   draid2,
   "dRAID2",
   "ResilverTimeMinutes",
   "dRAIDDataDisks",
   "VdevWidth",
   "41",
   "82"
)

draid3 <- resilver_results %>% filter(VdevType == "draid3")

plot_3group_set(
   draid3,
   "dRAID3",
   "ResilverTimeMinutes",
   "dRAIDDataDisks",
   "VdevWidth",
   "41",
   "82"
)

draid1 <- resilver_results %>% filter(VdevType == "draid1")

plot_3group_set(
   draid1,
   "dRAID1",
   "ResilverTimeMinutes",
   "dRAIDDataDisks",
   "VdevWidth",
   "41",
   "82"
)

for (i in 1:10) {
   plot_3group_set(
      draid2,
      "dRAID2",
      paste("PoolAFR",i,"percent100x", sep=""),
      "dRAIDDataDisks",
      "VdevWidth",
      "41",
      "82"
   )
}

plot_2group(
   draid2,
   "dRAID2",
   "ResilverTimeMinutes",
   "dRAIDDataDisks",
   "VdevWidth"
)

for (i in 1:10) {
   plot_2group(
      draid2,
      "dRAID2",
      paste("PoolAFR",i,"percent100x", sep=""),
      "dRAIDDataDisks",
      "VdevWidth"
   )
}

# Add a column to the data frame called "EffectiveWidth", if VdevType="dRAID2" then it is dRAIDDataDisks + ParityLevel, if VdevType="raidz2" then it is VdevWidth
resilver_results <- resilver_results %>%
   mutate(EffectiveWidth = ifelse(VdevType == "draid2", as.integer(dRAIDDataDisks) + ParityLevel, VdevWidth))

draid_raidz2 <- resilver_results %>% filter(RecordSize == "1M" & (VdevType == "draid2" | VdevType == "raidz2"))
draid_raidz2 <- draid_raidz2 %>% filter(!(VdevType == "draid2" & VdevWidth == 41))

plot_3group_set(
   draid_raidz2,
   "dRAID & RAIDZ",
   "ResilverTimeMinutes",
   "EffectiveWidth",
   "VdevType",
   "dRAID2",
   "raidz2"
)

for (i in 1:10) {
   print(i)
   plot_3group_set(
      draid_raidz2,
      "dRAID & RAIDZ",
      paste("PoolAFR",i,"percent100x", sep=""),
      "EffectiveWidth",
      "VdevType",
      "dRAID2",
      "raidz2"
   )
}

for (i in 1:10) {
   print(i)
   plot_3group_set(
      draid_raidz2,
      "dRAID & RAIDZ",
      paste("PoolAFR",i,"percent100x", sep=""),
      "PoolSizeTiB",
      "VdevType",
      "dRAID2",
      "raidz2"
   )
}

frag_array <- c("None", "Med", "High")
stress_array <- c("None", "Med", "High")

frag_array <- c("None")
stress_array <- c("None")

for (frag in frag_array) {
   for (stress in stress_array) {
      AFR_plots_by_fragstress(frag, stress, 41)
      AFR_plots_by_fragstress(frag, stress, 82)
      AFR_plots_by_draid_width(frag, stress)
   }
}

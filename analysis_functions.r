library(dplyr)
library(tidyr)
library(ggplot2)
library(scales)
library(stringr)

plot_3group <- function(df, datatype, ydata, group1, group2, group3, dashed_data, solid_data) {
   medians <- df %>%
      group_by(!!sym(group1), !!sym(group2), !!sym(group3)) %>%
      summarize(median_sample = median(!!sym(ydata)))

   medians[[group1]] <- as.double(medians[[group1]])
   medians[[group3]] <- as.character(medians[[group3]])

   if (ydata == "ResilverTimeMinutes") {
      ytitle <- paste("Median ", datatype, " Resilver Time", sep = "")
      yaxislabel <- "Median Resilver Time (Minutes)"
   } else if (grepl("PoolAFR", ydata)) {
      afr_value <- str_extract(ydata, "[0-9]+")
      ytitle <- paste("Median ", datatype, " Pool AFR at ", afr_value, "% Disk AFR", sep = "")
      if (grepl("100x", ydata)) {
         ytitle <- paste(ytitle, " (100x resilver time)")
      }
      yaxislabel <- paste("Median Pool AFR at ", afr_value, "% Disk AFR")
   }

   if (group1 == "VdevWidth") {
      title1 <- "Vdev Width"
   } else if (group1 == "dRAIDDataDisks") {
      title1 <- "dRAID Data Disks"
   } else if (group1 == "EffectiveWidth") {
      title1 <- "Effective Width"
   } else if (group1 == "PoolSizeTiB") {
      title1 <- "Pool Size (TiB)"
   }

   if (group2 == "FragLevel") {
      title2 <- "Frag. Level"
   } else if (group2 == "CPUStress") {
      title2 <- "CPU Stress"
   } else if (group2 == "DiskStress") {
      title2 <- "Disk Stress"
   }

   if (group3 == "RecordSize") {
      title3 <- "Record Size"
   } else if (group3 == "VdevWidth") {
      title3 <- "Vdev Width"
   } else if (group3 == "VdevType") {
      title3 <- "Vdev Type"
   }

   plot <- ggplot(medians, aes(x = !!sym(group1), y = median_sample, color = !!sym(group2), linetype = !!sym(group3))) +
      geom_point() +
      geom_line() +
      scale_color_manual(values = c("high" = red, "med" = yellow, "none" = green)) +
      scale_linetype_manual(values = setNames(c("dashed", "solid"), c(dashed_data, solid_data))) +
      labs(title = paste(ytitle," vs. ", title1, sep = ""),
         subtitle = paste("by ", title2, " and ", title3, sep = ""),
         x = title1,
         y = yaxislabel,
         color = title2,
         linetype = title3) +
      theme_light() + 
      theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))

   if (grepl("PoolAFR", ydata)) {
      plot <- plot + scale_y_continuous(labels = label_percent(scale = 100))
   }

   output_plot <- paste(ydata, "_by_", group1, "_", group2, "_", group3, ".png",sep = "")

   ggsave(paste("plots/",output_plot, sep = ""), plot, width = 10, height = 10, unit="in")
}

plot_2group <- function(df, datatype, ydata, group1, group2) {
   medians <- df %>%
      group_by(!!sym(group1), !!sym(group2)) %>%
      summarize(median_sample = median(!!sym(ydata)))

   medians[[group1]] <- as.double(medians[[group1]])
   medians[[group2]] <- as.character(medians[[group2]])

   if (ydata == "ResilverTimeMinutes") {
      ytitle <- paste("Median ", datatype, " Resilver Time", sep = "")
      yaxislabel <- "Median Resilver Time (Minutes)"
   } else if (grepl("PoolAFR", ydata)) {
      afr_value <- str_extract(ydata, "[0-9]+")
      ytitle <- paste("Median ", datatype, " Pool AFR at ", afr_value, "% Disk AFR", sep = "")
      if (grepl("100x", ydata)) {
         ytitle <- paste(ytitle, " (100x resilver time)")
      }
      yaxislabel <- paste("Median Pool AFR at ", afr_value, "% Disk AFR")
   }

   if (group1 == "VdevWidth") {
      title1 <- "Vdev Width"
   } else if (group1 == "dRAIDDataDisks") {
      title1 <- "dRAID Data Disks"
   } else if (group1 == "EffectiveWidth") {
      title1 <- "Effective Width"
   } else if (group1 == "PoolSizeTiB") {
      title1 <- "Pool Size (TiB)"
   }

   if (group2 == "RecordSize") {
      title2 <- "Record Size"
   } else if (group2 == "VdevWidth") {
      title2 <- "Vdev Width"
   } else if (group2 == "VdevType") {
      title2 <- "Vdev Type"
   }

   plot <- ggplot(medians, aes(x = !!sym(group1), y = median_sample, color = !!sym(group2))) +
      geom_point() +
      geom_line() +
      labs(title = paste(ytitle," vs. ", title1," by ", title2, sep = ""),
         x = title1,
         y = yaxislabel,
         color = title2) +
      theme_light() + 
      theme(plot.title = element_text(hjust = 0.5))

   output_plot <- paste(ydata, "_by_", group1, "_", group2, ".png",sep = "")

   if (grepl("PoolAFR", ydata)) {
      plot <- plot + scale_y_continuous(labels = label_percent(scale = 100))
   }

   ggsave(paste("plots/",output_plot, sep = ""), plot, width = 10, height = 10, unit="in")
}

plot_3group_set <- function(df, datatype, ydata, group1, group3, dashed_data, solid_data) {
   plot_3group(
      df,
      datatype,
      ydata,
      group1,
      "FragLevel",
      group3,
      dashed_data,
      solid_data
   )
   plot_3group(
      df,
      datatype,
      ydata,
      group1,
      "DiskStress",
      group3,
      dashed_data,
      solid_data
   )
   plot_3group(
      df,
      datatype,
      ydata,
      group1,
      "CPUStress",
      group3,
      dashed_data,
      solid_data
   )
}

pool_afr_labeller <- function(PoolAFR) {
   afr_value <- str_extract(PoolAFR, "\\d+")
   paste(afr_value, "% Disk AFR", sep = "")
}

AFR_plots_by_fragstress <- function(frag, stress, draid_vdevwidth) {

   if (frag == "None") {
      frag_label <- "No"
   } else {
      frag_label <- frag
   }

   if (stress == "None") {
      stress_label <- "No"
   } else {
      stress_label <- stress
   }

   AFR_data <- resilver_results %>% filter(VdevType == "draid2" | VdevType == "raidz2")
   AFR_data <- AFR_data %>% filter(!(VdevType == "raidz2" & NumHotSpares > 2))
   AFR_data <- AFR_data %>% filter(!(VdevType == "draid2" & VdevWidth == draid_vdevwidth))
   AFR_data <- AFR_data %>% filter(RecordSize == "1M")
   AFR_data <- AFR_data %>% filter(FragLevel == tolower(frag) & DiskStress == tolower(stress) & CPUStress == tolower(stress))
   
   afr_pivot <- AFR_data %>%
      pivot_longer(cols = matches("PoolAFR[1-9]+percent100x"), 
         names_to = "PoolAFR", 
         values_to = "Percent100x")

   # Create the plot, grouping by the median value
   plot <- ggplot(afr_pivot, aes(x = EffectiveWidth, y = Percent100x, color = PoolAFR, linetype = VdevType)) +
      geom_line() +
      scale_linetype_manual(values = c("dRAID2" = "dashed", "raidz2" = "solid")) +
      coord_cartesian(ylim = c(0, 0.02)) +
      facet_wrap(~ PoolAFR, scales = "free_y", labeller = pool_afr_labeller) +
      scale_y_continuous(labels = label_percent(scale = 100)) +
      labs(title = paste("dRAID2 and RAIDZ2 Pool AFR by Effective Width", " (", frag_label, " Frag, ", stress_label, " Stress) ", draid_vdevwidth, "-wide dRAID vdevs",  sep = ""),
         subtitle = "Effective Width: RAIDZ2 = vdev width; dRAID2 = data disks + parity level",
         x = "Effective Width",
         y = "Pool AFR Percent") +
      theme_light() +
      theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))

   ggsave(paste("plots/PoolAFR_EffectiveWidth_",frag_label,"Frag_", stress_label, "Stress", draid_vdevwidth, "widedRAID.png", sep = ""), plot = plot, width = 10, height = 10, unit="in")

   # Create the plot, grouping by the median value
   plot <- ggplot(afr_pivot, aes(x = PoolSizeTiB, y = Percent100x, color = PoolAFR, linetype = VdevType)) +
      geom_line() +
      scale_linetype_manual(values = c("dRAID2" = "dashed", "raidz2" = "solid")) +
      coord_cartesian(ylim = c(0, 0.02)) +
      facet_wrap(~ PoolAFR, scales = "free_y", labeller = pool_afr_labeller) +
      scale_y_continuous(labels = label_percent(scale = 100)) +
      labs(title = paste("dRAID2 and RAIDZ2 Pool AFR by Usable Capaicity (" , frag_label, " Frag, ", stress_label, " Stress) ", draid_vdevwidth, "-wide dRAID vdevs", sep = ""),
         x = "Usable Capacity (TiB)",
         y = "Pool AFR Percent") +
      theme_light() +
      theme(plot.title = element_text(hjust = 0.5))

   ggsave(paste("plots/PoolAFR_UsableCap_",frag_label,"Frag_", stress_label, "Stress_", draid_vdevwidth, "widedRAID.png", sep = ""), plot = plot, width = 10, height = 10, unit="in")
}

AFR_plots_by_draid_width <- function(frag, stress) {

   if (frag == "None") {
      frag_label <- "No"
   } else {
      frag_label <- frag
   }

   if (stress == "None") {
      stress_label <- "No"
   } else {
      stress_label <- stress
   }

   AFR_data <- resilver_results %>% filter(VdevType == "draid2")
   AFR_data <- AFR_data %>% filter(RecordSize == "1M")
   AFR_data <- AFR_data %>% filter(FragLevel == tolower(frag) & DiskStress == tolower(stress) & CPUStress == tolower(stress))
   
   AFR_data[["dRAIDDataDisks"]] <- as.integer(AFR_data[["dRAIDDataDisks"]])
   AFR_data[["VdevWidth"]] <- as.character(AFR_data[["VdevWidth"]])
   
   afr_pivot <- AFR_data %>%
      pivot_longer(cols = matches("PoolAFR[1-9]+percent100x"), 
         names_to = "PoolAFR", 
         values_to = "Percent100x")

   # Create the plot, grouping by the median value
   plot <- ggplot(afr_pivot, aes(x = dRAIDDataDisks, y = Percent100x, color = PoolAFR, linetype = VdevWidth)) +
      geom_line() +
      scale_linetype_manual(values = c("41" = "dashed", "82" = "solid")) +
      coord_cartesian(ylim = c(0, 0.02)) +
      facet_wrap(~ PoolAFR, scales = "free_y", labeller = pool_afr_labeller) +
      scale_y_continuous(labels = label_percent(scale = 100)) +
      labs(title = paste("dRAID2 Pool AFR by Vdev Width and dRAID Data Disk Qty.", " (", frag_label, " Frag, ", stress_label, " Stress)", sep = ""),
         x = "dRAID Data Disks",
         y = "Pool AFR Percent") +
      theme_light() +
      theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))

   ggsave(paste("plots/dRAIDPoolAFR_VdevWidth_",frag_label,"Frag_", stress_label, "Stress.png", sep = ""), plot = plot, width = 10, height = 10, unit="in")
}
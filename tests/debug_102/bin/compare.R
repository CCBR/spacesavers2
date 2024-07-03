#!/usr/bin/env Rscript
library(tidyverse)

#' Convert to and from bytes
#'
#' @param x numeric value
#' @param unit metric unit to convert from or to.
#'   supported options: `KiB`, `MiB`, `GiB`, `TiB`
#'
#' @export
#'
#' @examples
#' to_bytes(1, "KiB")
#' from_bytes(1024^3, "GiB")
#'
to_bytes <- function(x, unit) {
  bytes_units <- list(
    KiB = 1,
    MiB = 2,
    GiB = 3,
    TiB = 4
  )
  return(x * (1024^bytes_units[[unit]]))
}

#' @rdname to_bytes
#' @export
from_bytes <- function(x, unit) {
  return(x * x / (to_bytes(x, unit)))
}
#' @rdname to_bytes
#' @export
from_bytes_v <- Vectorize(from_bytes)
#' @rdname to_bytes
#' @export
to_bytes_v <- Vectorize(to_bytes)

# main script
dat_du <- read_tsv('du.tsv') %>%
  mutate(
    usage_TiB_du = from_bytes(to_bytes(usage, "KiB"), "TiB")
  ) %>%
  rename(FolderPath = proj_dir) %>%
  select(FolderPath, usage_TiB_du)

dat_spacesavers <- list.files(list.dirs('spacesavers', recursive = FALSE), full.names = TRUE) %>%
  Filter(function(x) { str_detect(x, 'allusers.mimeo.summary') }, .) %>%
  map(read_tsv) %>%
  list_rbind()  %>%
  mutate(
    usage_TiB_spacesavers = from_bytes(TotalBytes, "TiB")
  ) %>%
  select(FolderPath, usage_TiB_spacesavers)

dat_spacesavers %>% full_join(dat_du)


RDF_PATH = "/home/lz80/rdf/sp161/shared/soccer-decision-making-r/sportec/"

#' Compute windowed cumulative xG for offensive or defensive context
#'
#' For each row `i`, this function looks ahead up to `n_next` subsequent rows,
#' excluding rows with the same frame number as row `i`. It then keeps only
#' teammate events (`offense = TRUE`) or opponent events (`offense = FALSE`)
#' based on `team`, multiplies their `complement_xg` values, and returns
#' `1 - product` per row. Rows without valid future events are set to `0`.
#'
#' @param frame Numeric or integer vector of frame numbers.
#' @param complement_xg Numeric vector of `1 - xG` values aligned with `frame`.
#' @param team Vector of team identifiers aligned with `frame`.
#' @param offense Logical scalar. If `TRUE`, use same-team future rows; if
#'   `FALSE`, use opponent-team future rows.
#' @param n_next Integer scalar. Maximum number of subsequent rows to inspect.
#'
#' @return Numeric vector of length `length(frame)` with windowed xG values.
calculate_xG <- function(frame, complement_xg, team, offense, n_next = 10) {
  n <- length(frame)
  out <- rep(NA_real_, n)

  for (i in seq_len(n)) {
    if (i == n) next

    window_idx <- (i + 1):min(i + n_next, n) #gets next 10 rows or til last play
    window_idx <- window_idx[frame[window_idx] != frame[i]] #gets future frames which are not equal to current frame
    
    
    if (length(window_idx) == 0) next

    keep <- if (offense) {
      team[window_idx] == team[i]
    } else {
      team[window_idx] != team[i] 
    }
    vals <- complement_xg[window_idx[keep]]

    out[i] <- if (all(is.na(vals))) NA_real_ else prod(vals, na.rm = TRUE)
  }
  out <- ifelse(is.na(out), 0, 1 - out)
}

#ran code:

dir_path <- paste0(RDF_PATH,"KPI_Merged_all")
csv_files <- list.files(dir_path, pattern = "\\.csv$", full.names = TRUE)

pb <- progress::progress_bar$new(
  format = "[:bar] :current/:total (:percent) eta: :eta elapsed: :elapsed",
  total = length(csv_files),
  clear = FALSE,
  show_after = 0,
  stream = stderr()
)

all_labels <- purrr::map_dfr(csv_files, function(f) {
  kpi_df <- read.csv(f, sep = ";") |>
    dplyr::mutate(
      xG = as.numeric(stringr::str_replace(xG, ",", ".")),
      complement_xG = 1 - xG
    ) |>
    dplyr::arrange(FRAME_NUMBER)
  
  kpi_df <- kpi_df |>
    dplyr::mutate(
      offense_xG = calculate_xG(FRAME_NUMBER, complement_xG, CUID1, TRUE),
      defense_xG = calculate_xG(FRAME_NUMBER, complement_xG, CUID1, FALSE),
      complete = EVALUATION %in% c("successfullyComplete", "successful")
    ) |>
    dplyr::filter(SUBTYPE == "Pass") |>
    dplyr::select(EVENT_ID, MUID, offense_xG, defense_xG, complete)
  pb$tick()
  kpi_df
})
write.csv(all_labels, paste0(RDF_PATH, "xgb_labels.csv"), row.names = FALSE)
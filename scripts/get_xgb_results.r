library(dplyr)
library(arrow)
library(ggplot2)
library(xml2)
library(ggsoccer)
library(tidyr)
library(purrr)
library(progress)
library(xgboost)
library(bit64)

build_features <- function(event_df, event) {
  team <- event_df |> filter(player_side == "ATTACK", object_id != player_id)
  players <- team |> pull(object_id) |> unique()
  player_dfs <- vector("list", length(players))

  for (i in seq_along(players)) {
    player <- players[[i]]
    event_player_id <- paste(event, player, sep = "_")

    player_dfs[[i]] <- event_df |>
      mutate(
        event_id = event_player_id,
        is_intended = (object_id == player)
      )
  }
  bind_rows(player_dfs)
}

get_results <- function(frames, event, match){
  event_df <- frames |> filter(event_id == event, match_id == match) |> mutate(
    player_side = ifelse(player_team == team_id, "ATTACK", ifelse(player_team == "BALL", "BALL", "DEFENSE"))
  )
  expanded_events <- event_df |> build_features(event)

  group_ids <- interaction(expanded_events$match_id, expanded_events$event_id, drop = TRUE)
  group_index <- split(seq_len(nrow(expanded_events)), group_ids, drop = TRUE)

  feats_all <- purrr::map_dfr(group_index, function(ix) {
    event_df <- expanded_events[ix, , drop = FALSE]
    eid <- event_df$event_id[[1]]
    mid <- event_df$match_id[[1]]

    tryCatch(
      calc_event_features(event_df) |> mutate(event_id = eid, match_id = mid, .before = 1),
      error = function(e) {
        message(sprintf("Error in event_id %s: %s", eid, conditionMessage(e)))
        tibble()
      }
    )
  })
  feats_mat <- feats_all |> select(-c("event_id", "match_id")) |> xgb.DMatrix()
  model <- xgb.load("/home/lz80/rdf/sp161/shared/soccer-decision-making-r/sportec/xgb_offense_successful.json")
  preds <- predict(model, feats_mat)
  results <- feats_all |> select(event_id, match_id) |>
    separate(
      col = event_id,
      into = c("event_id", "object_id"),
      sep = "_",
      extra = "merge"
    ) |> mutate(
    prediction = preds,
    event_id = as.integer64(event_id)
  )
  coord_df <- event_df |> left_join(results, by = c("event_id","match_id", "object_id")) |> 
    select(x, y, ball_x_10frame_forward, ball_y_10frame_forward, player_side, prediction)
  coord_df
}
feature_script <- "/home/lz80/soccer-decision-making-r/scripts/generate_xgb_features.r"
feature_lines <- readLines(feature_script)#there is probably a better way to do this...
cut_idx <- grep("^goal_coords", feature_lines)[1] - 1

eval(parse(text = feature_lines[1:cut_idx]), envir = .GlobalEnv)
goal_coords <- c(x = 52.5, y = 0)

frames <- read_parquet("/home/lz80/rdf/sp161/shared/soccer-decision-making-r/sportec/passes.parquet") |> filter(is.na(set_piece_type)) |> mutate(
  x_velo = x_p5 - x_m5,
  y_velo = y_p5 - y_m5
)


pitch_sportec <- list(
  length = 105,
  width = 68,
  penalty_box_length = 16.5,
  penalty_box_width = 40.32,
  six_yard_box_length = 5.5,
  six_yard_box_width = 18.32,
  penalty_spot_distance = 11,
  goal_width = 7.32,
  origin_x = -52.5,
  origin_y = -34
)

sample_events <- frames|> select(event_id, match_id) |> head(2000) |> unique()
pdf("sample_results.pdf", width = 8, height = 6)
c = 1
for (i in 1:nrow(sample_events)) {
  row_data <- sample_events[i, ]
  event <- row_data$event_id
  match <- row_data$match_id
  coord_df <- frames |> get_results(event, match)
  plot <- coord_df |> ggplot() + annotate_pitch(dimension = pitch_sportec) + geom_point(aes(x = x, y = y, col = player_side)) + geom_text(
    data = coord_df |> dplyr::filter(!is.na(prediction)),
    aes(x = x, y = y, label = sprintf("%.3f", prediction)),
    nudge_y = 1.5,
    size = 3
  ) + theme_pitch() 
  c = c + 1
  print(paste0(c, "/", nrow(sample_events)))
  print(plot)
}
dev.off()





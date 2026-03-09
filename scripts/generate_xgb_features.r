library(dplyr)
library(arrow)
library(ggplot2)
library(xml2)
library(ggsoccer)
library(tidyr)
library(purrr)
library(progress)

# Progression features
#' Normalize Event Orientation And Add Goal Distance
#'
#' Flips x-axis coordinates for all tracked objects in an event when the
#' possessing team's goalkeeper (`position == "TW"`) is on the positive x side,
#' so attacking direction is standardized. Also computes `dist_from_goal`
#' for each row using global `goal_coords`.
#'
#' @param event_df A data frame/tibble containing one event's tracking rows.
#'   Expected columns include `position`, `player_team`, `team_id`, `x`, `y`,
#'   and the x-based coordinate variants used in `mutate()`.
#'
#' @return A tibble with possibly flipped x-related fields and a new
#'   `dist_from_goal` column.
flip_event <- function(event_df){
  gk_x_t <- event_df |> filter(position == 'TW' & player_team == team_id) |> pull(x)
  gk_x_opp <- event_df |> filter(position == 'TW' & player_team != team_id) |> pull(x)
  needs_flip <- length(gk_x_t) > 0 && (gk_x_t[[1]] > gk_x_opp[[1]])

  if (needs_flip) {
    event_df <- event_df |>
      mutate(
        x = -x,
        ball_x_10frame_forward = -ball_x_10frame_forward,
        ball_x_10frame_forward_p5 = -ball_x_10frame_forward_p5,
        x_p5 = -x_p5,
        x_m5 = -x_m5,
        x_rec = -x_rec,
        x_velo = -x_velo
      )
  }
  event_df <- event_df |> mutate(
      dist_from_goal = sqrt((x - goal_coords["x"])**2 + (y - goal_coords["y"])**2)
  )
  return(event_df)
}

#' Passer And Intended Receiver Location Features
#'
#' Extracts goal-relative location and velocity features for the passer and
#' intended receiver within a single event. Angles are computed relative to
#' the global `goal_coords` via `get_angle_rel_goal()`.
#'
#' @param event_df Event-level tracking rows containing the passer, where
#'   the passer row is identified by `player_id == object_id`.
#' @param intended Intended receiver row(s) for the same event.
#'
#' @return A one-row tibble with passer distance/angle to goal and intended
#'   receiver distance/angle to goal plus x/y velocity components.
calc_location_feats <- function(event_df, intended) {
  passer <- event_df |> filter(player_id == object_id)
  passer_dist_from_goal <- passer |> select(dist_from_goal) |> pull(1)
  passer_x <- passer |> select(x) |> pull(1)
  passer_y <- passer |> select(y) |> pull(1)

  passer_angle <- get_angle_rel_goal(passer_x, passer_y, 0, 0) 
  #defining angle to goal as angle between vector between player and goal and midpoint and goal

  intended_dist_from_goal <- intended |> select(dist_from_goal) |> pull(1)
  intended_x <- intended |> select(x) |> pull(1)
  intended_y <- intended |> select(y) |> pull(1)

  intended_angle <- get_angle_rel_goal(intended_x, intended_y, 0, 0)
  intended_x_velo <- intended |> select(x_velo) |> pull(1)
  intended_y_velo <- intended |> select(y_velo) |> pull(1)

  tibble(
      p_dist_from_goal = passer_dist_from_goal,
      p_angle = passer_angle,
      i_dist_from_goal = intended_dist_from_goal,
      i_angle = intended_angle,
      i_x_velo = intended_x_velo,
      i_y_velo = intended_y_velo
    )
  }

#' Angle Difference Relative To Goal Direction
#'
#' Computes the absolute angle (in degrees) between vectors from two points
#' (`x1`,`y1`) and (`x2`,`y2`) toward the global goal coordinate.
#'
#' @param x1,y1 Numeric coordinates of the first point.
#' @param x2,y2 Numeric coordinates of the second point.
#'
#' @return Numeric angle in degrees.
get_angle_rel_goal <- function(x1, y1, x2, y2) {
  dx1 <- goal_coords["x"] - x1
  dy1 <- goal_coords["y"] - y1
  dx2 <- goal_coords["x"] - x2
  dy2 <- goal_coords["y"] - y2
  abs(atan2(dx1 * dy2 - dy1 * dx2, dx1 * dx2 + dy1 * dy2)) * 180 / pi
}
#' Passer-Receiver Core Features
#'
#' Builds passer-to-intended-receiver features for a single event:
#' passer-receiver distance, relative angle to goal, relative velocity, and
#' counts of attackers/defenders between passer-receiver and receiver-goal.
#'
#' @param event_df Event-level tracking data containing passer and intended
#'   receiver rows.
#' @param opponent_df Opponent-only rows for the event.
#' @param teammate_df Teammate-only rows for the event.
#'
#' @return A one-row data frame with passer/receiver scalar features.
calc_passer_intended_feats <- function(event_df, opponent_df, teammate_df) {

  

  passer_receiver <- event_df |> filter(object_id == player_id | is_intended) 


  
  passer <- passer_receiver |> filter(object_id == player_id)
  receiver <- passer_receiver |> filter(is_intended)
  ##distance
  pr_distance <- sqrt((passer$x[[1]] - receiver$x[[1]])^2 + (passer$y[[1]] - receiver$y[[1]])^2)
  ## angle
  pr_angle <- get_angle_rel_goal(passer$x, passer$y, receiver$x, receiver$y)

  ## relative velocity

  pr_x_rel_velo = passer$x_velo - receiver$x_velo
  pr_y_rel_velo = passer$y_velo - receiver$y_velo 

  

  closer_to_goal = min(passer$dist_from_goal, receiver$dist_from_goal)
  further_to_goal = max(passer$dist_from_goal, receiver$dist_from_goal)

  #number of attackers between passer and intended receiver (1 feature)
  num_att_pr <- teammate_df |> filter(closer_to_goal < dist_from_goal, further_to_goal > dist_from_goal) |> nrow()
  #number of defenders between passer and intended receiver (1 feature)
  num_def_pr <- opponent_df |> filter(closer_to_goal < dist_from_goal, further_to_goal > dist_from_goal) |> nrow()
  #number of attackers between intended receiver and goal (1 feature)
  num_att_rg <- teammate_df |> filter(receiver$dist_from_goal > dist_from_goal) |> nrow()
  #number of defenders between intended receiver and goal (1 feature)
  num_def_rg <- opponent_df |> filter(receiver$dist_from_goal > dist_from_goal) |> nrow()

  #distance, angle (relative to angle to goal), relative velocity between passer and intended receiver
  return(tibble(
    pr_dist = pr_distance, 
    pr_angle = pr_angle, 
    pr_x_rel_velo = pr_x_rel_velo, 
    pr_y_rel_velo = pr_y_rel_velo, 
    num_att_pr = num_att_pr,
    num_def_pr = num_def_pr,
    num_att_rg = num_att_rg,
    num_def_rg = num_def_rg))
}

#' Angle At Middle Point From Three Coordinates
#'
#' Computes the angle (in degrees) at point B formed by segments B->A and B->C.
#' Returns `NA_real_` for zero-length segments.
#'
#' @param x1,y1 Numeric coordinates for point A.
#' @param x2,y2 Numeric coordinates for point B (vertex).
#' @param x3,y3 Numeric coordinates for point C.
#'
#' @return Numeric angle in degrees, possibly `NA_real_`.
get_angle_AB_BC <- function(x1, y1, x2, y2, x3, y3) {
  v21x <- x1 - x2
  v21y <- y1 - y2
  v23x <- x3 - x2
  v23y <- y3 - y2
  n1 <- sqrt(v21x^2 + v21y^2)
  n2 <- sqrt(v23x^2 + v23y^2)
  denom <- n1 * n2
  cos_theta <- ifelse(denom == 0, NA_real_, (v21x * v23x + v21y * v23y) / denom)
  cos_theta <- pmax(-1, pmin(1, cos_theta))
  acos(cos_theta) * 180 / pi
}
#' Select Nearest Two Players In Front And Behind Ball
#'
#' From a candidate player data frame, selects up to two nearest players to the
#' intended receiver on each side of the ball (in front/behind by x position).
#'
#' @param df Candidate players (teammates or opponents) for one event.
#' @param ball Ball row(s) for the same event.
#' @param intended Intended receiver row(s) for the same event.
#'
#' @return A tibble with selected players and helper columns
#'   `dist_from_intended` and `infront`.
get_players_front_behind <- function(df, ball, intended){
  df |>
    filter(position != "TW", object_id != player_id, !is_intended) |> #Removing goalkeeper, passer, and intended receiver; TW = Goalkeeper
    cross_join(ball, suffix = c("", "_ball")) |>
    cross_join(intended, suffix = c("", "_intended")) |>
    mutate(
      dist_from_intended = sqrt((x - x_intended)^2 + (y - y_intended)^2),
      infront = x > x_ball
    ) |>
    group_by(infront) |>
    arrange(dist_from_intended, .by_group = TRUE) |>
    slice_head(n = 2) |>
    ungroup()
}
#' Distances Between Intended Receiver And Selected Players
#'
#' Computes Euclidean distance between intended receiver and each selected
#' player returned by `get_players_front_behind()`.
#'
#' @param intended Intended receiver row(s) with `x` and `y`.
#' @param f2b2_df Selected player rows with `x`, `y`, `object_id`, `infront`,
#'   and `dist_from_intended`.
#'
#' @return A tibble with `dist`, `object_id`, `infront`, `dist_from_intended`.
calc_dists_front_behind <- function(intended, f2b2_df){
  bind_cols(
  intended |> select(x1 = x, y1 = y),
  f2b2_df |> select(x2 = x, y2 = y, object_id, infront, dist_from_intended)) |>
  mutate(dist = sqrt((x1 - x2)^2 + (y1 - y2)^2)) |> select(dist, object_id, infront, dist_from_intended)
}

#' Goal-Relative Angles Between Intended Receiver And Selected Players
#'
#' Computes absolute angle difference to goal direction between intended receiver
#' and each selected player.
#'
#' @param intended Intended receiver row(s) with `x` and `y`.
#' @param f2b2_df Selected player rows with `x`, `y`, and `object_id`.
#'
#' @return A tibble with `angle` and `object_id`.
calculate_angles_front_behind <- function(intended, f2b2_df){
  bind_cols(
    intended |> select(x1 = x, y1 = y),
    f2b2_df |> select(x2 = x, y2 = y, object_id)) |>
    mutate(angle = get_angle_rel_goal(x1, y1, x2, y2)) |>
    select(angle, object_id)
}

#' Relative Velocity Of Selected Players Vs Intended Receiver
#'
#' Computes x/y relative velocities for selected players against the intended
#' receiver.
#'
#' @param intended Intended receiver row(s) with `x_velo` and `y_velo`.
#' @param t2_def Selected player rows with `x_velo`, `y_velo`, and `object_id`.
#'
#' @return A tibble with `x_rel_velo`, `y_rel_velo`, and `object_id`.
calc_rel_velos_front_behind <- function(intended, t2_def) {
    bind_cols(
    intended |> select(x1_velo = x_velo, y1_velo = y_velo),
    t2_def |> select(x2_velo = x_velo, y2_velo = y_velo, object_id)
  ) |>
    mutate(
      x_rel_velo = x2_velo - x1_velo,
      y_rel_velo = y2_velo - y1_velo) |> select(x_rel_velo, y_rel_velo, object_id)
}

#' Receiver-Defender Feature Block
#'
#' Builds features for up to two nearest defenders in front of and behind the
#' ball relative to the intended receiver, including distance, angle to goal,
#' relative velocity, and passer-intended-defender angle.
#'
#' @param event_df Full event tracking data.
#' @param opponent_df Opponent-only rows for the event.
#' @param teammate_df Teammate-only rows (currently unused in this function
#'   signature but passed by caller for symmetry).
#' @param intended Intended receiver row(s).
#' @param ball Ball row(s).
#'
#' @return A one-row wide tibble with defender feature columns suffixed `_d`.
calc_receiver_defender_feats <- function(event_df, opponent_df, teammate_df, intended, ball) {

  t2_def <- opponent_df |>
    get_players_front_behind(ball, intended)
  ##distance

  dists <- calc_dists_front_behind(intended, t2_def)

  ## angle
  angles <- calculate_angles_front_behind(intended, t2_def)

  ## relative velocity
  rel_vels <- calc_rel_velos_front_behind(intended, t2_def)
  #angle formed by passer, intended receiver, and defender (1 feature)
  pid_angles <- bind_cols(
    intended |> select(i_x = x, i_y = y),
    event_df |> filter(player_id == object_id) |> select(p_x = x, p_y = y),
    t2_def |> select(d_x = x, d_y = y,  object_id)
  ) |>
    mutate(pid_angle = get_angle_AB_BC(p_x, p_y, i_x, i_y, d_x, d_y)) |>
    select(pid_angle, object_id)

  opp_defs <- list(dists, angles, rel_vels, pid_angles) 
  def_feats <- reduce(opp_defs, inner_join, by = "object_id")

  def_feats  |>
      mutate(side = if_else(infront, "infront", "behind")) |>
      group_by(side) |>
      arrange(dist_from_intended, .by_group = TRUE) |>
      mutate(rnk = row_number()) |>
      filter(rnk <= 2) |>
      ungroup() |>
      complete(side = c("infront", "behind"), rnk = 1:2) |>
      select(side, rnk, dist, angle, x_rel_velo, y_rel_velo, pid_angle) |>
      pivot_wider(
        names_from = c(side, rnk),
        values_from = c(dist, angle, x_rel_velo, y_rel_velo, pid_angle),
        names_glue = "{.value}_{side}_{rnk}_d"
      )
  }

#' Receiver-Teammate Feature Block
#'
#' Builds features for up to two nearest teammates in front of and behind the
#' ball relative to the intended receiver, including receiver-teammate
#' geometry and nearest-opponent interaction features.
#'
#' @param event_df Full event tracking data.
#' @param teammates Teammate-only rows for the event.
#' @param ball Ball row(s).
#' @param opponent_df Opponent-only rows for the event.
#' @param intended Intended receiver row(s).
#'
#' @return A one-row wide tibble with teammate feature columns suffixed `_o`.
calc_receiver_attacker_feats <- function(event_df, teammates, ball, opponent_df, intended) {
  t2_att <- teammates |> get_players_front_behind(ball, intended)
  # distance, angle (relative to goal) between intended receiver and teammate (2 features)
  dists <- calc_dists_front_behind(intended, t2_att)
  ## angle
  angles <- calculate_angles_front_behind(intended, t2_att)

    # angle formed by passer, intended receiver, and teammate (1 feature)

  pit_angles <- bind_cols(
    event_df |> filter(is_intended) |> select(i_x = x, i_y = y),
    event_df |> filter(player_id == object_id) |> select(p_x = x, p_y = y),
    t2_att |> select(d_x = x, d_y = y, object_id)) |>
    mutate(pit_angle = get_angle_AB_BC(p_x, p_y, i_x, i_y, d_x, d_y)) |>
    select(pit_angle, object_id)

  # distance, angle (relative to angle to goal), relative velocity between teammate and nearest opponent (4 features)
  ## closest defenders
  ### distance
  closest_opponent <- t2_att |> cross_join(opponent_df |> select(x, y, x_velo, y_velo), suffix = c("", "_def")) |> mutate(
    cls_distance = sqrt((x - x_def)**2 + (y - y_def)**2)
  ) |> group_by(object_id,  .by_group = TRUE) |> arrange(cls_distance) |> slice_head(n = 1) |> ungroup() 

  cls_distance = closest_opponent |> select(cls_distance, object_id)

  ### angle
  cls_angle <- closest_opponent |>
    mutate(cls_angle = get_angle_rel_goal(x, y, x_def, y_def)) |>
    select(cls_angle, object_id)

  cls_rel_velo <- closest_opponent |>
    mutate(
      cls_x_rel_velo = x_velo - x_velo_def,
      cls_y_rel_velo = y_velo - y_velo_def
    ) |> select(cls_x_rel_velo, cls_y_rel_velo, object_id)

  tm_dfs = list(dists, angles, pit_angles, cls_distance, cls_angle, cls_rel_velo)
  tm_feats = reduce(tm_dfs, inner_join, by = "object_id")

  tm_feats <- tm_feats  |>
    mutate(side = if_else(infront, "infront", "behind")) |>
    group_by(side) |>
    arrange(dist_from_intended, .by_group = TRUE) |>
    mutate(rnk = row_number()) |>
    filter(rnk <= 2) |>
    ungroup() |>
    complete(side = c("infront", "behind"), rnk = 1:2) |>
    select(side, rnk, dist, angle, cls_angle, cls_distance, cls_x_rel_velo, cls_y_rel_velo, pit_angle) |>
    pivot_wider(
      names_from = c(side, rnk),
      values_from = c(dist, angle, cls_x_rel_velo, cls_y_rel_velo, pit_angle, cls_angle, cls_distance),
      names_glue = "{.value}_{side}_{rnk}_o"
    )
  
  return(tm_feats)
}


#' Build Model Features For A Single Pass Event
#'
#' Computes the full feature vector used for XGBoost modeling from one
#' event-level tracking frame. The event is first normalized with
#' `flip_event()` to standardize attacking direction, then split into teammate,
#' opponent, ball, and intended-receiver subsets. Feature blocks are assembled
#' by combining:
#' - passer/intended receiver location features (`calc_location_feats()`)
#' - passer-to-intended interaction features (`calc_passer_intended_feats()`)
#' - intended-vs-defender neighborhood features (`calc_receiver_defender_feats()`)
#' - intended-vs-teammate neighborhood features (`calc_receiver_attacker_feats()`)
#'
#' @param event_df A data frame/tibble containing all tracked rows for one
#'   event. Expected columns include role/identity fields (for example
#'   `player_team`, `team_id`, `object_id`, `player_id`, `is_intended`,
#'   `position`) and kinematic fields used by downstream feature functions.
#'
#' @return A one-row tibble of engineered features for the event. Returns an
#'   empty tibble when the event has no ball row or no intended receiver row.
calc_event_features <- function(event_df) {
  event_df <- flip_event(event_df)
  
  teammate_df <- event_df |> filter(player_team == team_id)
  opponent_df <- event_df |> filter(player_team != team_id & player_team != "BALL")
  ball <- event_df |> filter(player_team == "BALL")
  intended <- event_df |> filter(is_intended)
  
  if (nrow(ball) == 0 || nrow(intended) == 0) {
    return(tibble())
  }
  
  calc_location_feats(event_df, intended) |>
    bind_cols(
      calc_passer_intended_feats(event_df, opponent_df, teammate_df),
      calc_receiver_defender_feats(event_df, opponent_df, teammate_df, intended, ball),
      calc_receiver_attacker_feats(event_df, teammate_df, ball, opponent_df, intended)
    )
}

goal_coords <- c(x = 52.5, y = 0)
frames <- read_parquet("/home/lz80/rdf/sp161/shared/soccer-decision-making-r/sportec/passes.parquet") |> filter(is.na(set_piece_type)) |> mutate(
  x_velo = x_p5 - x_m5,
  y_velo = y_p5 - y_m5
)
print("Finished Loading Data, grouping event data...")

group_ids <- interaction(frames$match_id, frames$event_id, drop = TRUE) 
group_index <- split(seq_len(nrow(frames)), group_ids, drop = TRUE)

group_pb <- progress_bar$new(
  format = "Grouping [:bar] :current/:total (:percent) eta: :eta elapsed: :elapsed",
  total = length(group_index),
  clear = FALSE,
  show_after = 0,
  stream = stderr()
)

event_groups <- vector("list", length(group_index))
for (i in seq_along(group_index)) {
  event_groups[[i]] <- frames[group_index[[i]], , drop = FALSE]
  group_pb$tick()
}

pb <- progress_bar$new(
  format = "[:bar] :current/:total (:percent) eta: :eta elapsed: :elapsed",
  total = length(event_groups),
  clear = FALSE,
  show_after = 0,
  stream = stderr()
)

print("Finshed event data, calculating features...")
feats_all <- map_dfr(event_groups, function(event_df) {
  eid <- event_df$event_id[[1]]
  mid <- event_df$match_id[[1]]
  pb$tick()
  tryCatch(
    {
      out <- calc_event_features(event_df)
      out |> mutate(event_id = eid, match_id = mid, .before = 1)
    },
    error = function(e) {
      message(sprintf("Error in event_id %s: %s", eid, conditionMessage(e)))
      tibble()
    }
  )
})
write.csv(feats_all, "/home/lz80/rdf/sp161/shared/soccer-decision-making-r/sportec/xgb_features.csv", row.names = TRUE)

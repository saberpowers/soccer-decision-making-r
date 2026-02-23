library(dplyr)
library(arrow)
library(ggplot2)
library(xml2)
library(ggsoccer)
library(tidyr)
library(purrr)
library(progress)

#Progression features
flip_event <- function(event_df){
  gk_x <- event_df |> filter(position == 'TW' & player_team == team_id) |> pull(x)
  needs_flip <- length(gk_x) > 0 && gk_x[[1]] > 0

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



#distance, angle (relative to angle to goal), relative velocity between passer and intended receiver (4 features)
get_angle_rel_goal <- function(x1, y1, x2, y2) {
  dx1 <- goal_coords["x"] - x1
  dy1 <- goal_coords["y"] - y1
  dx2 <- goal_coords["x"] - x2
  dy2 <- goal_coords["y"] - y2
  abs(atan2(dx1 * dy2 - dy1 * dx2, dx1 * dx2 + dy1 * dy2)) * 180 / pi
}
calc_passer_intended_feats <- function(event_df, opponent_df, teammate_df) {

  ##distance

  passer_receiver <- event_df |> filter(object_id == player_id | is_intended) 
  pr_distance <- passer_receiver |> select(x, y) |> dist()


  ## angle
  passer <- passer_receiver |> filter(object_id == player_id)
  receiver <- passer_receiver |> filter(is_intended)

  pr_angle <- get_angle_rel_goal(passer$x, passer$y, receiver$x, receiver$y)

  #angle_deg

  ## relative velocity

  pr_x_rel_velo = passer$x_velo - receiver$x_velo
  pr_y_rel_velo = passer$y_velo - receiver$y_velo 

  #number of attackers between passer and intended receiver (1 feature)

  closer_to_goal = min(passer$dist_from_goal, receiver$dist_from_goal)
  further_to_goal = max(passer$dist_from_goal, receiver$dist_from_goal)

  num_att_pr <- teammate_df |> filter(closer_to_goal < dist_from_goal, further_to_goal > dist_from_goal) |> nrow()

  #number of defenders between passer and intended receiver (1 feature)
  num_def_pr <- opponent_df |> filter(closer_to_goal < dist_from_goal, further_to_goal > dist_from_goal) |> nrow()
  #number of attackers between intended receiver and goal (1 feature)
  num_att_rg <- teammate_df |> filter(receiver$dist_from_goal > dist_from_goal) |> nrow()
  #number of defenders between intended receiver and goal (1 feature)
  num_def_rg <- opponent_df |> filter(receiver$dist_from_goal > dist_from_goal) |> nrow()

  #distance, angle (relative to angle to goal), relative velocity between passer and intended receiver
  return(list(
    pr_dist = pr_distance, 
    pr_angle = pr_angle, 
    pr_x_rel_velo = pr_x_rel_velo, 
    pr_y_rel_velo = pr_y_rel_velo, 
    num_att_pr = num_att_pr,
    num_def_pr = num_def_pr,
    num_att_rg = num_att_rg,
    num_def_rg = num_def_rg) |> as.data.frame())
}


#distance, angle (relative to goal), relative velocity between intended receiver and defender (4 features)
angle_12_23 <- function(x1, y1, x2, y2, x3, y3) {
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
get_f2_b2_players <- function(df, ball, intended){
  df |>
    filter(position != "TW", object_id != player_id, !is_intended) |>
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
calc_f2b2_dists <- function(intended, f2b2_df){
  bind_cols(
  intended |> select(x1 = x, y1 = y),
  f2b2_df |> select(x2 = x, y2 = y, object_id, infront, dist_from_intended)) |>
  mutate(dist = sqrt((x1 - x2)^2 + (y1 - y2)^2)) |> select(dist, object_id, infront, dist_from_intended)
}

calc_f2b2_angles <- function(intended, f2b2_df){
  bind_cols(
    intended |> select(x1 = x, y1 = y),
    f2b2_df |> select(x2 = x, y2 = y, object_id)) |>
    mutate(angle = get_angle_rel_goal(x1, y1, x2, y2)) |>
    select(angle, object_id)
}

calc_f2b2_rel_velos <- function(intended, t2_def) {
    bind_cols(
    intended |> select(x1_velo = x_velo, y1_velo = y_velo),
    t2_def |> select(x2_velo = x_velo, y2_velo = y_velo, object_id)
  ) |>
    mutate(
      x_rel_velo = x2_velo - x1_velo,
      y_rel_velo = y2_velo - y1_velo) |> select(x_rel_velo, y_rel_velo, object_id)
}

calc_receiver_defender_feats <- function(event_df, opponent_df, teammate_df, intended, ball) {

  t2_def <- opponent_df |>
    get_f2_b2_players(ball, intended)
  ##distance

  dists <- calc_f2b2_dists(intended, t2_def)

  ## angle
  angles <- calc_f2b2_angles(intended, t2_def)

  ## relative velocity
  rel_vels <- calc_f2b2_rel_velos(intended, t2_def)
  #angle formed by passer, intended receiver, and defender (1 feature)
  pid_angles <- bind_cols(
    intended |> select(i_x = x, i_y = y),
    event_df |> filter(player_id == object_id) |> select(p_x = x, p_y = y),
    t2_def |> select(d_x = x, d_y = y,  object_id)
  ) |>
    mutate(pid_angle = angle_12_23(p_x, p_y, i_x, i_y, d_x, d_y)) |>
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

# For 2 teammates nearest to the intended receiver in front and behind the ball

# distance, angle (relative to goal) between intended receiver and teammate (2 features)
# angle formed by passer, intended receiver, and teammate (1 feature)
# distance, angle (relative to angle to goal), relative velocity between teammate and nearest opponent (4 features)

calc_receiver_attacker_feats <- function(event_df, teammates, ball, opponent_df, intended) {
  t2_att <- teammates |> get_f2_b2_players(ball, intended)
  # distance, angle (relative to goal) between intended receiver and teammate (2 features)
  dists <- calc_f2b2_dists(intended, t2_att)
  ## angle
  angles <- calc_f2b2_angles(intended, t2_att)

    # angle formed by passer, intended receiver, and teammate (1 feature)

  pit_angles <- bind_cols(
    event_df |> filter(is_intended) |> select(i_x = x, i_y = y),
    event_df |> filter(player_id == object_id) |> select(p_x = x, p_y = y),
    t2_att |> select(d_x = x, d_y = y, object_id)) |>
    mutate(pit_angle = angle_12_23(p_x, p_y, i_x, i_y, d_x, d_y)) |>
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

calc_event_features <- function(event_df) {
  event_df <- flip_event(event_df)
  
  teammate_df <- event_df |> filter(player_team == team_id)
  opponent_df <- event_df |> filter(player_team != team_id & player_team != "BALL")
  ball <- event_df |> filter(player_team == "BALL")
  intended <- event_df |> filter(is_intended)
  
  if (nrow(ball) == 0 || nrow(intended) == 0) {
    return(tibble())
  }
  
  calc_passer_intended_feats(event_df, opponent_df, teammate_df) |>
    bind_cols(
      calc_receiver_defender_feats(event_df, opponent_df, teammate_df, intended, ball),
      calc_receiver_attacker_feats(event_df, teammate_df, ball, opponent_df, intended)
    )
}

goal_coords = c(x = 52.5, y = 0)
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

cli_progress_done(pb)

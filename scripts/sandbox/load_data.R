
dir_data <- "~/rdf/sp161/shared/soccer-decision-making-r/sportec/event"



# 1. Define the worker function for a single file
# This handles the nested structure (Event > Play > Pass, etc.)
process_match_xml <- function(file_path) {
  # Parse XML structure
  xml_doc <- xml2::read_xml(file_path)
  event_nodes <- xml2::xml_find_all(xml_doc, ".//Event")
  
  # Recursive helper to flatten attributes from nested tags
  extract_nested_attrs <- function(node) {
    node_name <- xml2::xml_name(node)
    attrs <- as.list(xml2::xml_attrs(node))
    
    # Prefix attributes with node name (e.g., Pass_Direction)
    if (length(attrs) > 0) {
      names(attrs) <- paste0(node_name, "_", names(attrs))
    }

    # Add the node name as a column (useful to identify event types like "TacklingGame")
    attrs[[paste0(node_name, "_tag")]] <- node_name
    
    # Recursively find children and append their attributes
    children <- xml2::xml_children(node)
    if (length(children) > 0) {
      child_attrs <- purrr::map(children, extract_nested_attrs)
      child_attrs <- purrr::list_flatten(child_attrs)
      attrs <- c(attrs, child_attrs)
    }
    return(attrs)
  }
  
  # Map across all events in the match
  # Using data.table inside the function is much faster than standard data frames
  match_data <- purrr::map(event_nodes, function(x) {
    flat_list <- extract_nested_attrs(x)
    data.table::as.data.table(t(unlist(flat_list)))
  })
  
  # Bind all rows for this specific match
  match_dt <- data.table::rbindlist(match_data, fill = TRUE)
  
  # Add metadata for the file name
  match_dt[, original_file := basename(file_path)]
  
  return(match_dt)
}

# 2. Setup Parallel Processing
# Using 'multisession' allows R to use multiple CPU workers
future::plan(future::multisession)

# 3. Identify all files in the season directory
xml_files <- list.files(path = dir_data, pattern = "\\.xml$", full.names = TRUE)

# 4. Run the processing in parallel
# furrr::future_map runs 'process_match_xml' on multiple files at once
all_matches_list <- furrr::future_map(xml_files, process_match_xml, .progress = TRUE)

# 5. Combine everything into one master Season Table
# data.table::rbindlist is the fastest way in R to merge large lists of tables
full_season_dt <- data.table::rbindlist(all_matches_list, fill = TRUE)

# 6. (Recommended) Save the result to Parquet
# Parquet files are significantly faster and smaller than CSVs for millions of rows
# arrow::write_parquet(full_season_dt, "full_season_events.parquet")











action_type <- full_season_dt |>
  dplyr::select(Event_MatchId, Event_EventId, dplyr::ends_with("_tag")) |>
  tidyr::pivot_longer(cols = dplyr::ends_with("_tag")) |>
  dplyr::filter(!is.na(value)) |>
  dplyr::arrange(value) |>
  dplyr::group_by(Event_MatchId, Event_EventId) |>
  dplyr::summarize(event_tags = paste(value, collapse = ","), .groups = "drop") |>
  dplyr::mutate(
    # https://socceraction.readthedocs.io/en/latest/documentation/spadl/SPADL_definitions.html
    # The notable tag we exclude is TacklingGame, which seems to describe aerial and ground duels.
    # We excluded it because it's not clear how to convert this to SPADL.
    action_type = dplyr::case_when(
      event_tags %in% c("Event,Pass,Play", "Event,KickOff,Pass,Play") ~ "Pass",
      event_tags %in% c("Cross,Event,Play") ~ "Cross",
      event_tags %in% c("Event,Pass,Play,ThrowIn", "Cross,Event,Play,ThrowIn") ~ "Throw-in",
      event_tags %in% c("Event,GoalKick,Pass,Play") ~ "Goal kick",
      event_tags %in% c("Cross,Event,FreeKick,Play") ~ "Crossed free-kick",
      event_tags %in% c("Event,FreeKick,Pass,Play") ~ "Short free-kick",
      event_tags %in% c("CornerKick,Cross,Event,Play") ~ "Crossed corner",
      event_tags %in% c("CornerKick,Event,Pass,Play") ~ "Short corner",
      event_tags %in% c(
        "Event,ShotAtGoal,ShotWide",
        "BlockedShot,Event,ShotAtGoal",
        "Event,SavedShot,ShotAtGoal",
        "Event,ShotAtGoal,SuccessfulShot",
        "Event,OtherShot,ShotAtGoal",
        "Event,ShotAtGoal,ShotWoodWork"
      ) ~ "Shot",
      event_tags %in% c(
        "BlockedShot,Event,FreeKick,ShotAtGoal",
        "Event,FreeKick,ShotAtGoal,ShotWide",
        "Event,FreeKick,SavedShot,ShotAtGoal",
        "Event,FreeKick,ShotAtGoal,SuccessfulShot",
        "Event,FreeKick,ShotAtGoal,ShotWoodWork"
      ) ~ "Free-kick shot",
      event_tags %in% c(
        "Event,Penalty,ShotAtGoal,SuccessfulShot",
        "Event,Penalty,SavedShot,ShotAtGoal",
        "Event,Penalty,ShotAtGoal,ShotWide",
        "Event,Penalty,ShotAtGoal,ShotWoodWork"
      ) ~ "Penalty shot",
      event_tags %in% c("BallClaiming,Event") ~ "Keeper claim",
      event_tags %in% c("Event,Foul") ~ "Foul",
      event_tags %in% c("Event,OwnGoal") ~ "Own goal"
    )
  )

full_season_action_dt <- full_season_dt |>
  dplyr::left_join(action_type, by = c("Event_MatchId", "Event_EventId"))

pass_actions <- c(
  "Pass", "Cross", "Throw-in", "Goal kick",
  "Crossed free-kick", "Short free-kick", "Crossed corner", "Short corner"
)
shot_actions <- c("Shot", "Free-kick shot", "Penalty shot")

full_season_spadl_dt <- full_season_action_dt |>
  dplyr::mutate(
    start_loc_x = as.numeric(`Event_X-Position`),
    start_loc_y = as.numeric(`Event_Y-Position`),
    player = dplyr::case_when(
      action_type %in% pass_actions ~ Play_Player,
      action_type %in% shot_actions ~ ShotAtGoal_Player,
      action_type == "Keeper claim" ~ BallClaiming_Player,
      action_type == "Foul" ~ Foul_Fouler,
      action_type == "Own goal" ~ OwnGoal_Player,
      event_tags == "Event,OtherBallAction" ~ OtherBallAction_Player
    ),
    team = dplyr::case_when(
      action_type %in% pass_actions ~ Play_Team,
      action_type %in% shot_actions ~ ShotAtGoal_Team,
      action_type == "Keeper claim" ~ BallClaiming_Team,
      action_type == "Foul" ~ Foul_TeamFouler,
      action_type == "Own goal" ~ OwnGoal_Team,
      event_tags == "Event,OtherBallAction" ~ OtherBallAction_Team
    ),
    body_part = dplyr::case_when(
      action_type %in% shot_actions ~ ShotAtGoal_TypeOfShot,
      action_type == "Own goal" ~ OwnGoal_TypeOfShot
    ),
    is_success = dplyr::case_when(
      action_type %in% pass_actions ~ Play_Evaluation %in% c("successfullyCompleted", "successful"),
      action_type %in% shot_actions ~ !is.na(SuccessfulShot_tag),
      action_type == "Keeper claim" ~ TRUE,
      action_type == "Foul" ~ FALSE,
      action_type == "Own goal" ~ FALSE
    ),
    xg = dplyr::case_when(
      action_type %in% shot_actions ~ as.numeric(ShotAtGoal_xG)
    )
  )

full_season_spadl_dt |>
  dplyr::filter(Event_MatchId == Event_MatchId[1]) |>
  dplyr::select(
    Event_MatchId, player, team, action_type, is_success,
    `Event_X-Position`, `Event_Y-Position`, `Event_X-Source-Position`, `Event_Y-Source-Position`,
    event_tags
  ) |>
  data.table::fwrite("output/temp.csv")
RDF_PATH = "/home/lz80/rdf/sp161/shared/soccer-decision-making-r/sportec/"
#' Join Feature Rows To Outcome Labels
#'
#' Filters the label table to either offensive or defensive targets and to
#' either successful or unsuccessful passes, then inner-joins those labels onto
#' the engineered feature table by event and match identifiers.
#'
#' @param feats A data frame of engineered pass features containing `EVENT_ID`
#'   and `MUID`.
#' @param labels A data frame of label rows containing `EVENT_ID`, `MUID`,
#'   `complete`, and both `offense_xG` and `defense_xG`.
#' @param offensive Logical; if `TRUE`, keep `offense_xG`, otherwise keep
#'   `defense_xG`.
#' @param successful Logical; if `TRUE`, keep completed passes, otherwise keep
#'   incomplete passes.
#'
#' @return A data frame containing feature rows joined with the selected target
#'   column.
build_dataset <- function(feats, labels, offensive = TRUE, successful = TRUE) {
  if (offensive) {
    labels <- labels |> dplyr::filter(complete == successful) |> dplyr::select(EVENT_ID, MUID, offense_xG)
  } else {
    labels <- labels |> dplyr::filter(complete == successful) |> dplyr::select(EVENT_ID, MUID, defense_xG)
  }

  feats |> dplyr::inner_join(labels, by = c("EVENT_ID", "MUID"))
}

#' Build A Model Matrix And Target Vector
#'
#' Selects the requested XG target, removes identifier columns from the feature
#' set, preserves missing values during formula processing, and returns a design
#' matrix ready for XGBoost.
#'
#' @param passes A data frame produced by `build_dataset()`.
#' @param offensive Logical; if `TRUE`, use `offense_xG` as the response,
#'   otherwise use `defense_xG`.
#'
#' @return A list with `X` (numeric model matrix), `y` (target vector), and
#'   `feature_cols` (expanded column names in `X`).
prepare_training_matrix <- function(passes, offensive = TRUE) {
  label_col <- if (offensive) "offense_xG" else "defense_xG"

  feature_cols <- setdiff(colnames(passes), c("MUID", "EVENT_ID", "X", label_col))

  model_df <- passes |>
    dplyr::select(dplyr::all_of(c(feature_cols, label_col))) |>
    dplyr::filter(!is.na(.data[[label_col]]))   # use chosen label

  mf <- model.frame(
    reformulate(feature_cols, response = label_col),
    data = model_df,
    na.action = na.pass
  )

  X <- model.matrix(
    reformulate(feature_cols, response = NULL, intercept = FALSE),
    data = mf
  )

  list(
    X = X,
    y = model_df[[label_col]], 
    feature_cols = colnames(X)
  )
}


#' Run Manual K-Fold Cross-Validation For XGBoost
#'
#' Randomly assigns rows to folds, trains an XGBoost regressor on each training
#' split, evaluates on the held-out fold, and reports fold-level RMSE and MAE.
#'
#' @param X Numeric feature matrix.
#' @param y Numeric response vector aligned with `X`.
#' @param nfold Number of folds to evaluate.
#' @param nrounds Maximum number of boosting rounds per fold.
#' @param params Optional XGBoost parameter list. If `NULL`, a default
#'   regression configuration is used.
#' @param seed Integer seed used for fold assignment.
#'
#' @return A tibble with one row per fold and columns `fold`, `n_valid`, `rmse`,
#'   and `mae`.
run_kfold_cv <- function(X, y, nfold = 5, nrounds = 300, params = NULL, seed = 42) {
  if (is.null(params)) {
    params <- list(
      objective = "reg:squarederror",
      eval_metric = "rmse",
      eta = 0.05,
      max_depth = 6,
      subsample = 0.8,
      colsample_bytree = 0.8,
      min_child_weight = 1
    )
  }

  set.seed(seed)
  fold_id <- sample(rep(seq_len(nfold), length.out = nrow(X)))
  fold_metrics <- vector("list", nfold)

  pb <- progress::progress_bar$new(
    format = "CV [:bar] :current/:total (:percent) eta: :eta elapsed: :elapsed fold=:fold rmse=:rmse",
    total = nfold,
    clear = FALSE,
    show_after = 0,
    stream = stderr()
  )

  for (fold in seq_len(nfold)) {
    valid_idx <- which(fold_id == fold)
    train_idx <- which(fold_id != fold)

    dtrain <- xgboost::xgb.DMatrix(data = X[train_idx, , drop = FALSE], label = y[train_idx])
    dvalid <- xgboost::xgb.DMatrix(data = X[valid_idx, , drop = FALSE], label = y[valid_idx])

    model <- xgboost::xgb.train(
      params = params,
      data = dtrain,
      nrounds = nrounds,
      evals = list(train = dtrain, eval = dvalid),
      early_stopping_rounds = 100,
      verbose = 100
    )

    pred <- predict(model, dvalid)
    fold_rmse <- sqrt(mean((pred - y[valid_idx])^2))
    fold_mae <- mean(abs(pred - y[valid_idx]))

    fold_metrics[[fold]] <- tibble::tibble(
      fold = fold,
      n_valid = length(valid_idx),
      rmse = fold_rmse,
      mae = fold_mae
    )

    pb$tick(tokens = list(
      fold = fold,
      rmse = sprintf("%.5f", fold_rmse)
    ))
  }

  bind_rows(fold_metrics)
}

#' Tune XGBoost Hyperparameters With Internal Cross-Validation
#'
#' Evaluates a fixed hyperparameter grid with `xgb.cv()`, tracks the best RMSE
#' and boosting round for each configuration, and returns the best-performing
#' parameter set.
#'
#' @param X Numeric feature matrix.
#' @param y Numeric response vector aligned with `X`.
#' @param nfold Number of folds passed to `xgb.cv()`.
#' @param nrounds Maximum number of boosting rounds evaluated per grid row.
#' @param early_stopping_rounds Early-stopping patience used by `xgb.cv()`.
#' @param seed Integer seed used to stabilize each grid evaluation.
#'
#' @return A list with `best_params`, `best_round`, and `tuning_results`
#'   ordered by performance.
tune_hyperparams <- function(
  X,
  y,
  nfold = 5,
  nrounds = 2000,
  early_stopping_rounds = 100,
  seed = 42
) {
  dtrain <- xgboost::xgb.DMatrix(data = X, label = y)

  grid <- expand.grid(
    eta = c(0.03, 0.05, 0.1),
    max_depth = c(4L, 6L, 8L),
    subsample = c(0.7, 0.9),
    colsample_bytree = c(0.7, 0.9),
    min_child_weight = c(1, 5),
    stringsAsFactors = FALSE
  )

  base_params <- list(
    objective = "reg:squarederror",
    eval_metric = "rmse"
  )

  pb <- progress::progress_bar$new(
    format = "Tuning [:bar] :current/:total (:percent) eta: :eta elapsed: :elapsed rmse=:rmse",
    total = nrow(grid),
    clear = FALSE,
    show_after = 0,
    stream = stderr()
  )

  results <- vector("list", nrow(grid))

  for (i in seq_len(nrow(grid))) {
    row <- grid[i, ]
    params <- c(base_params, as.list(row))

    set.seed(seed + i)
    cv <- xgboost::xgb.cv(
      params = params,
      data = dtrain,
      nrounds = nrounds,
      nfold = nfold,
      early_stopping_rounds = early_stopping_rounds,
      verbose = 1
    )

    best_round <- if (!is.null(cv$best_iteration)) cv$best_iteration else nrounds
    best_rmse <- if (!is.null(cv$best_score)) as.numeric(cv$best_score) else min(cv$evaluation_log$test_rmse_mean, na.rm = TRUE)

    results[[i]] <- tibble::tibble(
      eta = row$eta,
      max_depth = row$max_depth,
      subsample = row$subsample,
      colsample_bytree = row$colsample_bytree,
      min_child_weight = row$min_child_weight,
      best_round = best_round,
      best_rmse = best_rmse
    )

    pb$tick(tokens = list(rmse = sprintf("%.5f", best_rmse)))
  }

  tuning_results <- dplyr::bind_rows(results) |> dplyr::arrange(best_rmse, best_round)
  best <- tuning_results |> dplyr::slice(1)

  best_params <- list(
    objective = "reg:squarederror",
    eval_metric = "rmse",
    eta = best$eta[[1]],
    max_depth = as.integer(best$max_depth[[1]]),
    subsample = best$subsample[[1]],
    colsample_bytree = best$colsample_bytree[[1]],
    min_child_weight = best$min_child_weight[[1]]
  )

  list(
    best_params = best_params,
    best_round = as.integer(best$best_round[[1]]),
    tuning_results = tuning_results
  )
}

#' Train A Final XGBoost Regression Model
#'
#' Fits an XGBoost regressor on the full design matrix using either supplied
#' hyperparameters or the script defaults.
#'
#' @param X Numeric feature matrix.
#' @param y Numeric response vector aligned with `X`.
#' @param nrounds Number of boosting rounds to train.
#' @param params Optional XGBoost parameter list. If `NULL`, a default
#'   regression configuration is used.
#'
#' @return An `xgb.Booster` object trained on the full dataset.
train_final_model <- function(X, y, nrounds = 300, params = NULL) {
  if (is.null(params)) {
    params <- list(
      objective = "reg:squarederror",
      eval_metric = "rmse",
      eta = 0.05,
      max_depth = 6,
      subsample = 0.8,
      colsample_bytree = 0.8,
      min_child_weight = 1
    )
  }

  dtrain <- xgboost::xgb.DMatrix(data = X, label = y)

  xgboost::xgb.train(
    params = params,
    data = dtrain,
    nrounds = nrounds,
    verbose = 100
  )
}

#' Convert Objects To Single-String Log Output
#'
#' @param x Any R object.
#'
#' @return A length-1 character string suitable for `logger::log_info()`.
format_for_log <- function(x) {
  paste(utils::capture.output(print(x)), collapse = "\n")
}

feats <- read.csv(paste0(RDF_PATH, "xgb_features.csv")) |>
  dplyr::rename(
    EVENT_ID = event_id,
    MUID = match_id
  ) |>
  dplyr::mutate(
    MUID = stringr::str_sub(MUID, 1, -4 - 1)
  )

labels <- read.csv(paste0(RDF_PATH,"xgb_labels.csv"))
passes <- build_dataset(feats, labels, offensive = TRUE)
logger::log_info("built dataset")
prepared <- prepare_training_matrix(passes)
X <- prepared$X
y <- prepared$y

nfold <- 5
tuning_nrounds <- 10
logger::log_info("beginning hyperparameter tuning")
tuned <- tune_hyperparams(
  X,
  y,
  nfold = nfold,
  seed = 42
)
best_rmse <- tuned$tuning_results |> dplyr::select("best_rmse") |> dplyr::pull(1)
logger::log_info("Best RMSE")
logger::log_info(best_rmse)
logger::log_info("selected params")
logger::log_info(format_for_log(tuned$best_params))
logger::log_info(sprintf("selected best_round=%d", tuned$best_round))

final_model <- train_final_model(X, y, nrounds = tuned$best_round, params = tuned$best_params)
xgb.save(final_model, paste0(RDF_PATH, "xgb_offense_successful.json"))
write.csv(
  tuned$tuning_results,
  paste0(RDF_PATH, "xgb_offense_successful_tuning_results.csv"),
  row.names = FALSE
)
writeLines(
  as.character(tuned$best_round),
  paste0(RDF_PATH, "xgb_offense_successful_best_round.txt")
)

import os

from kloppy import sportec
import pandas as pd
from tqdm import tqdm

import pandas as pd

def tracking_wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts wide kloppy output of tracking data to more manageable long format

    Inputs:
        df - wide kloppy output
    Returns
        df in long format
    """
    track_cols = [c for c in df.columns if c.startswith("DFL-OBJ-") and c.count("_") >= 1]

    id_cols = [c for c in df.columns if c not in track_cols]
    m = df.melt(
        id_vars=id_cols,
        value_vars=track_cols,
        var_name="var",
        value_name="value"
    )
    obj_metric = m["var"].str.rsplit("_", n=1, expand=True)
    m["object_id"] = obj_metric[0]
    m["metric"] = obj_metric[1]

    m = m.drop(columns=["var"])

    long_df = (
        m.pivot_table(
            index=id_cols + ["object_id"],
            columns="metric",
            values="value",
            aggfunc="first" 
        )
        .reset_index()
    )

    long_df.columns.name = None

    return long_df

def add_ball_rows(df_wide: pd.DataFrame, players_long: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces ball columns with its own designated row in the tracking data to avoid unnecessary duplicates
    """
    key_cols = ["period_id", "timestamp", "frame_id", "ball_state", "ball_owning_team_id"]

    ball_long = (
        df_wide[key_cols + ["ball_x", "ball_y", "ball_z", "ball_speed"]]
        .rename(columns={"ball_speed": "s"})
        .assign(
            object_id="BALL",
            x=lambda d: d["ball_x"],
            y=lambda d: d["ball_y"],
            z=lambda d: d["ball_z"],
        )
        .drop(columns=["ball_x", "ball_y", "ball_z"])
    )

    out = pd.concat([players_long, ball_long], ignore_index=True, sort=False)
    return out.drop(columns = ["ball_x", "ball_y", "ball_z", "ball_speed"])

def main():
    root_path = "../../../rdf/sp161/shared/soccer-decision-making-r/sportec"
    games = os.listdir("../../../rdf/sp161/shared/soccer-decision-making-r/sportec/tracking/xml")
    dfs = []
    for game in tqdm(games):
        event = sportec.load_event(
            event_data= f"{root_path}/event/{game}",
            meta_data=f"{root_path}/match_information/{game}",
            coordinates="sportec",
            event_types=["pass"],
        ).to_df()

        event['event_id'] = event['event_id'].astype(int)
        event['match_id'] = game
        tracking = sportec.load_tracking(
            raw_data= f"{root_path}/tracking/xml/{game}",
            meta_data=f"{root_path}/match_information/{game}",
            sample_rate=1,
            coordinates="sportec",
            only_alive=False,
        ).to_df()

        kpi_path = f"{root_path}/KPI_Merged_all/KPI_MGD_{game[:-4]}.csv"
        kpi_df = pd.read_csv(kpi_path ,sep = ';', encoding='latin-1', on_bad_lines='skip')
        event_frame_map = kpi_df[['EVENT_ID', 'FRAME_NUMBER']]

        event = pd.merge(event, event_frame_map, left_on = "event_id", right_on = "EVENT_ID")
        #because of potential velocity computations, we look at tracking 5 frames in the future and past
        t = tracking[tracking['frame_id'].isin(event['FRAME_NUMBER'])]
        c_frame = tracking_wide_to_long(t)
        c_frame_ball = add_ball_rows(t, c_frame)
        t = tracking[tracking['frame_id'].isin(event['FRAME_NUMBER'] + 5)]
        p5_frame = tracking_wide_to_long(t)
        p5_frame_ball = add_ball_rows(t, p5_frame)

        t = tracking[tracking['frame_id'].isin(event['FRAME_NUMBER'] - 5)]
        m5_frame = tracking_wide_to_long(t)
        m5_frame_ball = add_ball_rows(t, m5_frame)

        p5_frame_ball['og_FRAME_NUMBER'] = p5_frame_ball['frame_id'] - 5
        m5_frame_ball['og_FRAME_NUMBER'] = m5_frame_ball['frame_id'] + 5

        track_frame = pd.merge(c_frame_ball, p5_frame_ball, left_on = ["frame_id", "object_id"], right_on = ["og_FRAME_NUMBER", "object_id"], suffixes = ["", "_p5"])
        track_frame = pd.merge(track_frame, m5_frame_ball, left_on = ["frame_id", "object_id"], right_on = ["og_FRAME_NUMBER", "object_id"], suffixes = ["", "_m5"])
        track_frame = track_frame.drop(columns = ["og_FRAME_NUMBER_m5", "og_FRAME_NUMBER",'period_id', 'timestamp', 'ball_state'])

        event_track = pd.merge(event, track_frame, left_on = "FRAME_NUMBER", right_on = "frame_id")
        dfs.append(event_track)
    all_track = pd.concat(dfs) #needs pyarrow
    all_track.to_parquet(f"{root_path}/passes.parquet")

if __name__ == "__main__":
    main()


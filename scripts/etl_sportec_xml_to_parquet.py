import os

from kloppy import sportec
import pandas as pd
from tqdm import tqdm

import pandas as pd
import numpy as np

RDF_PATH = "/home/lz80/rdf/sp161/shared/soccer-decision-making-r/sportec"

def get_rec(row : pd.Series , tracking : pd.DataFrame, oob_df : pd.DataFrame) -> int:
    """Gets reception frames for a pass
        Inputs:
            row: pd.Series
                a single row of data from the tracking and event data
            tracking: pd.DataFrame
                tracking data from a game of interest
            oob_df: pd.DataFrame
                dataframe containing all points where the ball is out of bounds, precomputed
        Outputs:
            The reception frame
    """
    if not pd.isna(row['RECFRM']):
        return row['RECFRM']
    frame = row['FRAME_NUMBER']
    
    post_frame_track = tracking[(tracking['frame_id'] >= frame) & ((tracking['frame_id'] <= row['NEXT_FRAME']))]
    if not pd.isna(row['PUID2']):
        #if we know the receiver, choose frame where receiver is closest
        recept_player = row['PUID2']
        post_frame_track['ball_dist'] = (post_frame_track['ball_x'] - post_frame_track[f'{recept_player}_x'])**2 + (post_frame_track['ball_y'] - post_frame_track[f'{recept_player}_y'])**2
        min_index = post_frame_track['ball_dist'].idxmin()
        return post_frame_track.loc[min_index, 'frame_id']
    if row['result'] == 'OUT':
        #if we know it went out, take next out frame
        oob = oob_df[oob_df['frame_id'] >= frame]
        if oob.shape[0] > 0:
            return oob['frame_id'].iloc[0]
        
    #else: perform 20 degree interception approach
    # checks if the angle between the ball velocity at ball pass 
    pass_x, pass_y = post_frame_track[post_frame_track['frame_id'] == frame][['x_velo', 'y_velo']].iloc[0].values

    p = np.array([pass_x, pass_y], dtype=float)
    p_norm = np.linalg.norm(p)

    v = post_frame_track[["x_velo", "y_velo"]].to_numpy(dtype=float)
    v_norm = np.linalg.norm(v, axis=1)
    dot = v @ p 

    denom = v_norm * p_norm

    cos_theta = np.full(len(post_frame_track), np.nan)
    mask = denom > 0

    cos_theta[mask] = dot[mask] / denom[mask]
    cos_theta = np.clip(cos_theta, -1.0, 1.0)
    post_frame_track["angle_deg"] = np.degrees(np.arccos(cos_theta))

    intercept_mask = (post_frame_track['angle_deg'] >= 20) | ((abs(post_frame_track['ball_x']) > 105/2 -.02) | (abs(post_frame_track['ball_y']) > 34 -.02))
    over_twenty = post_frame_track[intercept_mask]

    if over_twenty.shape[0] == 0:
        # if no such frame, simply take 2 seconds from pass
        return min(row['NEXT_FRAME'], frame + 50, tracking['frame_id'].max())
    
    return over_twenty['frame_id'].iloc[0]


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
    root_path = f"{RDF_PATH}"
    games = os.listdir(f"{RDF_PATH}/tracking/xml")
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

        tracking['x_velo'] = tracking['ball_x'] - tracking['ball_x'].shift(-10)
        tracking['y_velo'] = tracking['ball_y'] - tracking['ball_y'].shift(-10)

        kpi_path = f"{root_path}/KPI_Merged_all/KPI_MGD_{game[:-4]}.csv"
        kpi_df = pd.read_csv(kpi_path ,sep = ';', encoding='latin-1', on_bad_lines='skip')
        
        kpi_df = kpi_df.drop_duplicates(subset = "FRAME_NUMBER").sort_values(by = "FRAME_NUMBER")
        kpi_df['NEXT_FRAME'] = kpi_df['FRAME_NUMBER'].shift(-1)
        
        kpi_df = pd.merge(kpi_df, event[['event_id', 'result']], left_on = "EVENT_ID", right_on = "event_id").sort_values(by = "FRAME_NUMBER")

        oob_df = tracking[(abs(tracking['ball_x']) > (105/2 -.02)) | (abs(tracking['ball_y']) > (34 -.02))] #small tolerance for tracking issues
        kpi_df = kpi_df[(kpi_df['NEXT_FRAME'] - kpi_df['FRAME_NUMBER']) >= 10]

        kpi_df['n_RECFRM'] = kpi_df.apply(lambda x: get_rec(x, tracking, oob_df), axis = 1)
        kpi_df = kpi_df[(kpi_df['n_RECFRM'] - kpi_df['FRAME_NUMBER']) >= 10]

        event_frame_map = kpi_df[['EVENT_ID', 'FRAME_NUMBER', 'n_RECFRM']]

        event = pd.merge(event, event_frame_map, left_on = "event_id", right_on = "EVENT_ID")

        t = tracking[tracking['frame_id'].isin(event['n_RECFRM'])]
        rec_frame = tracking_wide_to_long(t)
        rec_frame_ball = add_ball_rows(t, rec_frame)
        rec_frame_ball = rec_frame_ball.drop(columns = ["period_id", "timestamp", "ball_state"])       

        event_recept = pd.merge(event, rec_frame_ball, left_on = "n_RECFRM", right_on = "frame_id")
        event_recept['TYPE'] = 'RECEPTION'
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
        event_track['TYPE'] = 'PASS'

        all_df = pd.concat([event_track, event_recept], axis=0)
        dfs.append(all_df)
    all_track = pd.concat(dfs) #needs pyarrow
    all_track.to_parquet(f"{root_path}/passes.parquet")

if __name__ == "__main__":
    #script will take ~7 hrs for all 306 matches
    main()


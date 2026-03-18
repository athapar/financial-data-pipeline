import pandas as pd

def compute_daily_returns(
        df: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute simple daily close to close returns

    Assumptions:
    - `t` is a timestamp column
    - `c` is close price
    """

    if df is None or df.empty:
        return df
    
    df_out = df.sort_values("t").copy()
    df_out['daily_return'] = df_out['c'].pct_change()

    return df_out
import pandas as pd

def save_to_csv(df, path):
    df.to_csv(path, index=False)
    print(f"Saved: {path}")

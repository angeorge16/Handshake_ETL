#import pandas as pd

#def save_to_csv(df, path):
    #df.to_csv(path, index=False)
    #print(f"Saved: {path}")


import os
from datetime import datetime
import pandas as pd

def save_to_csv(df, category, extra_info=None):
    """
    Save a DataFrame to a timestamped CSV file in the appropriate output folder.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to save.
    category : str
        One of ['current', 'alumni','mapping_codes'] — determines the folder and filename prefix.
    extra_info : str, optional
        Additional info to append in filename (e.g. 'delta', 'full', or term code).
    """
    # 1️⃣ Define base folder
    base_folder = os.path.join("output", category)
    os.makedirs(base_folder, exist_ok=True)

    # 2️⃣ Create timestamp (standard, sortable)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 3️⃣ Build filename
    if extra_info:
        filename = f"{category}_{extra_info}_{timestamp}.csv"
    else:
        filename = f"{category}_{timestamp}.csv"

    # 4️⃣ Full output path
    output_path = os.path.join(base_folder, filename)

    # 5️⃣ Save CSV
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")

    return output_path


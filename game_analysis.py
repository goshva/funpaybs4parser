import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import sqlite3
import numpy as np
from datetime import datetime

def load_games_data(db_path='funpay.db'):
    """
    Load games data from SQLite database and ensure no empty or null values.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all tables that start with 'games_'
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'games_%'")
    tables = cursor.fetchall()

    all_data = []
    for table in tables:
        table_name = table[0]

        # Get columns for this table
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]

        # Skip tables with only basic columns
        if len(columns) <= 3:
            continue

        numerical_columns = [col for col in columns if col not in ('game_id', 'game_url', 'game_title')]
        if not numerical_columns:
            continue

        # Read the table and drop rows with missing values
        query = f"""
            SELECT game_id, game_title, {', '.join(numerical_columns)}
            FROM {table_name}
            WHERE game_title IS NOT NULL
        """
        df = pd.read_sql_query(query, conn).dropna()

        # Extract timestamp from table name
        try:
            timestamp = pd.to_datetime(table_name.split('_')[1] + '_' + table_name.split('_')[2], 
                                       format='%Y%m%d_%H%M%S')
            df['timestamp'] = timestamp
            all_data.append(df)
        except (IndexError, ValueError):
            continue  # Skip tables with incorrect naming format

    conn.close()

    if not all_data:
        raise ValueError("No valid games_* tables found with numerical data")

    return pd.concat(all_data, ignore_index=True)

def get_top_10_games(df, metric_column):
    """
    Get top 10 games by latest absolute values and biggest changes.
    """
    latest_timestamp = df['timestamp'].max()
    latest_values = df[df['timestamp'] == latest_timestamp][['game_title', metric_column]].dropna()

    if latest_values.empty:
        return None, None

    top_10_absolute = latest_values.nlargest(10, metric_column)

    pivot_df = df.pivot_table(index='timestamp', columns='game_title', values=metric_column, aggfunc='mean')
    
    if pivot_df.shape[0] < 2:  # Ensure at least two timestamps exist for change calculation
        return top_10_absolute, None
    
    total_changes = pivot_df.iloc[-1] - pivot_df.iloc[0]
    top_10_changes = total_changes.abs().nlargest(10).dropna()

    return top_10_absolute, pd.DataFrame(top_10_changes)

def plot_time_series_for_games(df, games_list, metric_column, title, y_label, plot_type='absolute', output_dir=None):
    """
    Plot time series data for specific games only if valid data exists.
    """
    if not games_list:
        return  # No valid games to plot

    pivot_df = df.pivot_table(index='timestamp', columns='game_title', values=metric_column, aggfunc='mean')

    # Ensure only non-empty data is used
    plot_data = pivot_df[games_list].dropna(how='all')
    if plot_data.empty:
        return

    plt.figure(figsize=(15, 8))
    sns.set_style("whitegrid")
    sns.set_palette("husl")

    for column in plot_data.columns:
        plt.plot(plot_data.index, plot_data[column], label=column, marker='o', markersize=4)

    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel(y_label)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title="Games")
    plt.grid(True, alpha=0.3)
    plt.axhline(y=0, color='black', linestyle='-', alpha=0.2) if plot_type == 'change' else None
    plt.tight_layout()

    if output_dir:
        output_path = output_dir / f"{title.replace(' ', '_')}.png"
        plt.savefig(output_path, bbox_inches='tight', dpi=300)
        plt.close()

def main():
    try:
        print("Loading games data...")
        df = load_games_data()
        timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_dir = Path(f"reports/{timestamp_str}")
        output_dir.mkdir(parents=True, exist_ok=True)

        metrics = [col for col in df.select_dtypes(include=['int64', 'float64']).columns if col != 'game_id']
        if not metrics:
            print("No numerical metrics found. Exiting.")
            return

        for metric in metrics:
            print(f"Analyzing {metric}...")

            top_10_absolute, top_10_changes = get_top_10_games(df, metric)
            if top_10_absolute is None:
                continue  # Skip metric if no valid data exists

            # Generate plots
            plot_time_series_for_games(df, top_10_absolute['game_title'].tolist(), metric,
                                       f"Top 10 Games by {metric} (Absolute Values)", metric, 'absolute', output_dir)
            if top_10_changes is not None:
                plot_time_series_for_games(df, top_10_changes.index.tolist(), metric,
                                           f"Top 10 Games by {metric} (Rate of Change)", f"{metric} Change (%)",
                                           'change', output_dir)

            # Save summary report
            report_path = output_dir / f"{metric}_analysis_report.txt"
            with open(report_path, 'w') as f:
                f.write(f"Analysis Report for {metric}\n")
                f.write(f"Generated at: {timestamp_str} UTC\n")
                f.write("="*50 + "\n\n")

                f.write("Top 10 Games by Absolute Values:\n")
                f.write("-"*30 + "\n")
                f.write(top_10_absolute.to_string())
                f.write("\n\n")

                if top_10_changes is not None:
                    f.write("Top 10 Games by Total Change:\n")
                    f.write("-"*30 + "\n")
                    f.write(top_10_changes.to_string())
                    f.write("\n\n")

                f.write("Basic Statistics:\n")
                f.write("-"*30 + "\n")
                stats = df.groupby('game_title')[metric].agg(['mean', 'min', 'max', 'std']).dropna()
                f.write(stats.to_string())

        print(f"Analysis complete! Reports saved in {output_dir}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()

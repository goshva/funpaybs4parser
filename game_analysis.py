import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import sqlite3
import glob

def load_games_data(db_path='funpay.db'):
    """
    Load games data from SQLite database
    """
    conn = sqlite3.connect(db_path)
    
    # Get all tables that start with 'games_'
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'games_%'")
    tables = cursor.fetchall()
    
    # Combine all games_* tables
    all_data = []
    for table in tables:
        table_name = table[0]
        # Get columns for this table
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Skip tables with only basic columns
        if len(columns) <= 3:  # game_id, game_url, game_title are basic columns
            continue
            
        # Get numerical columns (excluding basic columns)
        numerical_columns = [col for col in columns 
                           if col not in ('game_id', 'game_url', 'game_title')]
        
        if not numerical_columns:
            continue
            
        # Read the table
        query = f"SELECT game_id, game_title, {', '.join(numerical_columns)} FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        
        # Add timestamp column from table name
        timestamp = pd.to_datetime(table_name.split('_')[1] + '_' + table_name.split('_')[2], 
                                 format='%Y%m%d_%H%M%S')
        df['timestamp'] = timestamp
        
        all_data.append(df)
    
    conn.close()
    
    if not all_data:
        raise ValueError("No games_* tables found with numerical data")
    
    # Combine all dataframes
    combined_df = pd.concat(all_data, ignore_index=True)
    return combined_df

def create_time_series_analysis(df, metric_column, 
                              group_by='game_id', 
                              resample_freq='D'):
    """
    Create time series analysis grouped by game_id
    
    Parameters:
    - df: DataFrame with games data
    - metric_column: Column to analyze
    - group_by: Column to group by (default: 'game_id')
    - resample_freq: Frequency for resampling ('D' for daily, 'W' for weekly, etc.)
    """
    # Group by timestamp and game_id, then calculate mean for the metric
    grouped = df.groupby([pd.Grouper(key='timestamp', freq=resample_freq), group_by])[metric_column].mean()
    # Unstack to create a matrix with games as columns
    return grouped.unstack()

def plot_time_series(df, title="Game Metrics Over Time", 
                    y_label="Value", figsize=(15, 8)):
    """
    Plot time series data
    """
    plt.figure(figsize=figsize)
    sns.set_style("whitegrid")
    sns.set_palette("husl")
    
    # Plot each game's time series
    for column in df.columns:
        plt.plot(df.index, df[column], label=f"Game: {column}", marker='o', markersize=4)
    
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel(y_label)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    return plt

def main():
    try:
        # Load the data
        print("Loading games data...")
        df = load_games_data()
        
        # Get available metrics (numerical columns)
        metrics = df.select_dtypes(include=['int64', 'float64']).columns
        metrics = [col for col in metrics if col != 'game_id']
        
        print(f"Available metrics: {', '.join(metrics)}")
        
        for metric in metrics:
            print(f"Creating time series analysis for {metric}...")
            
            # Daily analysis
            daily_data = create_time_series_analysis(df, 
                                                   metric_column=metric,
                                                   resample_freq='D')
            
            # Weekly analysis
            weekly_data = create_time_series_analysis(df, 
                                                    metric_column=metric,
                                                    resample_freq='W')
            
            # Create and save plots
            print(f"Generating plots for {metric}...")
            
            # Daily plot
            if not daily_data.empty:
                plot = plot_time_series(daily_data, 
                                      title=f"Daily {metric} Changes by Game",
                                      y_label=metric)
                plot.savefig(f'daily_{metric}_changes.png')
                plt.close()
            
            # Weekly plot
            if not weekly_data.empty:
                plot = plot_time_series(weekly_data,
                                      title=f"Weekly {metric} Changes by Game",
                                      y_label=metric)
                plot.savefig(f'weekly_{metric}_changes.png')
                plt.close()
            
        print("Analysis complete! Check the generated PNG files for visualizations.")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
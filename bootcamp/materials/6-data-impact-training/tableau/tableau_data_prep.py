import pandas as pd
import os

def prepare_top_players_data(match_details_df, output_path):
    """Prepare top players by kills data"""
    top_players = match_details_df.groupby('player_gamertag').agg({
        'match_id': 'count',
        'player_total_kills': ['mean', 'sum']
    }).reset_index()
    
    # Rename columns
    top_players.columns = ['player_gamertag', 'total_matches', 'avg_kills_per_game', 'total_kills']
    
    # Filter and sort
    top_players = top_players[top_players['total_matches'] >= 10] \
        .sort_values('avg_kills_per_game', ascending=False) \
        .head(10)
    
    # Round averages
    top_players['avg_kills_per_game'] = top_players['avg_kills_per_game'].round(2)
    
    top_players.to_csv(os.path.join(output_path, "top_players.csv"), index=False)

def prepare_map_popularity_data(matches_df, maps_df, output_path):
    """Prepare map popularity data"""
    # Rename columns for clarity
    maps_df = maps_df.rename(columns={'name': 'map_name'})
    
    map_popularity = matches_df.merge(maps_df, left_on='mapid', right_on='mapid') \
        .groupby('map_name').size().reset_index(name='total_matches')
    
    # Calculate percentages
    total_matches = map_popularity['total_matches'].sum()
    map_popularity['percentage_of_total'] = (map_popularity['total_matches'] * 100.0 / total_matches).round(2)
    
    map_popularity = map_popularity.sort_values('total_matches', ascending=False)
    map_popularity.to_csv(os.path.join(output_path, "map_popularity.csv"), index=False)

def prepare_killing_spree_data(medals_matches_df, medals_df, matches_df, maps_df, output_path):
    """Prepare killing spree distribution data"""
    # Rename columns for clarity
    maps_df = maps_df.rename(columns={'name': 'map_name'})
    
    # We'll consider these medal IDs as killing spree related
    # This is a simplified approach since we don't have clear medal descriptions
    killing_spree_medals = medals_matches_df['medal_id'].value_counts().head(5).index.tolist()
    
    # Merge all required tables
    killing_sprees = medals_matches_df[medals_matches_df['medal_id'].isin(killing_spree_medals)] \
        .merge(matches_df, on='match_id') \
        .merge(maps_df, left_on='mapid', right_on='mapid')
    
    # Calculate statistics by map
    spree_stats = killing_sprees.groupby('map_name').agg({
        'match_id': ['count', 'nunique']
    }).reset_index()
    
    # Rename columns
    spree_stats.columns = ['map_name', 'total_sprees', 'matches_with_sprees']
    
    # Calculate sprees per match
    spree_stats['sprees_per_match'] = (spree_stats['total_sprees'] / spree_stats['matches_with_sprees']).round(2)
    
    spree_stats = spree_stats.sort_values('total_sprees', ascending=False)
    spree_stats.to_csv(os.path.join(output_path, "killing_sprees.csv"), index=False)

def prepare_match_details_data(match_details_df, matches_df, maps_df, output_path):
    """Prepare detailed match performance data"""
    # Rename columns for clarity
    maps_df = maps_df.rename(columns={'name': 'map_name'})
    
    match_details = match_details_df.merge(matches_df, on='match_id') \
        .merge(maps_df, left_on='mapid', right_on='mapid')
    
    # Calculate K/D ratio
    match_details['kd_ratio'] = match_details.apply(
        lambda row: row['player_total_kills'] if row['player_total_deaths'] == 0 
        else round(row['player_total_kills'] / row['player_total_deaths'], 2),
        axis=1
    )
    
    # Select and reorder columns
    final_details = match_details[[
        'match_id', 'completion_date', 'map_name', 'playlist_id',
        'player_gamertag', 'player_total_kills', 'player_total_deaths', 
        'player_total_assists', 'kd_ratio'
    ]]
    
    # Rename columns for clarity
    final_details.columns = [
        'match_id', 'match_date', 'map_name', 'playlist_name',
        'player_name', 'kills', 'deaths', 'assists', 'kd_ratio'
    ]
    
    final_details.to_csv(os.path.join(output_path, "match_details.csv"), index=False)

def main():
    try:
        # Create output directory if it doesn't exist
        output_path = "data/tableau"
        os.makedirs(output_path, exist_ok=True)
        
        # Source data path
        input_path = "bootcamp/materials/3-spark-fundamentals/data"
        
        # Read input CSV files
        match_details_df = pd.read_csv(f"{input_path}/match_details.csv")
        matches_df = pd.read_csv(f"{input_path}/matches.csv")
        medals_df = pd.read_csv(f"{input_path}/medals.csv")
        medals_matches_df = pd.read_csv(f"{input_path}/medals_matches_players.csv")
        maps_df = pd.read_csv(f"{input_path}/maps.csv")
        
        # Prepare all datasets
        prepare_top_players_data(match_details_df, output_path)
        prepare_map_popularity_data(matches_df, maps_df, output_path)
        prepare_killing_spree_data(medals_matches_df, medals_df, matches_df, maps_df, output_path)
        prepare_match_details_data(match_details_df, matches_df, maps_df, output_path)
        
        print(f"Data preparation complete. Files saved in {output_path}")
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        raise

if __name__ == "__main__":
    main() 
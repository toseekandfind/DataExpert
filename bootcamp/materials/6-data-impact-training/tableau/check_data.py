import pandas as pd

def check_file(file_path):
    """Print information about a CSV file"""
    print(f"\nChecking file: {file_path}")
    df = pd.read_csv(file_path)
    print("Columns:", df.columns.tolist())
    print("First row:", df.iloc[0].to_dict())
    print("Shape:", df.shape)
    print("-" * 80)

def main():
    base_path = "bootcamp/materials/3-spark-fundamentals/data"
    files = [
        "match_details.csv",
        "matches.csv",
        "medals.csv",
        "medals_matches_players.csv",
        "maps.csv"
    ]
    
    for file in files:
        try:
            check_file(f"{base_path}/{file}")
        except Exception as e:
            print(f"Error processing {file}: {str(e)}")

if __name__ == "__main__":
    main() 
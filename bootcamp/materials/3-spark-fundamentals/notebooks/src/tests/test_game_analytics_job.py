import pytest
from pyspark.sql import SparkSession
from jobs.game_analytics_job import (
    create_spark_session,
    analyze_player_kills,
    analyze_playlist_popularity,
    analyze_map_popularity,
    analyze_killing_spree_maps
)

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing"""
    return SparkSession.builder \
        .appName("TestGameAnalytics") \
        .master("local[1]") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate()

@pytest.fixture
def sample_match_details(spark):
    """Create sample match details data"""
    data = [
        ("match1", "player1", 10),  # match_id, player_id, kills
        ("match1", "player2", 5),
        ("match2", "player1", 15),
        ("match2", "player2", 8)
    ]
    return spark.createDataFrame(data, ["match_id", "player_id", "kills"])

@pytest.fixture
def sample_matches(spark):
    """Create sample matches data"""
    data = [
        ("match1", "map1", "ranked"),  # match_id, map_id, playlist
        ("match2", "map2", "casual"),
        ("match3", "map1", "ranked")
    ]
    return spark.createDataFrame(data, ["match_id", "map_id", "playlist"])

@pytest.fixture
def sample_medals(spark):
    """Create sample medals data"""
    data = [
        ("medal1", "Killing Spree"),  # medal_id, medal_name
        ("medal2", "Double Kill")
    ]
    return spark.createDataFrame(data, ["medal_id", "medal_name"])

@pytest.fixture
def sample_maps(spark):
    """Create sample maps data"""
    data = [
        ("map1", "Haven"),  # map_id, map_name
        ("map2", "Bind")
    ]
    return spark.createDataFrame(data, ["map_id", "map_name"])

@pytest.fixture
def sample_medal_matches_players(spark):
    """Create sample medal matches players data"""
    data = [
        ("match1", "player1", "medal1"),  # match_id, player_id, medal_id
        ("match2", "player2", "medal1"),
        ("match1", "player2", "medal2")
    ]
    return spark.createDataFrame(data, ["match_id", "player_id", "medal_id"])

def test_analyze_player_kills(spark, sample_match_details, sample_matches):
    """Test player kills analysis"""
    result = analyze_player_kills(sample_match_details, sample_matches).collect()
    
    assert len(result) == 2
    # player1 should have highest average kills
    assert result[0]["player_id"] == "player1"
    assert result[0]["avg_kills"] == 12.5  # (10 + 15) / 2
    assert result[0]["total_games"] == 2

def test_analyze_playlist_popularity(spark, sample_matches):
    """Test playlist popularity analysis"""
    result = analyze_playlist_popularity(sample_matches).collect()
    
    assert len(result) == 2
    # ranked should be most popular
    assert result[0]["playlist"] == "ranked"
    assert result[0]["times_played"] == 2

def test_analyze_map_popularity(spark, sample_matches, sample_maps):
    """Test map popularity analysis"""
    result = analyze_map_popularity(sample_matches, sample_maps).collect()
    
    assert len(result) == 2
    # Haven (map1) should be most played
    assert result[0]["map_name"] == "Haven"
    assert result[0]["times_played"] == 2

def test_analyze_killing_spree_maps(
    spark, sample_medal_matches_players, sample_medals, 
    sample_matches, sample_maps
):
    """Test killing spree analysis"""
    result = analyze_killing_spree_maps(
        sample_medal_matches_players, sample_medals,
        sample_matches, sample_maps
    ).collect()
    
    assert len(result) == 2
    # Check counts of killing sprees per map
    assert result[0]["spree_count"] == 1  # Each map should have 1 spree 
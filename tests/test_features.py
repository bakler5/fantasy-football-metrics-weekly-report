__author__ = "Wren J. R. (uberfastman)"
__email__ = "uberfastman@uberfastman.dev"

import os
import sys
from pathlib import Path

from pytest import mark

module_dir = Path(__file__).parent.parent
sys.path.append(str(module_dir))

from utils.app_config_parser import AppConfigParser

from calculate.bad_boy_stats import BadBoyStats
from calculate.beef_stats import BeefStats
from calculate.covid_risk import CovidRisk

from report.logger import get_logger

logger = get_logger(__file__)

test_data_dir = Path(module_dir) / "tests"
if not Path(test_data_dir).exists():
    os.makedirs(test_data_dir)

config = AppConfigParser()
config.read(Path(__file__).parent.parent / "config.ini")

player_first_name = "Marquise"
player_last_name = "Brown"
player_full_name = f"{player_first_name} {player_last_name}"
player_team_abbr = "ARI"
player_position = "WR"


def test_bad_boy_init():
    bad_boy_stats = BadBoyStats(
        data_dir=test_data_dir,
        save_data=True,
        dev_offline=False,
        refresh=True
    )
    bad_boy_stats.generate_crime_categories_json()

    logger.info(
        f"\nPlayer Bad Boy crime for {player_first_name} {player_last_name}: "
        f"{bad_boy_stats.get_player_bad_boy_crime(player_first_name, player_last_name, player_team_abbr, player_position)}"
    )
    logger.info(
        f"\nPlayer Bad Boy points for {player_first_name} {player_last_name}: "
        f"{bad_boy_stats.get_player_bad_boy_points(player_first_name, player_last_name, player_team_abbr, player_position)}"
    )

    assert bad_boy_stats.bad_boy_data is not None


def test_beef_init():
    beef_stats = BeefStats(
        data_dir=test_data_dir,
        save_data=True,
        dev_offline=False,
        refresh=True
    )
    beef_stats.generate_player_info_json()

    logger.info(
        f"\nPlayer weight for {player_full_name}: "
        f"{beef_stats.get_player_weight(player_first_name, player_last_name, player_team_abbr)}"
    )
    logger.info(
        f"\nPlayer TABBU for {player_full_name}: "
        f"{beef_stats.get_player_tabbu(player_first_name, player_last_name, player_team_abbr)}"
    )

    assert beef_stats.beef_data is not None


@mark.skip
def test_covid_init():
    covid_risk = CovidRisk(
        config=config,
        data_dir=test_data_dir,
        season=2020,
        week=1,
        save_data=True,
        dev_offline=False,
        refresh=True
    )
    covid_risk.generate_covid_risk_json()

    logger.info(
        f"\nCOVID-19 risk for {player_full_name}: "
        f"{covid_risk.get_player_covid_risk(player_full_name, player_team_abbr, player_position)}"
    )

    assert covid_risk.covid_data is not None


if __name__ == "__main__":
    logger.info("Testing features...")

    # uncomment below function to test bad boy data retrieval
    test_bad_boy_init()

    # uncomment below function to test player weight (beef) data retrieval
    test_beef_init()

    # uncomment below function to test player covid data retrieval
    test_covid_init()

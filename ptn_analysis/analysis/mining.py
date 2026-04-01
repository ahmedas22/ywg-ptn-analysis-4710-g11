"""Association rule mining for transit-equity pattern discovery.

Uses Apriori (mlxtend) on binarized neighbourhood features to discover
co-occurrence patterns between demographic vulnerability and transit
service characteristics.
"""

from __future__ import annotations

from loguru import logger
import pandas as pd

from ptn_analysis.analysis.base import AnalyzerBase
from ptn_analysis.context.db import TransitDB


class AssociationRuleMiner(AnalyzerBase):
    """Mine association rules from neighbourhood-level transit and census data.

    Args:
        city_key: City namespace.
        feed_id: Feed identifier.
        db_instance: Database handle.
    """

    def __init__(
        self,
        city_key: str,
        feed_id: str,
        db_instance: TransitDB,
    ) -> None:
        super().__init__(city_key, feed_id, db_instance)

    def build_binary_feature_matrix(self) -> pd.DataFrame:
        """Build a binarized feature matrix for Apriori.

        Binarizes 15+ features against their medians. Each row is a
        neighbourhood, each column is a boolean feature.

        Returns:
            Boolean DataFrame suitable for ``mlxtend.frequent_patterns.apriori``.
        """
        census_view = self._table("census_by_neighbourhood")
        density_tbl = self._table("neighbourhood_stop_count_density")

        if not (
            self._db.relation_exists(census_view)
            and self._db.relation_exists(density_tbl)
        ):
            logger.warning("Census or density tables missing for mining.")
            return pd.DataFrame()

        base = self._db.query(
            f"""
            SELECT c.neighbourhood_id, c.neighbourhood,
                   c.population_density_per_km2,
                   c.pct_commute_public_transit,
                   c.pct_commute_car,
                   c.median_household_income_2020,
                   c.pct_seniors_65_plus,
                   c.pct_recent_immigrants,
                   d.stop_density_per_km2,
                   d.stop_count
            FROM {census_view} c
            LEFT JOIN {density_tbl} d
                ON c.neighbourhood_id = d.neighbourhood_id
               AND d.feed_id = :feed_id
            WHERE c.population_density_per_km2 > 0
            """,
            {"feed_id": self._feed_id},
        )
        if base.empty:
            return base

        binary = pd.DataFrame({"neighbourhood": base["neighbourhood"]})

        # Binarize against medians
        binary["high_density"] = (
            base["population_density_per_km2"]
            > base["population_density_per_km2"].median()
        )
        binary["low_stop_density"] = (
            base["stop_density_per_km2"]
            < base["stop_density_per_km2"].median()
        )
        binary["high_transit_commute"] = (
            base["pct_commute_public_transit"]
            > base["pct_commute_public_transit"].median()
        )
        binary["high_car_commute"] = (
            base["pct_commute_car"]
            > base["pct_commute_car"].median()
        )
        binary["low_income"] = (
            base["median_household_income_2020"]
            < base["median_household_income_2020"].median()
        )
        binary["high_seniors"] = (
            base["pct_seniors_65_plus"]
            > base["pct_seniors_65_plus"].median()
        )
        binary["high_immigrants"] = (
            base["pct_recent_immigrants"]
            > base["pct_recent_immigrants"].median()
        )

        # Underserved = low stop density AND low income
        binary["underserved"] = binary["low_stop_density"] & binary["low_income"]

        # Add passup data if available
        passup_tbl = self._table("route_passups")
        if self._db.relation_exists(passup_tbl):
            nb_passups = self._db.query(
                f"""
                SELECT n.name AS neighbourhood,
                       SUM(p.passup_count) AS total_passups
                FROM {passup_tbl} p
                JOIN {self._table("stops")} s
                    ON p.feed_id = s.feed_id
                   AND p.route_short_name = s.stop_id
                JOIN {self._table("neighbourhoods")} n
                    ON ST_Contains(
                        n.geometry,
                        ST_Point(s.stop_lon, s.stop_lat)
                    )
                WHERE p.feed_id = :feed_id
                GROUP BY n.name
                """,
                {"feed_id": self._feed_id},
            )
            if not nb_passups.empty:
                binary = binary.merge(nb_passups, on="neighbourhood", how="left")
                binary["high_passups"] = (
                    binary["total_passups"].fillna(0)
                    > binary["total_passups"].median()
                )
                binary = binary.drop(columns=["total_passups"])

        # Cast to bool before Apriori (CRITICAL)
        bool_cols = [
            c for c in binary.columns if c != "neighbourhood"
        ]
        for col in bool_cols:
            binary[col] = binary[col].astype(bool)

        return binary

    def mine_rules(
        self,
        min_support: float = 0.15,
        min_confidence: float = 0.5,
        min_lift: float = 1.0,
        max_len: int = 3,
    ) -> pd.DataFrame:
        """Run Apriori and generate association rules.

        Args:
            min_support: Minimum support threshold.
            min_confidence: Minimum confidence threshold.
            min_lift: Minimum lift threshold.
            max_len: Maximum itemset length.

        Returns:
            DataFrame with antecedents, consequents, support, confidence,
            lift, leverage, conviction columns.
        """
        from mlxtend.frequent_patterns import apriori, association_rules

        binary = self.build_binary_feature_matrix()
        if binary.empty:
            return pd.DataFrame()

        # Drop neighbourhood column for Apriori
        feature_df = binary.drop(columns=["neighbourhood"]).astype(bool)

        frequent = apriori(
            feature_df,
            min_support=min_support,
            use_colnames=True,
            max_len=max_len,
        )
        if frequent.empty:
            return pd.DataFrame()

        rules = association_rules(
            frequent,
            metric="confidence",
            min_threshold=min_confidence,
        )
        if rules.empty:
            return pd.DataFrame()

        # Filter by lift
        rules = rules[rules["lift"] >= min_lift].copy()
        rules = rules.sort_values("lift", ascending=False).reset_index(drop=True)

        # Convert frozensets to sorted tuples for display
        rules["antecedents"] = rules["antecedents"].apply(
            lambda x: tuple(sorted(x))
        )
        rules["consequents"] = rules["consequents"].apply(
            lambda x: tuple(sorted(x))
        )

        return rules
